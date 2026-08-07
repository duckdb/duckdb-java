# this is the pgp key we use to sign releases
# if this key should be lost, generate a new one with `gpg --full-generate-key`
# AND upload to keyserver: `gpg --keyserver hkp://keys.openpgp.org --send-keys [...]`
# export the keys for GitHub Actions like so: `gpg --export-secret-keys | base64`
# --------------------------------
# pub   ed25519 2022-02-07 [SC]
#       65F91213E069629F406F7CF27F610913E3A6F526
# uid           [ultimate] DuckDB <quack@duckdb.org>
# sub   cv25519 2022-02-07 [E]

import os
import pathlib
import shutil
import subprocess
import sys
import tempfile
import zipfile
import re
import base64
import hashlib
from os import path

script_dir = path.dirname(path.abspath(__file__))
project_dir = path.dirname(script_dir)

# Mapping of build directories to Maven classifiers.
# These architecture-specific JARs contain native libraries for a single platform only,
# useful for reducing deployment size when the target platform is known.
arch_builds = {
    'java-linux-amd64': 'linux_amd64',
    'java-linux-aarch64': 'linux_arm64',
    'java-linux-amd64-musl': 'linux_amd64_musl',    # Alpine Linux
    'java-linux-aarch64-musl': 'linux_arm64_musl',  # Alpine Linux ARM
    'java-osx-universal': 'macos_universal',        # Intel + Apple Silicon
    'java-windows-amd64': 'windows_amd64',
    'java-windows-aarch64': 'windows_arm64',
}

# Builds to combine into the main (fat) JAR.
# The main JAR includes natives for all major platforms for convenience.
combine_builds = ['java-linux-amd64', 'java-osx-universal', 'java-windows-amd64', 'java-linux-aarch64']

def run_cmd(cmd, check=True, cwd=project_dir):
    """Execute a command and return output."""
    print(f"+ {cmd}")
    result = subprocess.run(cmd, shell=True, capture_output=True, text=True, cwd=cwd)
    if check and result.returncode != 0:
        print(f"STDOUT: {result.stdout}")
        print(f"STDERR: {result.stderr}")
        raise RuntimeError(f"Command failed with code {result.returncode}")
    return result.stdout.strip()

def get_snapshot_version(external_version):
    # Get short commit hash for traceability
    commit_hash = run_cmd('git rev-parse --short=7 HEAD')
    prefix = "2.0.0-dev"
    if len(external_version) > 0:
        prefix = external_version
    if prefix[0] == "v":
        prefix = prefix[1:]
    return f"{prefix}-{commit_hash}"

def create_combined_jar(artifact_dir, bundle_dir, version):
    """Create a fat JAR combining native libraries from multiple platforms."""
    combined_jar = os.path.join(bundle_dir, f'duckdb_jdbc-{version}.jar')
    base_jar = os.path.join(artifact_dir, 'java-linux-amd64', 'duckdb_jdbc.jar')

    with zipfile.ZipFile(combined_jar, 'w') as dst:
        # Copy base jar excluding native libs
        with zipfile.ZipFile(base_jar) as src:
            for item in src.infolist():
                if not item.filename.startswith('libduckdb_java.so'):
                    dst.writestr(item, src.read(item.filename))

        # Add native libraries from all platforms
        for build in combine_builds:
            build_jar = os.path.join(artifact_dir, build, 'duckdb_jdbc.jar')
            with zipfile.ZipFile(build_jar) as src:
                for item in src.infolist():
                    if item.filename.startswith('libduckdb_java.so'):
                        dst.writestr(item, src.read(item.filename))

    return combined_jar

def create_nolib_jar(artifact_dir, bundle_dir, version):
    """
    Create a JAR without native libraries (nolib classifier).

    This variant contains only Java classes without any bundled native libraries.
    Useful for:
    - Custom native library management (loading from system path or custom location)
    - Platforms not covered by pre-built natives (users compile their own)
    - Smaller artifact size when natives are managed separately
    - Container/deployment scenarios where natives are provided at infrastructure level
    """
    nolib_jar = os.path.join(bundle_dir, f'duckdb_jdbc-{version}-nolib.jar')
    base_jar = os.path.join(artifact_dir, 'java-linux-amd64', 'duckdb_jdbc.jar')

    with zipfile.ZipFile(base_jar) as src:
        with zipfile.ZipFile(nolib_jar, 'w') as dst:
            for item in src.infolist():
                if not item.filename.startswith('libduckdb_java.so'):
                    dst.writestr(item, src.read(item.filename))

    return nolib_jar

def create_pom(bundle_dir, version):
    """Create POM file for the artifact."""
    pom_content = f"""<?xml version="1.0" encoding="UTF-8"?>
<project>
  <modelVersion>4.0.0</modelVersion>
  <groupId>org.duckdb</groupId>
  <artifactId>duckdb_jdbc</artifactId>
  <version>{version}</version>
  <packaging>jar</packaging>
  <name>DuckDB JDBC Driver</name>
  <description>A JDBC-Compliant driver for the DuckDB data management system</description>
  <url>https://www.duckdb.org</url>

  <licenses>
    <license>
      <name>MIT License</name>
      <url>https://raw.githubusercontent.com/duckdb/duckdb/main/LICENSE</url>
      <distribution>repo</distribution>
    </license>
  </licenses>

  <developers>
    <developer>
      <name>Mark Raasveldt</name>
      <email>mark@duckdblabs.com</email>
      <organization>DuckDB Labs</organization>
      <organizationUrl>https://www.duckdblabs.com</organizationUrl>
    </developer>
    <developer>
      <name>Hannes Muehleisen</name>
      <email>hannes@duckdblabs.com</email>
      <organization>DuckDB Labs</organization>
      <organizationUrl>https://www.duckdblabs.com</organizationUrl>
    </developer>
  </developers>

  <scm>
    <connection>scm:git:git://github.com/duckdb/duckdb-java.git</connection>
    <developerConnection>scm:git:ssh://github.com:duckdb/duckdb-java.git</developerConnection>
    <url>https://github.com/duckdb/duckdb-java</url>
  </scm>
</project>
"""
    pom_path = os.path.join(bundle_dir, f'duckdb_jdbc-{version}.pom')
    pathlib.Path(pom_path).write_text(pom_content)
    return pom_path


def create_sources_jar(jdbc_root, bundle_dir, version):
    """Create sources JAR."""
    sources_jar = os.path.join(bundle_dir, f'duckdb_jdbc-{version}-sources.jar')
    run_cmd(f'jar -cvf {sources_jar} -C {jdbc_root}/src/main/java org')
    return sources_jar


def create_javadoc_jar(jdbc_root, bundle_dir, version):
    """Create javadoc JAR."""
    javadoc_dir = tempfile.mkdtemp()
    try:
        run_cmd(f'javadoc -Xdoclint:-reference -d {javadoc_dir} -sourcepath {jdbc_root}/src/main/java org.duckdb')
        javadoc_jar = os.path.join(bundle_dir, f'duckdb_jdbc-{version}-javadoc.jar')
        run_cmd(f'jar -cvf {javadoc_jar} -C {javadoc_dir} .')
        return javadoc_jar
    finally:
        shutil.rmtree(javadoc_dir, ignore_errors=True)

# main

if len(sys.argv) < 2:
    print("Usage: jdbc_maven_deploy_s3.py <artifact_dir> [external_version]")
    print("\nDeploys SNAPSHOT builds to S3.")
    sys.exit(1)

artifact_dir = sys.argv[1]
external_version = ""
if len(sys.argv) == 3:
    external_version = sys.argv[2]
version = get_snapshot_version(external_version)

if not os.path.isdir(artifact_dir):
    print(f"Error: artifact_dir '{artifact_dir}' is not a directory")
    sys.exit(1)

print(f"Deploying SNAPSHOT version: {version}")

staging_dir = tempfile.mkdtemp()

bundle_dir = path.join(staging_dir, version)
os.mkdir(bundle_dir)

pom = create_pom(bundle_dir, version)
combined_jar = create_combined_jar(artifact_dir, bundle_dir, version)
sources_jar = create_sources_jar(project_dir, bundle_dir, version)
javadoc_jar = create_javadoc_jar(project_dir, bundle_dir, version)
nolib_jar = create_nolib_jar(artifact_dir, bundle_dir, version)
arch_specific_jars = []

for build_name, classifier in arch_builds.items():
    src_jar = path.join(artifact_dir, build_name, 'duckdb_jdbc.jar')
    dest_jar = path.join(bundle_dir, f'duckdb_jdbc-{version}-{classifier}.jar')
    shutil.copyfile(src_jar, dest_jar)
    arch_specific_jars.append(dest_jar)

files_to_deploy = [
  combined_jar,
  sources_jar,
  javadoc_jar,
  nolib_jar,
  pom
]
for jar in arch_specific_jars:
  files_to_deploy.append(jar)

# make sure all files exist before continuing
for file in files_to_deploy:
  if not path.isfile(file):
    raise ValueError(f"Could not create all required files: {file}")

# now sign all files 

for file in files_to_deploy:
  file_name = path.basename(file)
  run_cmd(f"gpg --sign -ab {file_name}", cwd=bundle_dir)
  with open(file, "rb") as fd:
    file_bytes = fd.read()
  for alg in ["md5", "sha1", "sha256"]:
    digest = hashlib.new(alg)
    digest.update(file_bytes)
    hashsum = digest.hexdigest()
    with open(f"{file}.{alg}", "w") as fd:
      fd.write(hashsum)
subprocess.run(["ls", "-laR", bundle_dir])

# upload files to s3

dry_run = ""
if not ("AWS_ENDPOINT_URL" in os.environ and
        "AWS_ACCESS_KEY_ID" in os.environ and 
        "AWS_SECRET_ACCESS_KEY" in os.environ):
  dry_run = "--dryrun"

run_cmd(f"aws s3 cp {bundle_dir} s3://duckdb-staging/duckdb/duckdb-java/maven/org/duckdb/duckdb_jdbc/{version}/ {dry_run} --recursive", cwd=staging_dir)

print(f"""
<dependency>
    <groupId>org.duckdb</groupId>
    <artifactId>duckdb_jdbc</artifactId>
    <version>{version}</version>
</dependency>

<repository>
    <id>duckdb</id>
    <url>https://duckdb-staging.duckdb.org/duckdb/duckdb-java/maven/</url>
</repository>
""")
