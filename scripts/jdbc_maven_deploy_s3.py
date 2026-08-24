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
import hashlib
from os import path

script_dir = path.dirname(path.abspath(__file__))
project_dir = path.dirname(script_dir)

# Per CI build directory: (Maven classifier, name of the native JAR produced by CMake).
# Each native JAR contains the native library for a single platform (libduckdb_java.so_
# <os>_<arch>[<libc>]) at its root. glibc and musl share the same .so name, so the libc
# is reflected in both the CMake native JAR name and the published classifier.
arch_builds = {
    'java-linux-amd64': ('linux_amd64', 'duckdb_jdbc_native_linux_amd64.jar'),
    'java-linux-aarch64': ('linux_arm64', 'duckdb_jdbc_native_linux_arm64.jar'),
    'java-linux-amd64-musl': ('linux_amd64_musl', 'duckdb_jdbc_native_linux_amd64_musl.jar'),    # Alpine Linux
    'java-linux-aarch64-musl': ('linux_arm64_musl', 'duckdb_jdbc_native_linux_arm64_musl.jar'),  # Alpine Linux ARM
    'java-osx-universal': ('macos_universal', 'duckdb_jdbc_native_osx_universal.jar'),        # Intel + Apple Silicon
    'java-windows-amd64': ('windows_amd64', 'duckdb_jdbc_native_windows_amd64.jar'),
    'java-windows-aarch64': ('windows_arm64', 'duckdb_jdbc_native_windows_arm64.jar'),
}

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

def create_pom(version):
    """Create the aggregate duckdb_jdbc (thin) POM declaring the 4 core OS natives."""
    pom_template = pathlib.Path(project_dir, "pom.xml.template").read_text()
    return pom_template.replace("${VERSION}", version)

def create_module_pom(version, artifact_id, classifier):
    """Create the minimal POM for a first-class module artifact (java or native)."""
    module_template = pathlib.Path(project_dir, "classifier-pom.xml.template").read_text()
    return module_template\
        .replace("${ARTIFACT_ID}", artifact_id)\
        .replace("${VERSION}", version)\
        .replace("${CLASSIFIER}", classifier)

def create_sources_jar(jdbc_root, bundle_dir, version):
    """Create sources JAR (attached to duckdb_jdbc_java)."""
    sources_jar = os.path.join(bundle_dir, f'duckdb_jdbc_java-{version}-sources.jar')
    run_cmd(f'jar -cvf {sources_jar} -C {jdbc_root}/src/main/java org')
    return sources_jar


def create_javadoc_jar(jdbc_root, bundle_dir, version):
    """Create javadoc JAR (attached to duckdb_jdbc_java)."""
    javadoc_dir = tempfile.mkdtemp()
    try:
        run_cmd(f'javadoc -Xdoclint:-reference -d {javadoc_dir} -sourcepath {jdbc_root}/src/main/java org.duckdb')
        javadoc_jar = os.path.join(bundle_dir, f'duckdb_jdbc_java-{version}-javadoc.jar')
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

# bundle is laid out as the Maven repo it will be uploaded to:
#   org/duckdb/<artifactId>/<version>/<files>
maven_root = path.join(staging_dir, "org", "duckdb")
def artifact_dir_for(artifact_id):
    return path.join(maven_root, artifact_id, version)

# aggregate: empty JAR (backward-compat shell) + thin POM
aggregate_dir = artifact_dir_for("duckdb_jdbc")
os.makedirs(aggregate_dir)
aggregate_jar = path.join(aggregate_dir, f'duckdb_jdbc-{version}.jar')
aggregate_manifest = os.path.join(staging_dir, "aggregate-manifest")
pathlib.Path(aggregate_manifest).write_text("Manifest-Version: 1.0\nCreated-By: duckdb-java\n\n")
run_cmd(f"jar -cfm {aggregate_jar} {aggregate_manifest}")
pathlib.Path(aggregate_dir, f'duckdb_jdbc-{version}.pom').write_text(create_pom(version))

# java artifact: classes + sources + javadoc + minimal POM
java_dir = artifact_dir_for("duckdb_jdbc_java")
os.makedirs(java_dir)
run_cmd(f'cp {path.join(artifact_dir, "java-linux-amd64", "duckdb_jdbc.jar")} {path.join(java_dir, f"duckdb_jdbc_java-{version}.jar")}')
pathlib.Path(java_dir, f'duckdb_jdbc_java-{version}.pom').write_text(create_module_pom(version, "duckdb_jdbc_java", "java"))
create_sources_jar(project_dir, java_dir, version)
create_javadoc_jar(project_dir, java_dir, version)

# native artifacts: one first-class GAV per classifier
for build_name, (classifier, native_jar) in arch_builds.items():
    native_dir = artifact_dir_for("duckdb_jdbc_%s" % classifier)
    os.makedirs(native_dir)
    run_cmd(f'cp {path.join(artifact_dir, build_name, native_jar)} {path.join(native_dir, f"duckdb_jdbc_{classifier}-{version}.jar")}')
    pathlib.Path(native_dir, f'duckdb_jdbc_{classifier}-{version}.pom').write_text(
        create_module_pom(version, "duckdb_jdbc_%s" % classifier, classifier))

files_to_deploy = []
for root, dirs, files in os.walk(maven_root):
    for f in files:
        files_to_deploy.append(os.path.join(root, f))

# make sure all files exist before continuing
for file in files_to_deploy:
  if not path.isfile(file):
    raise ValueError(f"Could not create all required files: {file}")

# now sign all files

for file in files_to_deploy:
  file_dir = path.dirname(file)
  file_name = path.basename(file)
  run_cmd(f"gpg --sign -ab {file_name}", cwd=file_dir)
  with open(file, "rb") as fd:
    file_bytes = fd.read()
  for alg in ["md5", "sha1", "sha256"]:
    digest = hashlib.new(alg)
    digest.update(file_bytes)
    hashsum = digest.hexdigest()
    with open(f"{file}.{alg}", "w") as fd:
      fd.write(hashsum)
subprocess.run(["find", maven_root, "-type", "f"])

# upload files to s3

dry_run = ""
if not ("AWS_ENDPOINT_URL" in os.environ and
        "AWS_ACCESS_KEY_ID" in os.environ and 
        "AWS_SECRET_ACCESS_KEY" in os.environ):
  dry_run = "--dryrun"

# upload the org/duckdb/... tree to the maven repository root
run_cmd(f"aws s3 cp {maven_root} s3://duckdb-staging/duckdb/duckdb-java/maven/org/duckdb/ {dry_run} --recursive", cwd=staging_dir)

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