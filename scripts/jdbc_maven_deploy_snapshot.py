#!/usr/bin/env python3
"""
Deploy SNAPSHOT builds to Maven Central Snapshots repository.

This script uses `mvn deploy:deploy-file` to upload pre-built JARs to
https://central.sonatype.com/repository/maven-snapshots/

Unlike release publishing, SNAPSHOT publishing:
- Does NOT require GPG signatures
- Does NOT require validation
- Uses standard Maven deploy mechanism
- Artifacts are cleaned up after 90 days

Requirements:
- Maven installed and available in PATH
- MAVEN_USERNAME and MAVEN_PASSWORD environment variables set
  (from https://central.sonatype.com/account)

Usage:
  python jdbc_maven_deploy_snapshot.py <artifact_dir>

See: https://central.sonatype.org/publish/publish-portal-snapshots/
"""

import os
import pathlib
import subprocess
import sys
import tempfile
import re

SNAPSHOT_REPO_URL = "https://central.sonatype.com/repository/maven-snapshots/"
GROUP_ID = "org.duckdb"
ARTIFACT_ID = "duckdb_jdbc"

# Mapping of build directories to Maven classifiers
ARCH_BUILDS = {
    'java-linux-amd64': 'linux_amd64',
    'java-linux-aarch64': 'linux_arm64',
    'java-linux-amd64-musl': 'linux_amd64_musl',
    'java-linux-aarch64-musl': 'linux_arm64_musl',
    'java-osx-universal': 'macos_universal',
    'java-windows-amd64': 'windows_amd64',
    'java-windows-aarch64': 'windows_arm64',
}

# Builds to combine into the main (fat) JAR
COMBINE_BUILDS = ['java-linux-amd64', 'java-osx-universal', 'java-windows-amd64', 'java-linux-aarch64']


def exec(cmd, check=True):
    """Execute a command and return output."""
    print(f"+ {cmd}")
    result = subprocess.run(cmd, shell=True, capture_output=True, text=True)
    if check and result.returncode != 0:
        print(f"STDOUT: {result.stdout}")
        print(f"STDERR: {result.stderr}")
        raise RuntimeError(f"Command failed with code {result.returncode}")
    return result.stdout.strip()


def get_snapshot_version():
    """Calculate SNAPSHOT version from the last release tag."""
    last_tag = exec('git tag --sort=-committerdate').split('\n')[0]
    version_regex = re.compile(r'^v((\d+)\.(\d+)\.\d+\.\d+)$')
    match = version_regex.search(last_tag)
    if not match:
        raise ValueError(f"Could not parse last tag: {last_tag}")
    major = int(match.group(2))
    minor = int(match.group(3))
    # Increment minor version for SNAPSHOT
    return f"{major}.{minor + 1}.0.0-SNAPSHOT"


def create_settings_xml(settings_path):
    """Create Maven settings.xml with Central Portal credentials."""
    username = os.environ.get('MAVEN_USERNAME')
    password = os.environ.get('MAVEN_PASSWORD')

    if not username or not password:
        raise RuntimeError("MAVEN_USERNAME and MAVEN_PASSWORD environment variables are required")

    settings_content = f"""<?xml version="1.0" encoding="UTF-8"?>
<settings>
  <servers>
    <server>
      <id>central-snapshots</id>
      <username>{username}</username>
      <password>{password}</password>
    </server>
  </servers>
</settings>
"""
    pathlib.Path(settings_path).write_text(settings_content)
    os.chmod(settings_path, 0o600)  # Restrict permissions


def deploy_file(settings_path, version, file_path, classifier=None, packaging='jar'):
    """Deploy a single file to the SNAPSHOT repository."""
    cmd = [
        'mvn', 'deploy:deploy-file',
        f'-DgroupId={GROUP_ID}',
        f'-DartifactId={ARTIFACT_ID}',
        f'-Dversion={version}',
        f'-Dpackaging={packaging}',
        f'-Dfile={file_path}',
        f'-DrepositoryId=central-snapshots',
        f'-Durl={SNAPSHOT_REPO_URL}',
        f'-s', settings_path,
        '-DgeneratePom=false',  # We provide our own POM
    ]
    if classifier:
        cmd.append(f'-Dclassifier={classifier}')

    exec(' '.join(cmd))


def create_combined_jar(artifact_dir, staging_dir, version):
    """Create a fat JAR combining native libraries from multiple platforms."""
    import zipfile

    combined_jar = os.path.join(staging_dir, f'duckdb_jdbc-{version}.jar')

    # Start with linux-amd64 as base (without its native lib)
    base_jar = os.path.join(artifact_dir, 'java-linux-amd64', 'duckdb_jdbc.jar')

    with zipfile.ZipFile(base_jar) as src:
        with zipfile.ZipFile(combined_jar, 'w') as dst:
            for item in src.infolist():
                if not item.filename.startswith('libduckdb_java.so'):
                    dst.writestr(item, src.read(item.filename))

    # Add native libraries from all platforms
    for build in COMBINE_BUILDS:
        build_jar = os.path.join(artifact_dir, build, 'duckdb_jdbc.jar')
        with zipfile.ZipFile(build_jar) as src:
            for item in src.infolist():
                if item.filename.startswith('libduckdb_java.so'):
                    with zipfile.ZipFile(combined_jar, 'a') as dst:
                        dst.writestr(item, src.read(item.filename))

    return combined_jar


def create_pom(staging_dir, version):
    """Create POM file for the artifact."""
    pom_content = f"""<?xml version="1.0" encoding="UTF-8"?>
<project>
  <modelVersion>4.0.0</modelVersion>
  <groupId>{GROUP_ID}</groupId>
  <artifactId>{ARTIFACT_ID}</artifactId>
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
    pom_path = os.path.join(staging_dir, f'duckdb_jdbc-{version}.pom')
    pathlib.Path(pom_path).write_text(pom_content)
    return pom_path


def create_sources_jar(jdbc_root, staging_dir, version):
    """Create sources JAR."""
    sources_jar = os.path.join(staging_dir, f'duckdb_jdbc-{version}-sources.jar')
    exec(f'jar -cvf {sources_jar} -C {jdbc_root}/src/main/java org')
    return sources_jar


def create_javadoc_jar(jdbc_root, staging_dir, version):
    """Create javadoc JAR."""
    javadoc_dir = tempfile.mkdtemp()
    exec(f'javadoc -Xdoclint:-reference -d {javadoc_dir} -sourcepath {jdbc_root}/src/main/java org.duckdb')
    javadoc_jar = os.path.join(staging_dir, f'duckdb_jdbc-{version}-javadoc.jar')
    exec(f'jar -cvf {javadoc_jar} -C {javadoc_dir} .')
    return javadoc_jar


def main():
    if len(sys.argv) < 2:
        print("Usage: jdbc_maven_deploy_snapshot.py <artifact_dir> [jdbc_root_path]")
        print("\nDeploys SNAPSHOT builds to Maven Central Snapshots repository.")
        sys.exit(1)

    artifact_dir = sys.argv[1]
    jdbc_root = sys.argv[2] if len(sys.argv) > 2 else '.'

    if not os.path.isdir(artifact_dir):
        print(f"Error: artifact_dir '{artifact_dir}' is not a directory")
        sys.exit(1)

    version = get_snapshot_version()
    print(f"Deploying SNAPSHOT version: {version}")

    staging_dir = tempfile.mkdtemp()
    settings_path = os.path.join(staging_dir, 'settings.xml')

    # Create Maven settings with credentials
    create_settings_xml(settings_path)

    # Create artifacts
    print("\n=== Creating artifacts ===")
    pom_path = create_pom(staging_dir, version)
    combined_jar = create_combined_jar(artifact_dir, staging_dir, version)
    sources_jar = create_sources_jar(jdbc_root, staging_dir, version)
    javadoc_jar = create_javadoc_jar(jdbc_root, staging_dir, version)

    # Deploy POM first (required for other artifacts)
    print("\n=== Deploying POM ===")
    deploy_file(settings_path, version, pom_path, packaging='pom')

    # Deploy main JAR
    print("\n=== Deploying main JAR ===")
    deploy_file(settings_path, version, combined_jar)

    # Deploy sources and javadoc
    print("\n=== Deploying sources JAR ===")
    deploy_file(settings_path, version, sources_jar, classifier='sources')

    print("\n=== Deploying javadoc JAR ===")
    deploy_file(settings_path, version, javadoc_jar, classifier='javadoc')

    # Deploy architecture-specific JARs
    print("\n=== Deploying architecture-specific JARs ===")
    for build_name, classifier in ARCH_BUILDS.items():
        jar_path = os.path.join(artifact_dir, build_name, 'duckdb_jdbc.jar')
        if os.path.exists(jar_path):
            print(f"Deploying {classifier}...")
            deploy_file(settings_path, version, jar_path, classifier=classifier)
        else:
            print(f"Warning: {jar_path} not found, skipping")

    # Create nolib JAR (JAR without native libraries)
    print("\n=== Deploying nolib JAR ===")
    import zipfile
    nolib_jar = os.path.join(staging_dir, f'duckdb_jdbc-{version}-nolib.jar')
    base_jar = os.path.join(artifact_dir, 'java-linux-amd64', 'duckdb_jdbc.jar')
    with zipfile.ZipFile(base_jar) as src:
        with zipfile.ZipFile(nolib_jar, 'w') as dst:
            for item in src.infolist():
                if not item.filename.startswith('libduckdb_java.so'):
                    dst.writestr(item, src.read(item.filename))
    deploy_file(settings_path, version, nolib_jar, classifier='nolib')

    print(f"\n=== SUCCESS ===")
    print(f"SNAPSHOT {version} deployed to {SNAPSHOT_REPO_URL}")
    print(f"\nTo use in Maven:")
    print(f"  <dependency>")
    print(f"    <groupId>{GROUP_ID}</groupId>")
    print(f"    <artifactId>{ARTIFACT_ID}</artifactId>")
    print(f"    <version>{version}</version>")
    print(f"  </dependency>")
    print(f"\nWith repository:")
    print(f"  <repository>")
    print(f"    <id>central-snapshots</id>")
    print(f"    <url>{SNAPSHOT_REPO_URL}</url>")
    print(f"    <snapshots><enabled>true</enabled></snapshots>")
    print(f"  </repository>")


if __name__ == '__main__':
    main()
