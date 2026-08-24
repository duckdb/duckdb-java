# https://central.sonatype.org/publish/publish-portal-api/

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
import re
import base64
import hashlib
from os import path


def exec(cmd):
    print(cmd)
    res = subprocess.run(cmd.split(' '), capture_output=True)
    if res.returncode == 0:
        return res.stdout
    raise ValueError(res.stdout + res.stderr)


if len(sys.argv) < 4 or not os.path.isdir(sys.argv[2]) or not os.path.isdir(sys.argv[3]):
    print("Usage: [release_tag, format: v1.2.3.4] [artifact_dir] [jdbc_root_path]")
    exit(1)

version_regex = re.compile(r'^v((\d+)\.(\d+)\.\d+\.\d+)$')
release_tag = sys.argv[1]
deploy_url = 'https://central.sonatype.com/api/v1/publisher/upload'
is_release = True

if release_tag == 'main':
    # for SNAPSHOT builds we increment the minor version and set patch level to zero.
    # seemed the most sensible
    last_tag = exec('git tag --sort=-committerdate').decode('utf8').split('\n')[0]
    re_result = version_regex.search(last_tag)
    if re_result is None:
        raise ValueError("Could not parse last tag %s" % last_tag)
    release_version = "%d.%d.0.0-SNAPSHOT" % (int(re_result.group(2)), int(re_result.group(3)) + 1)
    is_release = False
elif version_regex.match(release_tag):
    release_version = version_regex.search(release_tag).group(1)
else:
    print("Not running on %s" % release_tag)
    exit(0)

jdbc_artifact_dir = sys.argv[2]
jdbc_root_path = sys.argv[3]

# Per CI build: (Maven classifier, name of the native JAR produced by CMake).
# The native JAR produced by CMake is duckdb_jdbc_native_<os>_<arch>[<libc>].jar with
# the CMake OS/arch convention (osx_universal for macOS); each is published as a
# first-class artifact org.duckdb:duckdb_jdbc_<classifier>.
arch_specific_builds = {
  'linux-amd64': ('linux_amd64', 'duckdb_jdbc_native_linux_amd64.jar'),
  'linux-aarch64': ('linux_arm64', 'duckdb_jdbc_native_linux_arm64.jar'),
  'linux-amd64-musl': ('linux_amd64_musl', 'duckdb_jdbc_native_linux_amd64_musl.jar'),
  'linux-aarch64-musl': ('linux_arm64_musl', 'duckdb_jdbc_native_linux_arm64_musl.jar'),
  'osx-universal': ('macos_universal', 'duckdb_jdbc_native_osx_universal.jar'),
  'windows-amd64': ('windows_amd64', 'duckdb_jdbc_native_windows_amd64.jar'),
  'windows-aarch64': ('windows_arm64', 'duckdb_jdbc_native_windows_arm64.jar'),
}

# Native classifiers included in the duckdb_jdbc aggregate (thin) POM. musl and
# Windows ARM64 are published but not part of it; they are opt-in.
aggregate_classifiers = ['linux_amd64', 'linux_arm64', 'macos_universal', 'windows_amd64']

staging_dir = tempfile.mkdtemp()

def aggregate_pom_path(version):
  return '%s/duckdb_jdbc-%s.pom' % (staging_dir, version)

# sources/javadoc are attached to the duckdb_jdbc_java artifact only
java_jar = '%s/duckdb_jdbc_java-%s.jar' % (staging_dir, release_version)
java_pom = '%s/duckdb_jdbc_java-%s.pom' % (staging_dir, release_version)
sources_jar = '%s/duckdb_jdbc_java-%s-sources.jar' % (staging_dir, release_version)
javadoc_jar = '%s/duckdb_jdbc_java-%s-javadoc.jar' % (staging_dir, release_version)

# duckdb_jdbc: empty JAR (backward-compat shell) + thin aggregate POM
aggregate_jar = '%s/duckdb_jdbc-%s.jar' % (staging_dir, release_version)

# native artifacts per classifier
classifier_poms = {}
arch_specific_jars = []
for build, (classifier, native_jar) in arch_specific_builds.items():
  arch_specific_jars.append('%s/duckdb_jdbc_%s-%s.jar' % (staging_dir, classifier, release_version))
  classifier_poms[classifier] = '%s/duckdb_jdbc_%s-%s.pom' % (staging_dir, classifier, release_version)

# thin aggregate POM for duckdb_jdbc (unconditional deps on the 4 core OS natives)
pom_template = pathlib.Path(jdbc_root_path, "pom.xml.template").read_text()
aggregate_pom = aggregate_pom_path(release_version)
pathlib.Path(aggregate_pom).write_text(pom_template.replace("${VERSION}", release_version))

# java artifact POM + native classifier POMs come from the shared template
module_pom_template = pathlib.Path(jdbc_root_path, "classifier-pom.xml.template").read_text()
def write_module_pom(pom_path, artifact_id, classifier):
  content = module_pom_template\
      .replace("${ARTIFACT_ID}", artifact_id)\
      .replace("${VERSION}", release_version)\
      .replace("${CLASSIFIER}", classifier)
  pathlib.Path(pom_path).write_text(content)

write_module_pom(java_pom, "duckdb_jdbc_java", "java")
for classifier, classifier_pom in classifier_poms.items():
  write_module_pom(classifier_pom, "duckdb_jdbc_%s" % classifier, classifier)

# the pure-Java JAR is taken from any platform build (they are identical now that the
# native library lives in separate classifier JARs)
binary_src_jar = os.path.join(jdbc_artifact_dir, "java-linux-amd64", "duckdb_jdbc.jar")
shutil.copyfile(binary_src_jar, java_jar)

# empty JAR for the aggregate: manifest only, no classes/natives
aggregate_manifest = os.path.join(staging_dir, "aggregate-manifest")
pathlib.Path(aggregate_manifest).write_text(
  "Manifest-Version: 1.0\nCreated-By: duckdb-java\n\n")
exec("jar -cfm %s %s" % (aggregate_jar, aggregate_manifest))

javadoc_stage_dir = tempfile.mkdtemp()

exec("javadoc -Xdoclint:-reference -d %s -sourcepath %s/src/main/java org.duckdb" % (javadoc_stage_dir, jdbc_root_path))
exec("jar -cvf %s -C %s ." % (javadoc_jar, javadoc_stage_dir))
exec("jar -cvf %s -C %s/src/main/java org" % (sources_jar, jdbc_root_path))

# copy each platform's native JAR to its first-class artifact name
for build, (classifier, native_jar) in arch_specific_builds.items():
  src_jar = os.path.join(jdbc_artifact_dir, "java-" + build, native_jar)
  dest_jar = os.path.join(staging_dir, "duckdb_jdbc_%s-%s.jar" % (classifier, release_version))
  shutil.copyfile(src_jar, dest_jar)

files_to_deploy = [
  aggregate_jar,
  aggregate_pom,
  java_jar,
  java_pom,
  sources_jar,
  javadoc_jar,
]
for jar, classifier_pom in zip(arch_specific_jars, classifier_poms.values()):
  files_to_deploy.append(jar)
  files_to_deploy.append(classifier_pom)

# make sure all files exist before continuing
for file in files_to_deploy:
  if not path.isfile(file):
    raise ValueError(f"Could not create all required files: {file}")

# now sign and upload everything
# for this to work, you must have MAVEN_USERNAME and MAVEN_PASSWORD
# environment variables for the Sonatype Central Portal

bundle_root_dir = path.join(staging_dir, "central-bundle")
bundle_zip = path.join(staging_dir, "central-bundle.zip")

# The Central Portal bundle requires one directory per (artifact, version):
#   org/duckdb/<artifactId>/<version>/<files>
# Each staged file is routed to its artifact's directory by matching the known
# artifactId prefix (the aggregate 'duckdb_jdbc', the java artifact, or each native).
native_artifact_ids = ["duckdb_jdbc_%s" % classifier for classifier, _ in arch_specific_builds.values()]
all_artifact_ids = ["duckdb_jdbc", "duckdb_jdbc_java"] + native_artifact_ids

for file in files_to_deploy:
  file_name = path.basename(file)
  artifact_id = next(aid for aid in all_artifact_ids if file_name.startswith(aid + "-" + release_version))
  bundle_dir = path.join(bundle_root_dir, "org", "duckdb", artifact_id, release_version)
  os.makedirs(bundle_dir, exist_ok=True)
  bundle_file = path.join(bundle_dir, file_name)
  shutil.copyfile(file, bundle_file)
  subprocess.run(["gpg", "--sign", "-ab", file_name], cwd=bundle_dir)
  with open(bundle_file, "rb") as fd:
    file_bytes = fd.read()
  for alg in ["md5", "sha1", "sha256"]:
    digest = hashlib.new(alg)
    digest.update(file_bytes)
    hashsum = digest.hexdigest()
    with open(f"{bundle_file}.{alg}", "w") as fd:
      fd.write(hashsum)

subprocess.run(["ls", "-laR", bundle_root_dir])
subprocess.run(["zip", "-qr", bundle_zip, "org"], cwd=bundle_root_dir)

maven_username = os.environ["MAVEN_USERNAME"]
maven_password = os.environ["MAVEN_PASSWORD"]
token = base64.b64encode(f"{maven_username}:{maven_password}".encode("utf-8")).decode("utf-8")

subprocess.run([
  "curl",
  # "--verbose", do NOT enable it on CI, it leaks the auth token
  "--silent",
  "--header", f"Authorization: Bearer {token}",
  "--form", f"name={release_version}",
  "--form", "publishingType=AUTOMATIC",
  "--form", f"bundle=@{bundle_zip}",
  deploy_url,
  ], cwd=bundle_root_dir, check=True)

print("Done?")
