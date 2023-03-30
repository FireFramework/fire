#!/bin/bash

SELF=$(cd $(dirname $0) && pwd)
PARENT_PATH="$SELF/../../"

# 1. Change working directory to .git parent
cd $PARENT_PATH

##############################
# variables
##############################

git_hash=`git rev-parse --short HEAD`
NEXUS_ROOT="https://s01.oss.sonatype.org/service/local/staging"
PUBLISH_PROFILE="54ea9af76048"

# 兼容zsh
echo -n "Please input sonatype username: " && read SONATYPE_USERNAME
echo -n "Please input sonatype password: " && read SONATYPE_PASSWORD
echo -n "Please input gpg key: " && read GPG_KEY
echo -n "Please input gpg password: " && read GPG_PASSPHRASE

MVN="mvn"
MVN_SETTINGS="${MVN_SETTINGS-$(realpath ~/.m2/settings.xml)}"
MVN_TEMP_REPO=dev/repo/fire-repo

GPG="gpg -u $GPG_KEY --no-tty --batch --pinentry-mode loopback"

# Coerce the requested version
# $MVN versions:set -DnewVersion=$FIRE_VERSION

# 2. create staging repository in sonatype
# Using Nexus API documented here: https://support.sonatype.com/hc/en-us/articles/213465868-Uploading-to-a-Staging-Repository-via-REST-API
echo "Creating Nexus staging repository"
repo_request="<promoteRequest><data><description>Fire-Framework-uploading</description></data></promoteRequest>"
out=$(curl -X POST -d "${repo_request}" \
  -u $SONATYPE_USERNAME:$SONATYPE_PASSWORD \
  -H "Content-Type:application/xml" -v \
  "$NEXUS_ROOT/profiles/$PUBLISH_PROFILE/start")

staged_repo_id=$(echo $out | sed -n 's/.*<stagedRepositoryId>\(.*\)<\/stagedRepositoryId>.*/\1/p')
echo "Created Nexus staging repository: $staged_repo_id"

mkdir -p $MVN_TEMP_REPO
rm -rf $MVN_TEMP_REPO/com/zto/fire

# 3. build in MVN_TEMP_REPO
set -e
$MVN -Dmaven.repo.local=$MVN_TEMP_REPO -DskipTests -s $MVN_SETTINGS clean install -Phadoop-2.7,scala-2.11,flink-1.12,spark-2.3,release -U -T20C
$MVN -Dmaven.repo.local=$MVN_TEMP_REPO -DskipTests -s $MVN_SETTINGS clean install -Phadoop-2.7,scala-2.11,flink-1.13,spark-2.4,release -U -T20C
$MVN -Dmaven.repo.local=$MVN_TEMP_REPO -DskipTests -s $MVN_SETTINGS clean install -Phadoop-2.7,scala-2.11,flink-1.14,spark-2.4,release -U -T20C

$MVN -Dmaven.repo.local=$MVN_TEMP_REPO -DskipTests -s $MVN_SETTINGS clean install -Phadoop-2.7,scala-2.12,flink-1.12,spark-2.4,release -U -T20C
$MVN -Dmaven.repo.local=$MVN_TEMP_REPO -DskipTests -s $MVN_SETTINGS clean install -Phadoop-2.7,scala-2.12,flink-1.13,spark-3.0,release -U -T20C
$MVN -Dmaven.repo.local=$MVN_TEMP_REPO -DskipTests -s $MVN_SETTINGS clean install -Phadoop-2.7,scala-2.12,flink-1.14,spark-3.1,release -U -T20C
$MVN -Dmaven.repo.local=$MVN_TEMP_REPO -DskipTests -s $MVN_SETTINGS clean install -Phadoop-2.7,scala-2.12,flink-1.15,spark-3.2,release -U -T20C
$MVN -Dmaven.repo.local=$MVN_TEMP_REPO -DskipTests -s $MVN_SETTINGS clean install -Phadoop-2.7,scala-2.12,flink-1.16,spark-3.3,release -U -T20C
set +e

# 4. ready to uploading file
pushd $MVN_TEMP_REPO/com/zto/fire

# Remove any extra files generated during install
find . -type f |grep -v \.jar |grep -v \.pom | xargs rm

# 4.1 sign each generated fire file
echo "Creating hash and signature files"
# this must have .asc, .md5 and .sha1 - it really doesn't like anything else there
for file in $(find . -type f)
do
  echo "generate <gpg, checksum> for: $file"
  echo $GPG_PASSPHRASE | $GPG --passphrase-fd 0 --output $file.asc --detach-sig --armour $file
  if [ $(command -v md5) ]; then
    # Available on OS X; -q to keep only hash
    md5 -q $file > $file.md5
  else
    # Available on Linux; cut to keep only hash
    md5sum $file | cut -f1 -d' ' > $file.md5
  fi
  sha1sum $file | cut -f1 -d' ' > $file.sha1
done

# 4.2 uploading
nexus_upload=$NEXUS_ROOT/deployByRepositoryId/$staged_repo_id
echo "Uploading files to $nexus_upload"
for file in $(find . -type f)
do
  # strip leading ./
  file_short=$(echo $file | sed -e "s/\.\///")
  dest_url="$nexus_upload/com/zto/fire/$file_short"
  echo "  Uploading $file_short"
  curl -u $SONATYPE_USERNAME:$SONATYPE_PASSWORD --upload-file $file_short $dest_url
done

popd; exit 0

# 4.3 Try to closing staging repository
echo "Closing nexus staging repository"
repo_request="<promoteRequest><data><stagedRepositoryId>$staged_repo_id</stagedRepositoryId><description>Fire Framework uploading (commit $git_hash)</description></data></promoteRequest>"
out=$(curl -X POST -d "$repo_request" -u $SONATYPE_USERNAME:$SONATYPE_PASSWORD \
  -H "Content-Type:application/xml" -v \
  $NEXUS_ROOT/profiles/$PUBLISH_PROFILE/finish)
echo "Closed Nexus staging repository: $staged_repo_id"

popd