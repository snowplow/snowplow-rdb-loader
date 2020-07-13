#!/bin/bash

set -e

project=$1
tag=$2

export RDB_LOADER_VERSION=$tag

./.github/is_release_tag.sh $project $tag

echo "Launching release manager for $project"

release-manager \
    --config "./.github/release_${project}.yml" \
    --check-version \
    --make-version \
    --make-artifact \
    --upload-artifact
