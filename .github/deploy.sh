#!/bin/bash

set -e

project=$1
tag=$2

export RDB_LOADER_VERSION=$tag

release-manager \
    --config "./.github/release_${project}.yml" \
    --check-version \
    --make-version \
    --make-artifact \
    --upload-artifact
