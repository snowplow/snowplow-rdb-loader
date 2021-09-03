#!/bin/bash

set -e

project=$1
tag=$2

project_version=$(sbt "project common" version -Dsbt.log.noformat=true | perl -ne 'print "$1\n" if /info.*(\d+\.\d+\.\d+[^\r\n]*)/' | tail -n 1 | tr -d '\n')

export RDB_LOADER_VERSION=${tag##*/}

echo "Comparing project version ${project_version} and git tag ${RDB_LOADER_VERSION}"

if [ "${project_version}" == "${RDB_LOADER_VERSION}" ]; then
    echo "Launching release manager for $project"

    release-manager \
        --config "./.github/release_${project}.yml" \
        --check-version \
        --make-version \
        --make-artifact \
        --upload-artifact
else
    echo "${project_version} is not equal to ${RDB_LOADER_VERSION}"
    exit 1
fi

