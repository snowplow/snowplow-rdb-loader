#!/bin/bash

REFS_TAGS="refs/tags/"

project=$1
tag=$2

refs_tags_len=${#REFS_TAGS}

slashed="${project}/"
slashed_len=${#slashed}

cicd=${tag:$refs_tags_len:${slashed_len}}
release=${tag:${slashed_len}}

echo "Comparing project name from git tag ${cicd} (from ${tag}) and specified ${slashed}"

if [ "${cicd}" == "${slashed}" ]; then
    if [ "${release}" == "" ]; then
        echo "Warning! No release specified! Ignoring."
        exit 2
    fi
    exit 0
else
    echo "${cicd} is not equal to ${slashed}"
    exit 1
fi
