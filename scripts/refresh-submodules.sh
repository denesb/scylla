#!/bin/bash
#
# Refresh git submodules by fast-forward merging them to the tip of the
# master branch of their respective repositories and committing the
# update with a default commit message of "git submodule summary".
#
# Copyright (C) 2020 ScyllaDB
#
# This file is part of Scylla.
#
# See the LICENSE.PROPRIETARY file in the top-level directory for licensing information.
#

set -euo pipefail

submodules=(
    seastar
    tools/jmx
    tools/java
    tools/python3
)

for submodule in "${submodules[@]}"; do
    GIT_DIR="$submodule/.git" git pull --ff-only origin master
    SUMMARY=$(git submodule summary $submodule)
    if grep '^ *<' <<< "$SUMMARY"; then
        echo "Non fast-forward changes detected! Fire three red flares from your flare pistol."
        exit 1
    fi
    if [ ! -z "$SUMMARY" ]; then
        git commit --edit -m "Update $submodule submodule" -m "$SUMMARY" $submodule
    fi
done
