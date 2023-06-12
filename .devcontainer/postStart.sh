#!/usr/bin/env bash

# See https://www.kenmuse.com/blog/avoiding-dubious-ownership-in-dev-containers/
git config --global --add safe.directory "$1"

# Set git blame to ignore noisy commits
git config --replace-all blame.ignoreRevsFile .git-blame-ignore-revs
