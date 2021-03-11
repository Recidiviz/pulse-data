#!/usr/bin/env bash

# Add hooks
cp -R githooks/ .git/hooks/
chmod +x .git/hooks/*

# Set git blame to ignore noisy commits
git config blame.ignoreRevsFile .git-blame-ignore-revs

# Sync with openssl linked properly
export LDFLAGS="-L/usr/local/opt/openssl/lib"
export CPPFLAGS="-I/usr/local/opt/openssl/include"
pipenv sync --dev

# Install pre-commit hooks
pipenv run pre-commit install --overwrite
