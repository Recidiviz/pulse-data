#!/usr/bin/env bash

# Add hooks
cp -R githooks/ .git/hooks/
chmod +x .git/hooks/*

# Set git blame to ignore noisy commits
git config blame.ignoreRevsFile .git-blame-ignore-revs

# Ensure openssl is linked properly
export LDFLAGS="-L/usr/local/opt/openssl/lib"
export CPPFLAGS="-I/usr/local/opt/openssl/include"

# Install cmake to avoid qdldl build error: 'RuntimeError: CMake must be
# installed to build qdldl'
brew install cmake

# Sync dependencies now that environment is setup properly
pipenv sync --dev --verbose

# Install pre-commit hooks
pipenv run pre-commit install --overwrite

# Install pylint which is not currently managed by our Pipfile
# There is a not easily-solved requirement conflict on `dill` between `apache-beam` and `pylint`
# More details can be found here: https://recidiviz.slack.com/archives/C028X32LRH7/p1701458677360479
# TODO(apache/beam#22893): This can be moved into our Pipfile once Beam moves to cloudpickle or upgrades dill
pip install pylint
