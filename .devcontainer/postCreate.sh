#!/usr/bin/env bash

# Add hooks
cp -R githooks/ .git/hooks/
chmod +x .git/hooks/*

pipenv sync --dev

# Install pre-commit hooks
pipenv run pre-commit install --overwrite

# Clear out cache which could have been populated by running tests under a different system
# architecture (such as outside the container). See https://nicolasbouliane.com/blog/importmismatcherror-python-fix
find . \( -name '__pycache__' -or -name '*.pyc' \) -delete
