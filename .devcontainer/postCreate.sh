#!/usr/bin/env bash

# Add hooks
cp -R githooks/ .git/hooks/
chmod +x .git/hooks/*

# Install uv if not already installed
pip install uv

# Sync dependencies
uv sync --all-extras

# Install pre-commit hooks
uv run pre-commit install --overwrite

# Install pylint (workaround for dill/apache-beam conflict)
uv pip install pylint --python .venv/bin/python

# Clear out cache which could have been populated by running tests under a different system
# architecture (such as outside the container). See https://nicolasbouliane.com/blog/importmismatcherror-python-fix
find . \( -name '__pycache__' -or -name '*.pyc' \) -delete
