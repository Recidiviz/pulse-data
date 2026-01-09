#!/usr/bin/env bash

# Verify uv is installed
if ! command -v uv &> /dev/null; then
  echo "ERROR: uv is not installed."
  echo "Install uv: pip install uv"
  echo "  or: curl -LsSf https://astral.sh/uv/install.sh | sh"
  exit 1
fi

echo "Found uv, using uv for dependency management."

# The Debian VMs on GCP already have cmake and
# MacOS openssl is fussy, so we only run this
# on Macs.
if uname -a | grep --ignore-case darwin
then

  echo "Running MacOS specific installation of cmake."

  # Ensure openssl is linked properly
  export LDFLAGS="-L/usr/local/opt/openssl/lib"
  export CPPFLAGS="-I/usr/local/opt/openssl/include"

  # Install cmake to avoid qdldl build error: 'RuntimeError: CMake must be
  # installed to build qdldl'
  brew install cmake

else
  echo "Not running on MacOS. Skipping cmake install."
fi


# Add hooks
cp -R githooks/ .git/hooks/
chmod +x .git/hooks/*

# Set git blame to ignore noisy commits
git config blame.ignoreRevsFile .git-blame-ignore-revs


# Sync dependencies
echo "Syncing dependencies with uv..."
uv sync --all-extras

# Install pre-commit hooks
echo "Installing pre-commit hooks..."
uv run pre-commit install --overwrite

# Install pylint which is not currently managed by pyproject.toml
# There is a not easily-solved requirement conflict on `dill` between `apache-beam` and `pylint`
# More details can be found here: https://recidiviz.slack.com/archives/C028X32LRH7/p1701458677360479
# TODO(apache/beam#22893): This can be moved into pyproject.toml once Beam moves to cloudpickle or upgrades dill
echo "Installing pylint..."
uv pip install pylint --python .venv/bin/python

echo ""
echo "Setup complete!"
echo "You can run commands with: uv run <command>"
echo "Or activate the environment with: source .venv/bin/activate"
