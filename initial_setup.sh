#!/usr/bin/env bash

# Determine which package manager to use (prefer uv, fall back to pipenv)
# TODO(#51475): Remove pipenv fallback logic after full uv migration
USE_UV=false

if command -v uv &> /dev/null; then
  echo "Found uv, will use uv for dependency management (recommended)."
  USE_UV=true
# TODO(#51475): Remove this elif branch after full uv migration
elif [[ $(which pip) = $PYENV_ROOT/shims/pip ]]; then
  echo "Using pyenv installed pip..."
  if pip freeze | grep --quiet pipenv; then
    echo "Found pipenv, will use pipenv for dependency management."
  else
    echo "ERROR: Neither uv nor pipenv is installed."
    echo "Install uv (recommended): pip install uv"
    echo "  or: curl -LsSf https://astral.sh/uv/install.sh | sh"
    echo "Or install pipenv: pip install pipenv"
    exit 1
  fi
else
  echo "ERROR: pip is not from a pyenv installation, double check pyenv installation."
  echo "Did you set PYENV_ROOT in your environment after installation?"
  exit 1
fi

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


# Sync dependencies and install pre-commit hooks
if [ "$USE_UV" = true ]; then
  echo "Syncing dependencies with uv..."
  uv sync --all-extras

  echo "Installing pre-commit hooks..."
  uv run pre-commit install --overwrite

  # Note: The pylint/dill/apache-beam conflict that exists with pipenv may be
  # resolved by uv's dependency resolver. If you encounter issues with pylint,
  # you can install it manually with: uv pip install pylint
  # TODO(#51475): Confirm pylint works with uv and remove this note
# TODO(#51475): Remove this else branch after full uv migration
else
  echo "Syncing dependencies with pipenv..."
  pipenv sync --dev --verbose

  echo "Installing pre-commit hooks..."
  pipenv run pre-commit install --overwrite

  # Install pylint which is not currently managed by our Pipfile
  # There is a not easily-solved requirement conflict on `dill` between `apache-beam` and `pylint`
  # More details can be found here: https://recidiviz.slack.com/archives/C028X32LRH7/p1701458677360479
  # TODO(apache/beam#22893): This can be moved into our Pipfile once Beam moves to cloudpickle or upgrades dill
  pip install pylint
fi

echo ""
echo "Setup complete!"
if [ "$USE_UV" = true ]; then
  echo "You can run commands with: uv run <command>"
  echo "Or activate the environment with: source .venv/bin/activate"
else
  echo "You can run commands with: pipenv run <command>"
  echo "Or activate the environment with: pipenv shell"
fi
