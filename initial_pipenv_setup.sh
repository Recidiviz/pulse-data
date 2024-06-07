#!/usr/bin/env bash


if [[ $(which pip) = $PYENV_ROOT/shims/pip ]]
then
  echo "Using pyenv installed pip, proceeding..."
else
  echo "ERROR: pip is not from a pyenv installation, double check pyenv installation."
  echo "Did you set PYENV_ROOT in your environment after installation?"
  exit 1
fi

if pip freeze | grep --quiet pipenv
then
  echo "Found pipenv, proceeding..."
else
  echo "ERROR: pipenv was not installed to this python installation."
  echo "Have you confirmed your pyenv installation and run pip install pipenv?"
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


# Sync dependencies now that environment is setup properly
pipenv sync --dev --verbose

# Install pre-commit hooks
pipenv run pre-commit install --overwrite

# Install pylint which is not currently managed by our Pipfile
# There is a not easily-solved requirement conflict on `dill` between `apache-beam` and `pylint`
# More details can be found here: https://recidiviz.slack.com/archives/C028X32LRH7/p1701458677360479
# TODO(apache/beam#22893): This can be moved into our Pipfile once Beam moves to cloudpickle or upgrades dill
pip install pylint
