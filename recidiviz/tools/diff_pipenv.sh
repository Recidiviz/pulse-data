#!/bin/bash

# This checks to ensure your pipenv is in sync with the lock file.
# 'initial_pipenv_setup_mac' sets this up to run every time you pull a new version on main or checkout a branch.

BASH_SOURCE_DIR=$(dirname "$BASH_SOURCE")
source ${BASH_SOURCE_DIR}/script_base.sh

# To get the expected packages, we perform the following transformations on the packages
# from the lock file:
# - Remove the ';' and any markers that follow it
# - Remove any extras from the package name, e.g. '[gcp]'
# - Remove any lines starting with a comment ('#')
# - Remove the pypi line ('-i https://pypi.org/simple')
# - Remove any blank lines
# - Remove pip / setuptools, since they'll always be installed but aren't always in the Pipfile.lock
# - Sort, in case the above transformations affected the sort order
expected=$(pipenv requirements --dev \
    | cut -d';' -f1 \
    | sed 's/\[.*\]//' \
    | sed '/^#/d' \
    | sed '/^-i /d' \
    | sed '/^$/d' \
    | sed '/^pip/d' \
    | sed '/^setuptools/d' \
    | sort) || exit_on_fail
# The installed command gets all installed packages in a requirements format, with the following transformations
# - Replace any underscores with dashes
#   - Note: setuptools replaces any non-alphanumeric/. with a dash but so far we have only seen issues with underscore and
#     this lets us run it on the whole line instead of just the package name. See
#     https://github.com/pypa/setuptools/blob/main/pkg_resources/__init__.py#L1314)
# - Make everything lower case
# - Sort, in case the above affected the sort order
installed=$(pipenv run pip freeze | tr _ - | tr A-Z a-z | sort) || exit_on_fail

# Diff returns 1 if there are differences and >1 if an error occurred. We only want to fail here if there was an actual
# error.
ORIGINAL_ACCEPTABLE_RETURN_CODES=("${ACCEPTABLE_RETURN_CODES[@]}")
ACCEPTABLE_RETURN_CODES=(0 1)
differences=$(diff <(echo "$expected") <(echo "${installed}")) || exit_on_fail
ACCEPTABLE_RETURN_CODES=("${ORIGINAL_ACCEPTABLE_RETURN_CODES[@]}")

# If there are packages from the lock file that are not installed, print an error and fail.
# Note: grep returns 1 if nothing matched
ORIGINAL_ACCEPTABLE_RETURN_CODES=("${ACCEPTABLE_RETURN_CODES[@]}")
ACCEPTABLE_RETURN_CODES=(0 1)
missing=$(echo "$differences" | grep -e '^<' | sed 's/^< //') || exit_on_fail
ACCEPTABLE_RETURN_CODES=("${ORIGINAL_ACCEPTABLE_RETURN_CODES[@]}")
if [[ -n $missing ]]
then
    echo "Your environment is missing or has outdated packages, please run 'pipenv sync --dev'."
    echo "Expected packages or versions that are incorrect:"
    echo "$missing" | indent_output
    run_cmd exit 1
fi

# If there are installed packages that are not in the lock file, print a warning and succeed.
# Note: grep returns 1 if nothing matched
ORIGINAL_ACCEPTABLE_RETURN_CODES=("${ACCEPTABLE_RETURN_CODES[@]}")
ACCEPTABLE_RETURN_CODES=(0 1)
missing=$(echo "$differences" | grep -e '^<' | sed 's/^< //') || exit_on_fail
extra=$(echo "$differences" | grep -e '^>' | sed 's/^> //') || exit_on_fail
ACCEPTABLE_RETURN_CODES=("${ORIGINAL_ACCEPTABLE_RETURN_CODES[@]}")
if [[ -n $extra ]]
then
    echo "Your environment contains extra packages, if these are unexpected 'pip uninstall' them."
    echo "$extra" | indent_output
fi
