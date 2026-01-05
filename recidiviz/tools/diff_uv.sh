#!/bin/bash

# This checks to ensure your uv environment is in sync with the lock file.
# 'initial_setup.sh' sets this up to run every time you pull a new version on main or checkout a branch.

BASH_SOURCE_DIR=$(dirname "${BASH_SOURCE[0]}")
# shellcheck source=recidiviz/tools/script_base.sh
source "${BASH_SOURCE_DIR}/script_base.sh"

# Use uv sync with --frozen and --dry-run to check if environment is in sync.
# --frozen ensures we don't update the lock file
# --dry-run shows what would be done without making changes
sync_output=$(uv sync --all-extras --frozen --dry-run 2>&1)
sync_exit_code=$?

if [ $sync_exit_code -ne 0 ]; then
    echo "uv sync check failed:"
    echo "$sync_output" | indent_output
    exit $sync_exit_code
fi

# Check if there are any packages that would be installed/upgraded/removed
if echo "$sync_output" | grep -qE "(Would install|Would uninstall|Would upgrade)"; then
    echo "Your environment is out of sync with uv.lock."
    echo "Please run 'uv sync --all-extras' to update your environment."
    echo ""
    echo "Changes that would be made:"
    echo "$sync_output" | grep -E "(Would install|Would uninstall|Would upgrade)" | indent_output
    exit 1
fi

echo "Environment is in sync with uv.lock"
