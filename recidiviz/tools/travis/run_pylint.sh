#!/usr/bin/env bash

BASH_SOURCE_DIR=$(dirname "$BASH_SOURCE")
source ${BASH_SOURCE_DIR}/../script_base.sh

merge_base_hash=$(git merge-base master HEAD)
current_head_hash=$(git rev-parse HEAD)
current_head_branch_name=$(git rev-parse --abbrev-ref HEAD)

# Returns all files with updates that are not deletions
changed_files_cmd="git diff --diff-filter=d --name-only $(git merge-base HEAD master)"
changed_files=$(${changed_files_cmd})

# Look for changes in Pipfile.lock and .pylintrc - changes in these files could mean that python files that have not
# been touched now have new lint errors.
pylint_config_files_in_change_list=$(${changed_files_cmd} | grep -e Pipfile.lock -e .pylintrc)

if [[ -n "${pylint_config_files_in_change_list}" ]]
then
    pylint_config_may_have_changed=true
else
    pylint_config_may_have_changed=false
fi

# Sum the exit codes throughout the file so that we can run all checks and still exit if only the first one fails.
exit_code=0

# Pylint check
if [[ ${merge_base_hash} == ${current_head_hash} || ${pylint_config_may_have_changed} == true ]]
then
    echo "Running pylint for branch $current_head_branch_name. Pylint config changed=[$pylint_config_may_have_changed]";
    pylint recidiviz
else
    changed_python_files=$(${changed_files_cmd} | grep '.*\.py$')
    if [[ -z ${changed_python_files} ]]
    then
        echo "Found no python files changed - not running pylint."
    else
        echo "Running differential pylint for branch [$current_head_branch_name]";
        echo "Changed files:"
        echo "${changed_python_files}"

        pylint ${changed_python_files}
    fi
fi
exit_code=$((exit_code + $?))

# TODO format check
echo "Checking for TODO format"
# This set of commands does the following:
# - List all files with updates that are not deletions
# - Skips 'run_pylint.sh' as it is allowed to have 'TODO' that doesn't match the format
# - Runs grep for each updated file, getting all lines containing 'TODO' (and including the line number in the output via -n)
# - Filters to only the lines that don't contain 'TODO' with the correct format
invalid_lines=$(${changed_files_cmd} | grep --invert-match -e 'run_pylint\.sh' | xargs grep -n -e 'TODO' | grep --invert-match -e 'TODO(#[0-9]\+)')
if [[ -n ${invalid_lines} ]]
then
    echo "TODOs must be of format TODO(#123)"
    echo "${invalid_lines}" | indent_output
    exit_code=$((exit_code + 1))
else
    echo "All TODOs match format"
fi

exit $exit_code
