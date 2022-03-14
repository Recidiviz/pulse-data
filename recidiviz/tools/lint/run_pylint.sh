#!/usr/bin/env bash

#
#  Recidiviz - a data platform for criminal justice reform
#  Copyright (C) 2021 Recidiviz, Inc.
#
#  This program is free software: you can redistribute it and/or modify
#  it under the terms of the GNU General Public License as published by
#  the Free Software Foundation, either version 3 of the License, or
#  (at your option) any later version.
#
#  This program is distributed in the hope that it will be useful,
#  but WITHOUT ANY WARRANTY; without even the implied warranty of
#  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
#  GNU General Public License for more details.
#
#  You should have received a copy of the GNU General Public License
#  along with this program.  If not, see <https://www.gnu.org/licenses/>.
#  =============================================================================
#
#

BASH_SOURCE_DIR=$(dirname "$BASH_SOURCE")
source ${BASH_SOURCE_DIR}/../script_base.sh

merge_base_hash=$(git merge-base origin/main HEAD)|| exit_on_fail
current_head_hash=$(git rev-parse HEAD)|| exit_on_fail
current_head_branch_name=$(git rev-parse --abbrev-ref HEAD)|| exit_on_fail

# Returns all files with updates that are not deletions
changed_files_cmd="git diff --diff-filter=d --name-only $(git merge-base HEAD origin/main)" || exit_on_fail
changed_files=$(${changed_files_cmd}) || exit_on_fail

# Look for changes in Pipfile.lock and .pylintrc - changes in these files could mean that python files that have not
# been touched now have new lint errors.
pylint_config_files_in_change_list=$(${changed_files_cmd} | grep -e Pipfile.lock -e .pylintrc -e run_pylint.sh)

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
    echo "Running FULL pylint for branch $current_head_branch_name. Pylint config changed=[$pylint_config_may_have_changed]";
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
# - Skips `find*todos.py/yml`, `issue_references.py`, `bandit-baseline.json`, `run_pylint.sh`, `.pylintrc`, or any templates as they can have 'TODO' that doesn't match the format
# - Runs grep for each updated file, getting all lines containing 'TODO' (and including the line number in the output via -n)
# - Filters to only the lines that don't contain 'TODO' with the correct format
invalid_lines=$(${changed_files_cmd} \
    | grep --invert-match -e 'find-\(closed\|linked\)-todos.yml' \
    | grep --invert-match -e 'find_todos.py' \
    | grep --invert-match -e 'issue_references.py' \
    | grep --invert-match -e 'bandit-baseline.json' \
    | grep --invert-match -e 'run_pylint\.sh' \
    | grep --invert-match -e '\.pylintrc' \
    | grep --invert-match -e '/templates/' \
    | xargs grep -n -e 'TODO' \
    | grep --invert-match -e 'TODO(\(\(.*#[0-9]\+\)\|\(http.*\)\))')

if [[ -n ${invalid_lines} ]]
then
    echo "TODOs must be of format TODO(#000), TODO(organization/repo#000), or TODO(https://issues.com/000)"
    echo "${invalid_lines}" | indent_output
    exit_code=$((exit_code + 1))
else
    echo "All TODOs match format"
fi

# TODO(#10252): Once we have a linter setup for SQL, see if it can perform this check for us.
# CURRENT_DATE format check
echo "Checking CURRENT_DATE format"
# This set of commands does the following:
# - List all files with updates that are not deletions
# - Skips 'run_pylint.sh', `bandit-baseline.json` as it is allowed to have 'CURRENT_DATE' that doesn't match the format
# - Runs grep for each updated file, getting all lines containing 'CURRENT_DATE()' (and including the line number in the output via -n)
invalid_lines=$(${changed_files_cmd} \
    | grep --invert-match -e 'run_pylint\.sh' \
    | grep --invert-match -e 'bandit-baseline.json' \
    | xargs grep -n -e 'CURRENT_DATE()')

if [[ -n ${invalid_lines} ]]
then
    echo "CURRENT_DATE must include a timezone"
    echo "${invalid_lines}" | indent_output
    exit_code=$((exit_code + 1))
else
    echo "All CURRENT_DATE functions match format"
fi

# Checking no use of attr.evolve in entity normalization
echo "Checking no attr.evolve in entity normalization"
# This set of commands does the following:
# - List all files with updates that are not deletions
# - Filters to just files that perform entity normalization
# - Runs grep for each relevant updated file, getting all lines that contain calls to attr.evolve()
invalid_lines=$(${changed_files_cmd} \
    | grep -e 'recidiviz/calculator/pipeline/' \
    | grep -e 'normalization' \
    | xargs grep -n -e 'attr.evolve(')

if [[ -n ${invalid_lines} ]]
then
    echo "Must use deep_entity_update function instead of attr.evolve in entity_normalization files."
    echo "${invalid_lines}" | indent_output
    exit_code=$((exit_code + 1))
else
    echo "No invalid uses of attr.evolve()."
fi

# Checking no use of id() in entity normalization
echo "Checking no id() in entity normalization"
# This set of commands does the following:
# - List all files with updates that are not deletions
# - Filters to just files that perform entity normalization
# - Runs grep for each relevant updated file, getting all lines that contain calls to id()
invalid_lines=$(${changed_files_cmd} \
    | grep -e 'recidiviz/calculator/pipeline/' \
    | grep -e 'normalization' \
    | grep --invert-match -e 'normalized_entities_utils.py' \
    | xargs grep -n -e ' id(' -e '=id(')

if [[ -n ${invalid_lines} ]]
then
    echo "Must use update_normalized_entity_with_globally_unique_id function instead
    of id() in entity_normalization files. Usage of id() to set the entity id on an
    object is disallowed in entity normalization because it does not produce unique
    id values that can be persisted to BigQuery."
    echo "${invalid_lines}" | indent_output
    exit_code=$((exit_code + 1))
else
    echo "No invalid uses of id()."
fi

exit $exit_code
