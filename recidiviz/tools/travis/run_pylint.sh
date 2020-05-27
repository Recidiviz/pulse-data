#!/usr/bin/env bash

merge_base_hash=$(git merge-base master HEAD)
current_head_hash=$(git rev-parse HEAD)
current_head_branch_name=$(git rev-parse --abbrev-ref HEAD)
changed_files_cmd="git diff --name-only $(git merge-base HEAD master)"
changed_files=$(${changed_files_cmd})

# Look for changes in Pipfile.lock and .pylintrc - changes in these files could mean that python files that have not
# been touched now have new lint errors.
pylint_config_files_in_change_list=$(${changed_files_cmd} | grep -e Pipfile.lock -e .pylintrc)

if [[ -n "${pylint_config_files_in_change_list}" ]]
then
    pylint_config_may_have_changed=true
fi

if [[ ${merge_base_hash} == ${current_head_hash} || ${pylint_config_may_have_changed} == true ]]
then
    echo "Running pylint for branch $current_head_branch_name. Pylint config changed=[$pylint_config_may_have_changed]";
    pylint recidiviz;
else
    changed_python_files=$(${changed_files_cmd} | grep '.*\.py$')
    if [[ -z ${changed_python_files} ]]
    then
        echo "Found no python files changed - not running pylint."
        exit 0
    fi

    echo "Running differential pylint for branch $current_head_branch_name";
    pylint ${changed_python_files};
fi
