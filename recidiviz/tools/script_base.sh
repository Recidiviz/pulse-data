#!/usr/bin/env bash

# Get escape sequence / code that will allow us to make stdout output blue
BLUE_OUTPUT_ESCAPE_CODE=$(tput setaf 4)

# Get the escape sequence to reset from the current color to whatever the default is
RESET_OUTPUT_COLOR_ESCAPE_CODE=$(tput -T ansi sgr0)

# Prepends a blue '> ' to each line of output piped to this function.
#
# In order to use this function, append `2>&1 | indent_output` to all commands so that all output goes to stdout and
# gets run through this function.
function indent_output {
    sed "s/^/$BLUE_OUTPUT_ESCAPE_CODE> $RESET_OUTPUT_COLOR_ESCAPE_CODE /"
}

# Fails the command if any command in a pipeline fails. For example: `a | b | c` will fail if any of commands a, b
# or c fail.
set -o pipefail

# Fail if a variable is unset, instead of substituting a blank.
set -u

ACCEPTABLE_RETURN_CODES=(0)

# Prints a message to stderr, pre-pending the file number and line number of the caller.
function echo_error {
    CALLER_INFO=($(caller))
    CALLER_LINE_NUMBER=${CALLER_INFO[0]}
    CALLER_FILE=${CALLER_INFO[1]}
    ECHO_ARGS=$@
    echo "[$CALLER_FILE:$CALLER_LINE_NUMBER] $ECHO_ARGS" 1>&2;
}

# Checks the return code of the most recent command and prints a message then exits with that return code if it is
# not in the array of acceptable codes. Should be used whenever a command is captured into a variable:
#   MY_VAR=$(some command) || exit_on_fail
function exit_on_fail {
    # Gets the return code from the previous pipeline of commands (will be non-zero if any fail).
    ret_code=$?
    # Check if return code is in set of acceptable return codes
    if [[ ! " ${ACCEPTABLE_RETURN_CODES[@]} " =~ " ${ret_code} " ]]; then
        echo "Failed with exit code $ret_code" 1>&2
        exit ${ret_code}
    fi
}

# All script commands should be run via `run_cmd`, which handles output formatting and exiting on error.
#
# NOTE: This should be used with extreme caution when using pipes (|) or file descriptor redirection (> or >>) as the
# order of operations can be tricky to get right.
function run_cmd {
    # Runs the full array of arguments passed to `run_cmd` as a command, piping output to stdout so we can indent it.
    $@ 2>&1 | indent_output

    exit_on_fail
}

# Prompts the user for a Y/N answer and exits if they do not respond with 'Y' (case insensitive).
function script_prompt {
    prompt=$1

    read -p "$prompt (y/n): " -n 1 -r
    echo
    if [[ ! $REPLY =~ ^[Yy]$ ]]
    then
        exit 1
    fi
}

# Given a list of versions over pipe, temporarily appends '_' to all versions that do not have a '-' so '-alpha.x'
# versions will sort before their corresponding release versions, per the semver.org spec.
# Example:
# $ git tag -l | sort_versions
function sort_versions {
    sed '/-/! s/$/_/' | sort -V  | sed 's/_$//'
}

# Given two semantic version args, checks if the first semantic version is strictly less than the second. See:
# https://stackoverflow.com/questions/4023830/how-to-compare-two-strings-in-dot-separated-version-format-in-bash
function version_less_than {
    min_version=$(echo -e "$1\n$2" | sort_versions | head -n1) || exit_on_fail
    [[ "$1" = "$2" ]] && return 1 || [[  "$1" = "$min_version" ]]
}

function safe_git_checkout_branch {
    BRANCH=$1
    echo "Checking for clean git status"
    if [[ ! -z "$(git status --porcelain)" ]]; then
        echo_error "Git status not clean - please commit or stash changes before retrying."
        exit 1
    fi

    echo "Checking out [$BRANCH]"
    if ! git checkout -t origin/${BRANCH}
    then
        echo "Branch [$BRANCH] already exists - reusing"
        run_cmd git checkout ${BRANCH}
        echo "Pulling latest from [$BRANCH]"
        run_cmd git pull
    fi
}

function check_for_tags_at_branch_tip {
    BRANCH=$1
    ALLOW_ALPHA=${2-}  # Optional argument
    if [[ -z ${ALLOW_ALPHA} ]]; then
        TAGS_AT_TIP_OF_BRANCH=$(git tag --points-at ${BRANCH} | grep -v alpha || echo "") || exit_on_fail
    else
        TAGS_AT_TIP_OF_BRANCH=$(git tag --points-at ${BRANCH}) || exit_on_fail
    fi

    if [[ ! -z ${TAGS_AT_TIP_OF_BRANCH} ]]; then
        echo_error "The tip of branch [$BRANCH] is already tagged - exiting."
        echo_error "Tags: ${TAGS_AT_TIP_OF_BRANCH}"
        exit 1
    fi

}
