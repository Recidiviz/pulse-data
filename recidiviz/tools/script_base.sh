#!/usr/bin/env bash

#
# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2023 Recidiviz, Inc.
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program.  If not, see <https://www.gnu.org/licenses/>.
# =============================================================================
#

# Get escape sequence / code that will allow us to make stdout output blue
BLUE_OUTPUT_ESCAPE_CODE=$(tput setaf 4)

# Get the escape sequence to reset from the current color to whatever the default is
RESET_OUTPUT_COLOR_ESCAPE_CODE=$(tput -T ansi sgr0)

# Prepends a blue '> ' to each line of output piped to this function.
#
# In order to use this function, append `2>&1 | indent_output` to all commands so that all output goes to stdout and
# gets run through this function.
function indent_output {
    sed -u "s/^/$BLUE_OUTPUT_ESCAPE_CODE> $RESET_OUTPUT_COLOR_ESCAPE_CODE /"
}

# Fails the command if any command in a pipeline fails. For example: `a | b | c` will fail if any of commands a, b
# or c fail.
set -o pipefail

# Fail if a variable is unset, instead of substituting a blank.
set -u

ACCEPTABLE_RETURN_CODES=(0)

# Prints a message to stderr, pre-pending the file number and line number of the caller.
function echo_error {
    declare -a CALLER_INFO
    read -r -a CALLER_INFO < <(caller)
    CALLER_LINE_NUMBER=${CALLER_INFO[0]}
    CALLER_FILE=${CALLER_INFO[1]}
    echo "[$CALLER_FILE:$CALLER_LINE_NUMBER] $*" 1>&2;
}

# Checks the return code of the most recent command and prints a message then exits with that return code if it is
# not in the array of acceptable codes. Should be used whenever a command is captured into a variable:
#   MY_VAR=$(some command) || exit_on_fail
function exit_on_fail {
    # Gets the return code from the previous pipeline of commands (will be non-zero if any fail).
    ret_code=$?
    # Check if return code is in set of acceptable return codes
    if [[ ! " ${ACCEPTABLE_RETURN_CODES[*]} " == *" ${ret_code} "* ]]; then
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
    echo "[$(date '+%Y-%m-%d %T')] Running: $*"
    "$@" 2>&1 | indent_output

    exit_on_fail
}

# This is very similar to run_cmd except it allows for the user to decide how to exit when the command fails.
function run_cmd_no_exiting {
    # Runs the full array of arguments passed (similar to `run_cmd`) as a command, piping output to stdout so we can indent it.
    "$@" 2>&1 | indent_output
}

# Prompts the user for a Y/N answer and exits if they respond with 'N' (case insensitive).
function script_prompt {
    prompt=$1

    while true
    do
      read -p "$prompt (y/n): " -r

      if [[ $REPLY =~ ^[Nn]$ ]]
      then
        echo "Responded with (${REPLY}). Exiting."
        exit 1
      elif [[ ! $REPLY =~ ^[Yy]$ ]]
      then
        echo "Invalid input (${REPLY})."
      else
        break
      fi

      echo
    done
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
    if [[ "$1" = "$2" ]]; then
      return 1
    elif [[ "$2" = "$min_version" ]]; then
      return 1
    fi

    return 0
}

function verify_clean_git_status {
    LOCAL_REPO_PATH=${1:-$(git rev-parse --show-toplevel)}
    if [[ -n "$(git -C "$LOCAL_REPO_PATH" status --porcelain)" ]]; then
        echo_error "Git status not clean - please stash changes before retrying."
        echo_error "If you have made a local change to fix a deploy issue that cannot be solved by reverting a recent "
        echo_error "commit (e.g. if you have found an old bug in the script itself) AND you need to deploy "
        echo_error "urgently, you can comment this whole if-block out before deploying. Do not commit changes locally "
        echo_error "to suppress this error."
        echo_error "!! Please notify the #eng channel if you have to deploy with dirty local status. !!"
        exit 1
    fi
}

function safe_git_checkout_remote_branch {
    BRANCH=$1
    LOCAL_REPO_PATH=${2:-$(git rev-parse --show-toplevel)}
    echo "Checking for clean git status"
    verify_clean_git_status "$LOCAL_REPO_PATH"

    echo "Checking out [$BRANCH]"
    if ! git -C "$LOCAL_REPO_PATH" checkout -t "origin/${BRANCH}"
    then
        echo "Branch [$BRANCH] already exists - reusing"
        run_cmd git -C "$LOCAL_REPO_PATH" checkout "${BRANCH}"

        echo "Checking local commit exists on origin/${BRANCH}"
        # Queries for branches that contain the commit at HEAD, then searches for the remote branch in that list
        HEAD_EXISTS_ON_REMOTE_RESULT=$(git -C "$LOCAL_REPO_PATH" branch --remotes --contains HEAD | grep "origin/${BRANCH}")
        if [[ -z ${HEAD_EXISTS_ON_REMOTE_RESULT} ]]; then
            echo "origin/${BRANCH}" does not contain HEAD
            echo_error "Remote branch origin/${BRANCH} does not contain the commit at HEAD."
            echo_error "Please revert any local commit history before continuing."
            echo_error "!! DO NOT COMMENT OUT THIS BLOCK. IF YOU RUN THIS SCRIPT ON A COMMIT THAT IS NOT IN THE MAIN "
            echo_error "!! BRANCH, IT WILL BREAK FUTURE RUNS OF THIS SCRIPT BECAUSE THEY WILL NOT BE ABLE TO DETERMINE "
            echo_error "!! THE PROPER NEXT TAG TO DEPLOY. IF YOU MUST RUN WITH LOCAL CHANGES TO FIX AN ISSUE WITH THE "
            echo_error "!! DEPLOY SCRIPT ITSELF, APPLY THE CHANGES AS UN-COMMITTED UPDATES, THEN COMMENT OUT THE BLOCK "
            echo_error "!! THAT COMPLAINS ABOUT A DIRTY GIT STATUS."
            exit 1
        fi

        echo "Pulling latest from [$BRANCH]"
        run_cmd git -C "$LOCAL_REPO_PATH" pull
    fi
}

function check_for_tags_at_branch_tip {
    BRANCH=$1
    ALLOW_ALPHA=${2-}  # Optional argument
    if [[ -n ${ALLOW_ALPHA} ]]; then
        TAGS_AT_TIP_OF_BRANCH=$(git tag --points-at "${BRANCH}" | grep '^v\d\+\.' | grep -v alpha || echo "") || exit_on_fail
    else
        TAGS_AT_TIP_OF_BRANCH=$(git tag --points-at "${BRANCH}" | grep '^v\d\+\.' || echo "") || exit_on_fail
    fi

    if [[ -n ${TAGS_AT_TIP_OF_BRANCH} ]]; then
        echo_error "The tip of branch [$BRANCH] is already tagged - exiting."
        echo_error "Tags: ${TAGS_AT_TIP_OF_BRANCH}"
        echo_error "If you believe this tag exists due a previously failed deploy attempt and you want to retry the "
        echo_error "deploy for this version, run \`git push --delete origin <tag name>\` to delete the old tag from"
        echo_error "remote, if it exists, and \`git tag -D <tag name>\` to delete it locally."
        exit 1
    fi

}


function check_docker_running {
    if [[ -z $(which docker) ]]; then
        echo_error "Docker not installed. Please follow instructions in repo README to install."
        echo_error "Also make sure you've configured gcloud docker permissions with:"
        echo_error "    $ gcloud auth login"
        echo_error "    $ gcloud auth configure-docker"
        exit 1
    fi

    if ! docker info > /dev/null 2>&1; then
        echo_error "The docker daemon doesn't seem to be running. Please start it before continuing the script."
        exit 1
    fi
}

function verify_hash {
    EXPECTED_HASH=$1
    LOCAL_REPO_PATH=${2:-$(git rev-parse --show-toplevel)}
    CURRENT_HASH=$(git -C "$LOCAL_REPO_PATH" rev-parse HEAD) || exit_on_fail

    verify_clean_git_status "$LOCAL_REPO_PATH"

    if [[ "$CURRENT_HASH" != "$EXPECTED_HASH" ]]; then
        echo_error "Current commit [${CURRENT_HASH:0:7}] does not match expected commit "
        echo_error "[${EXPECTED_HASH:0:7}]. Please restart at the correct commit."
        exit 1
    fi
}


function get_secret () {
  PROJECT_ID=$1
  SECRET_NAME=$2
  gcloud secrets versions access latest --project="${PROJECT_ID}" --secret="${SECRET_NAME}" || exit_on_fail
}




function select_one () {
  echo "${1}: " >&2

  select SELECTED in "${@:2}"
  do
    if [ -z "$SELECTED" ]; then
        echo "Invalid input. Enter a number." >&2
    else
      break
    fi
  done

  echo "${SELECTED}"
}
