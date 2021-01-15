#!/usr/bin/env bash

BASH_SOURCE_DIR=$(dirname "$BASH_SOURCE")
source ${BASH_SOURCE_DIR}/../recidiviz/tools/script_base.sh

echo "Setting up helperbot@recidiviz.com as git user"
run_cmd git config user.email "helperbot@recidiviz.com"
run_cmd git config user.name "Helper Bot"

echo "Saving helperbot git credentials"
run_cmd `echo "https://helperbot-recidiviz:${GH_TOKEN}@github.com" > .git/credentials`

echo "Pulling the copybara image"
run_cmd docker pull sharelatex/copybara:2019-08.01

echo "Running copybara"

# Note: Copybara returns exit code 4 if the command was a no-op. This will
# happen if the change only modified files that are excluded from the public
# mirror. In this case we still want the script to pass, so we modify the set
# of exit codes `run_cmd` will allow, so we only exit if it is not 0 or 4.
ORIGINAL_ACCEPTABLE_RETURN_CODES=("${ACCEPTABLE_RETURN_CODES[@]}")
ACCEPTABLE_RETURN_CODES=(0 4)
# TODO(#5305) Remove `--ignore-noop` flag
run_cmd docker run -e COPYBARA_CONFIG='mirror/copy.bara.sky' \
           -e COPYBARA_WORKFLOW='exportSourceToPublic' \
           -e COPYBARA_OPTIONS='--ignore-noop' \
           -v "$(pwd)/.git/config":/root/.gitconfig \
           -v "$(pwd)/.git/credentials":/root/.git-credentials \
           -v "$(pwd)":/usr/src/app \
           -it sharelatex/copybara:2019-08.01 copybara
ACCEPTABLE_RETURN_CODES=("${ORIGINAL_ACCEPTABLE_RETURN_CODES[@]}")

echo "Cleaning up git state so helperbot is no longer the credentialed user"
run_cmd git config --unset user.email
run_cmd git config --unset user.name
run_cmd rm .git/credentials
