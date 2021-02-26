#!/bin/bash

# Script to automatically generate all region ingest documentation on commit if direct ingest files have been touched

GIT_DIR=$(git rev-parse --git-dir)
REPO_GIT_DIR=${GIT_DIR%%/.git/*}
REPO_ROOT_DIR=$(dirname "$REPO_GIT_DIR")

source ${REPO_ROOT_DIR}/recidiviz/tools/script_base.sh


if git diff --cached --name-only | grep --quiet "recidiviz/ingest/direct/regions/"
then
  echo "Generating documentation..."
  run_cmd pipenv run python -m recidiviz.tools.docs.documentation_generator
fi
