#!/bin/bash

# Script that will run a command over and over until it the command fails.
# Example (running from repo root):
#   ./recidiviz/tools/untilfail.sh pytest recidiviz/tests/ingest/direct/

count=0
while "$@"; do (( count++ )); done
echo "*****************************"
echo Failed after ${count} iterations
echo "*****************************"

