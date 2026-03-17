#!/bin/bash
# TODO(#12390): Delete this script once automatic pruning is implemented.
# Usage: bash recidiviz/tools/multi_state_raw_data_pruning.sh
BASH_SOURCE_DIR=$(dirname "${BASH_SOURCE[0]}")
# shellcheck source=recidiviz/tools/script_base.sh
source "${BASH_SOURCE_DIR}/script_base.sh"

# Please keep this declaration on a single line
declare -a states=("US_TN" "US_MI" "US_ND" "US_AR")

if ! gcloud auth application-default print-access-token > /dev/null 2>&1; then
    echo "No Google Application Default Credentials found or they are expired. Please run:" >&2
    echo "" >&2
    echo "  gcloud auth login --update-adc" >&2
    exit 1
fi

for state in "${states[@]}"
do
  echo "Starting raw data pruning in STAGING for $state"
  python -m recidiviz.tools.ingest.one_offs.clear_redundant_raw_data_on_bq --dry-run False --project-id=recidiviz-staging --state-code="$state" || exit_on_fail
  echo "Starting raw data pruning in PROD for $state"
  python -m recidiviz.tools.ingest.one_offs.clear_redundant_raw_data_on_bq --dry-run False --project-id=recidiviz-123 --state-code="$state" || exit_on_fail
done
echo "Deleting temporary output files..."
rm prune-prod.txt
rm prune-staging.txt

echo "Done with raw data pruning script for the following states: ${states[*]}."
