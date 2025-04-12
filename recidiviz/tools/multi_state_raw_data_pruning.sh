#!/bin/bash
#  Usage: bash recidiviz/tools/multi_state_raw_data_pruning.sh
BASH_SOURCE_DIR=$(dirname "${BASH_SOURCE[0]}")
# shellcheck source=recidiviz/tools/script_base.sh
source "${BASH_SOURCE_DIR}/script_base.sh"

declare -a states=("US_TN" "US_MI" "US_ND" "US_OR" "US_AR")

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
