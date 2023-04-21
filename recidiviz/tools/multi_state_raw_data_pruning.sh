#!/bin/bash
#  Usage: bash recidiviz/tools/multi_state_raw_data_pruning.sh

declare -a states=("US_TN" "US_MI" "US_ND")

for state in "${states[@]}"
do
  echo "Starting raw data pruning in STAGING for $state"
  python -m recidiviz.tools.ingest.one_offs.clear_redundant_raw_data_on_bq --dry-run False --project-id=recidiviz-staging --state-code="$state"
  echo "Starting raw data pruning in PROD for $state"
  python -m recidiviz.tools.ingest.one_offs.clear_redundant_raw_data_on_bq --dry-run False --project-id=recidiviz-123 --state-code="$state"
done
echo "Deleting temporary output files..."
rm prune-prod.txt
rm prune-staging.txt

echo "Done with raw data pruning script for the following states: ${states[*]}."
