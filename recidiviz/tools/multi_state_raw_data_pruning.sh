#!/bin/bash
#  Usage: bash recidiviz/tools/multi_state_raw_data_pruning.sh

declare -a states=("US_TN" "US_MI" "US_ID" "US_ND")

echo "Running DRY-RUN raw data pruning script in STAGING for the following states: ${states[*]}."
echo "Output can be found in prune-staging.txt."
for state in "${states[@]}"
do
  echo "Starting DRY-RUN in STAGING for $state"
  python -m recidiviz.tools.ingest.one_offs.clear_redundant_raw_data_on_bq --dry-run True --project-id=recidiviz-staging --state-code="$state" >> prune-staging.txt
done

echo "Running DRY-RUN raw data pruning script in PROD for the following states: ${states[*]}."
echo "Output can be found in prune-prod.txt."
for state in "${states[@]}"
do
  echo "Starting DRY-RUN in PROD for $state"
  python -m recidiviz.tools.ingest.one_offs.clear_redundant_raw_data_on_bq --dry-run True --project-id=recidiviz-123 --state-code="$state" >> prune-prod.txt
done

read -p "Look at prune-staging.txt and prune-prod.txt and confirm that dry-run output look correct. Are you ready to proceed with raw data pruning? " -n 1 -r
echo
if [[ $REPLY =~ ^[Yy]$ ]]
then
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
fi
