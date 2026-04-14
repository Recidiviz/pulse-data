#!/bin/bash
# Runs extraction for supervision-related collections for selected states.
#
# Collections:
#   - CASE_NOTE_EMPLOYMENT_INFO
#   - CASE_NOTE_HOUSING_INFO
#   - CASE_NOTE_PAYMENT_INFO
#
# States: US_AZ, US_CO, US_ME, US_NC, US_NE
#
# Each collection run handles document store refresh + extraction for the
# specified states automatically.
#
# Prerequisites:
#   - gcloud auth application-default login
#   - Python environment activated (uv run or source .venv/bin/activate)

set -euo pipefail

PROJECT_ID="recidiviz-staging"
SANDBOX_PREFIX="mayukas"
BUCKET="recidiviz-staging-anna-scratch"
LOOKBACK_DAYS=90
SAMPLE_ENTITY_COUNT=10
STATE_CODES="US_AZ,US_CO,US_ME,US_NC,US_NE"

COLLECTIONS=(
    # "CASE_NOTE_EMPLOYMENT_INFO"
    # "CASE_NOTE_HOUSING_INFO"
    "CASE_NOTE_PAYMENT_INFO"
)

for collection in "${COLLECTIONS[@]}"; do
    echo "============================================================"
    echo "Running collection: ${collection}"
    echo "============================================================"

    python -m recidiviz.NOT_FOR_PRODUCTION_USE.tools.document_extraction.run_sandbox_collection_extraction_job \
        --project_id "${PROJECT_ID}" \
        --collection_name "${collection}" \
        --sandbox_dataset_prefix "${SANDBOX_PREFIX}" \
        --sandbox_documents_bucket "${BUCKET}" \
        --state_codes "${STATE_CODES}" \
        --sample_entity_count_per_state "${SAMPLE_ENTITY_COUNT}" \
        --active_in_compartment SUPERVISION \
        --lookback_days "${LOOKBACK_DAYS}" \
        concurrent

    echo ""
    echo "Finished: ${collection}"
    echo ""
done

echo "All collections complete."
