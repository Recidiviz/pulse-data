#!/bin/bash
# Runs the full payment info extraction pipeline in a sandbox.
#
# Stages (comment out any stage to skip it):
#   1a. Doc store refresh         (US_IX_SUPERVISION_CASE_NOTES)
#   1b. Extraction + view deploy  (US_IX_CASE_NOTE_PAYMENT_INFO)
#   2.  Summary views             (payment sessions + current_payment_status_summary)
#
# Note: payment info has no entity resolution stage.
#
# Usage: edit the variables below, then run:
#   bash recidiviz/NOT_FOR_PRODUCTION_USE/tools/document_extraction/run_sandbox_payment_pipeline.sh

set -e

# ──────────────────────────────────────────────────────────────────────────────
# Configuration — edit these before running
# ──────────────────────────────────────────────────────────────────────────────

PROJECT_ID="recidiviz-staging"
SANDBOX_PREFIX="mayukas_ix_batch"
SANDBOX_BUCKET="recidiviz-staging-anna-scratch"

DOC_COLLECTION_NAME="US_IX_SUPERVISION_CASE_NOTES"
EXTRACTOR_ID="US_IX_CASE_NOTE_PAYMENT_INFO"

SAMPLE_ENTITY_COUNT="10"
LOOKBACK_DAYS="90"
ACTIVE_IN_COMPARTMENT="SUPERVISION"

# LLM mode: fake | concurrent | batch
MODE="concurrent"

# ──────────────────────────────────────────────────────────────────────────────
# Helpers
# ──────────────────────────────────────────────────────────────────────────────

DOC_REFRESH="recidiviz.NOT_FOR_PRODUCTION_USE.tools.document_extraction.refresh_sandbox_document_collection"
EXTRACTION="recidiviz.NOT_FOR_PRODUCTION_USE.tools.document_extraction.run_sandbox_document_extraction_job"
SUMMARY="recidiviz.NOT_FOR_PRODUCTION_USE.tools.document_extraction.deploy_sandbox_summary_views"

SAMPLE_FLAGS=()
[[ -n "${SAMPLE_ENTITY_COUNT:-}" ]]   && SAMPLE_FLAGS+=("--sample_entity_count" "${SAMPLE_ENTITY_COUNT}")
[[ -n "${LOOKBACK_DAYS:-}" ]]         && SAMPLE_FLAGS+=("--lookback_days" "${LOOKBACK_DAYS}")
[[ -n "${ACTIVE_IN_COMPARTMENT:-}" ]] && SAMPLE_FLAGS+=("--active_in_compartment" "${ACTIVE_IN_COMPARTMENT}")

MODE_FLAGS=("${MODE}")
[[ "${MODE}" == "batch" ]] && MODE_FLAGS+=("--sandbox_llm_job_artifact_bucket" "${SANDBOX_BUCKET}")

sep() { echo; printf '=%.0s' {1..60}; echo; echo "  $1"; printf '=%.0s' {1..60}; echo; }

# ──────────────────────────────────────────────────────────────────────────────

sep "STAGE 1a: Doc store refresh (${DOC_COLLECTION_NAME})"
python -m "${DOC_REFRESH}" \
  --project_id "${PROJECT_ID}" \
  --collection_name "${DOC_COLLECTION_NAME}" \
  --sandbox_dataset_prefix "${SANDBOX_PREFIX}" \
  --sandbox_bucket "${SANDBOX_BUCKET}" \
  "${SAMPLE_FLAGS[@]}"

sep "STAGE 1b: Extraction (${EXTRACTOR_ID})"
python -m "${EXTRACTION}" \
  --project_id "${PROJECT_ID}" \
  --extractor_id "${EXTRACTOR_ID}" \
  --sandbox_dataset_prefix "${SANDBOX_PREFIX}" \
  --sandbox_documents_bucket "${SANDBOX_BUCKET}" \
  "${SAMPLE_FLAGS[@]}" \
  "${MODE_FLAGS[@]}"

sep "STAGE 2: Payment summary views"
python -m "${SUMMARY}" \
  --project_id "${PROJECT_ID}" \
  --sandbox_dataset_prefix "${SANDBOX_PREFIX}" \
  --collection_name "CASE_NOTE_PAYMENT_INFO"

EXPIRATION_DAYS=90
EXPIRATION_SECS=$(( EXPIRATION_DAYS * 24 * 60 * 60 ))

SANDBOX_DATASETS=(
  "${SANDBOX_PREFIX}_document_extraction_metadata"
  "${SANDBOX_PREFIX}_document_extraction_results__raw"
  "${SANDBOX_PREFIX}_document_extraction_results__validated"
  "${SANDBOX_PREFIX}_document_extraction_results__exclusions"
  "${SANDBOX_PREFIX}_document_extraction_results"
  "${SANDBOX_PREFIX}_document_store_metadata"
  "${SANDBOX_PREFIX}_llm_extraction_views"
)

update_expiration() {
  local dataset="$1"
  if ! bq show "${PROJECT_ID}:${dataset}" >/dev/null 2>&1; then
    echo "  Skipping ${dataset} (not found)"
    return 0
  fi
  echo "  Updating ${dataset}..."
  bq update --default_table_expiration "${EXPIRATION_SECS}" "${PROJECT_ID}:${dataset}"
  bq ls --max_results 1000 "${PROJECT_ID}:${dataset}" \
    | tail -n +3 | awk '{print $1}' \
    | while read -r tbl; do
        if [[ -n "${tbl}" ]]; then
          bq update --expiration "${EXPIRATION_SECS}" "${PROJECT_ID}:${dataset}.${tbl}" \
            >/dev/null 2>&1 || true
        fi
      done
}

sep "Extending table expiration to ${EXPIRATION_DAYS} days"
for ds in "${SANDBOX_DATASETS[@]}"; do
  update_expiration "${ds}"
done

sep "Payment pipeline complete!"
