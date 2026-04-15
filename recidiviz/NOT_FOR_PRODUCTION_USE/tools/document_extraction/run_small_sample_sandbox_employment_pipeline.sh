#!/bin/bash
# Runs the full employment extraction pipeline on a small entity sample,
# with uploads and extractions each split into 2 segments.
#
# Segments within a phase can be run in parallel (different terminals),
# but each phase must complete before starting the next one.
#
# Usage: edit the variables below, then run:
#   bash recidiviz/NOT_FOR_PRODUCTION_USE/tools/document_extraction/run_small_sample_sandbox_employment_pipeline.sh

set -e

# ──────────────────────────────────────────────────────────────────────────────
# Configuration — edit these before running
# ──────────────────────────────────────────────────────────────────────────────

PROJECT_ID="recidiviz-staging"
STATE_CODE="US_IX"
SANDBOX_PREFIX="anna_small_test_6"
SANDBOX_BUCKET="recidiviz-staging-anna-scratch"

SAMPLE_ENTITY_COUNT="10"
LOOKBACK_DAYS="90"
ACTIVE_IN_COMPARTMENT="SUPERVISION"

TOTAL_SEGMENTS="2"

# ──────────────────────────────────────────────────────────────────────────────

PIPELINE="recidiviz.NOT_FOR_PRODUCTION_USE.tools.document_extraction.run_sandbox_employment_pipeline"

# Common args shared by all phases
COMMON=(
  --project_id "${PROJECT_ID}"
  --state_code "${STATE_CODE}"
  --sandbox_dataset_prefix "${SANDBOX_PREFIX}"
  --sandbox_documents_bucket "${SANDBOX_BUCKET}"
)

# Sampling flags — only used for DOC_UPLOAD phases to select which documents
# to upload. Extraction phases process everything in the metadata table.
SAMPLE_FLAGS=()
[[ -n "${SAMPLE_ENTITY_COUNT:-}" ]]   && SAMPLE_FLAGS+=("--sample_entity_count" "${SAMPLE_ENTITY_COUNT}")
[[ -n "${LOOKBACK_DAYS:-}" ]]         && SAMPLE_FLAGS+=("--lookback_days" "${LOOKBACK_DAYS}")
[[ -n "${ACTIVE_IN_COMPARTMENT:-}" ]] && SAMPLE_FLAGS+=("--active_in_compartment" "${ACTIVE_IN_COMPARTMENT}")

sep() { echo; printf '=%.0s' {1..60}; echo; echo "  $1"; printf '=%.0s' {1..60}; echo; }

# Phase 1: Upload case note docs (sampling flags apply here)
sep "EMPLOYMENT_DOC_UPLOAD (segment 0/${TOTAL_SEGMENTS})"
python -m "${PIPELINE}" "${COMMON[@]}" "${SAMPLE_FLAGS[@]}" --phase EMPLOYMENT_DOC_UPLOAD --segment_index 0 --total_segments "${TOTAL_SEGMENTS}"
sep "EMPLOYMENT_DOC_UPLOAD (segment 1/${TOTAL_SEGMENTS})"
python -m "${PIPELINE}" "${COMMON[@]}" "${SAMPLE_FLAGS[@]}" --phase EMPLOYMENT_DOC_UPLOAD --segment_index 1 --total_segments "${TOTAL_SEGMENTS}"

# Phase 2: Extract employment info (no sampling — extracts all uploaded docs)
sep "EMPLOYMENT_EXTRACTION (segment 0/${TOTAL_SEGMENTS})"
python -m "${PIPELINE}" "${COMMON[@]}" --phase EMPLOYMENT_EXTRACTION --segment_index 0 --total_segments "${TOTAL_SEGMENTS}"
sep "EMPLOYMENT_EXTRACTION (segment 1/${TOTAL_SEGMENTS})"
python -m "${PIPELINE}" "${COMMON[@]}" --phase EMPLOYMENT_EXTRACTION --segment_index 1 --total_segments "${TOTAL_SEGMENTS}"

# Phase 3: Deploy employment extraction views
sep "EMPLOYMENT_VIEW_DEPLOY"
python -m "${PIPELINE}" "${COMMON[@]}" --phase EMPLOYMENT_VIEW_DEPLOY

# Phase 4: Upload ER context docs (no sampling — scoped by extraction results)
sep "EMPLOYER_ER_DOC_UPLOAD (segment 0/${TOTAL_SEGMENTS})"
python -m "${PIPELINE}" "${COMMON[@]}" --phase EMPLOYER_ER_DOC_UPLOAD --segment_index 0 --total_segments "${TOTAL_SEGMENTS}"
sep "EMPLOYER_ER_DOC_UPLOAD (segment 1/${TOTAL_SEGMENTS})"
python -m "${PIPELINE}" "${COMMON[@]}" --phase EMPLOYER_ER_DOC_UPLOAD --segment_index 1 --total_segments "${TOTAL_SEGMENTS}"

# Phase 5: Extract employer entity resolution (no sampling)
sep "EMPLOYER_ER_EXTRACTION (segment 0/${TOTAL_SEGMENTS})"
python -m "${PIPELINE}" "${COMMON[@]}" --phase EMPLOYER_ER_EXTRACTION --segment_index 0 --total_segments "${TOTAL_SEGMENTS}"
sep "EMPLOYER_ER_EXTRACTION (segment 1/${TOTAL_SEGMENTS})"
python -m "${PIPELINE}" "${COMMON[@]}" --phase EMPLOYER_ER_EXTRACTION --segment_index 1 --total_segments "${TOTAL_SEGMENTS}"

# Phase 6: Deploy ER extraction views
sep "EMPLOYER_ER_VIEW_DEPLOY"
python -m "${PIPELINE}" "${COMMON[@]}" --phase EMPLOYER_ER_VIEW_DEPLOY

# Phase 7: Deploy summary views + set table expiration
sep "SUMMARY_VIEW_DEPLOY"
python -m "${PIPELINE}" "${COMMON[@]}" --phase SUMMARY_VIEW_DEPLOY

# Status check
sep "Final status"
python -m "${PIPELINE}" "${COMMON[@]}"

sep "Done!"
