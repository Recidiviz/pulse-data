#!/bin/bash
# Runs the full employment extraction pipeline on the full sandbox dataset
# (no entity sampling, 365-day lookback, no compartment filter).
#
# Phase 1 (employment doc upload + extraction) is split into 10 segments.
# Phase 2 (ER context upload + extraction) is split into 5 segments.
#
# Comment out individual phase/segment lines to skip them.
# Segments within a phase can be run in parallel (different terminals),
# but each phase must complete before starting the next one.
#
# Usage: edit the variables below, then run:
#   bash recidiviz/NOT_FOR_PRODUCTION_USE/tools/document_extraction/run_full_sandbox_employment_pipeline.sh

set -e

# ──────────────────────────────────────────────────────────────────────────────
# Configuration — edit these before running
# ──────────────────────────────────────────────────────────────────────────────

PROJECT_ID="recidiviz-staging"
STATE_CODE="US_IX"
SANDBOX_PREFIX="mayukas_ix_full"
SANDBOX_BUCKET="recidiviz-staging-anna-scratch"

SAMPLE_ENTITY_COUNT=""
LOOKBACK_DAYS="365"
ACTIVE_IN_COMPARTMENT=""

PHASE_1_SEGMENTS="10"  # EMPLOYMENT_DOC_UPLOAD + EMPLOYMENT_EXTRACTION
PHASE_2_SEGMENTS="5"   # EMPLOYER_ER_DOC_UPLOAD + EMPLOYER_ER_EXTRACTION

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
sep "EMPLOYMENT_DOC_UPLOAD (segment 0/${PHASE_1_SEGMENTS})"
python -m "${PIPELINE}" "${COMMON[@]}" "${SAMPLE_FLAGS[@]}" --phase EMPLOYMENT_DOC_UPLOAD --segment_index 0 --total_segments "${PHASE_1_SEGMENTS}"
sep "EMPLOYMENT_DOC_UPLOAD (segment 1/${PHASE_1_SEGMENTS})"
python -m "${PIPELINE}" "${COMMON[@]}" "${SAMPLE_FLAGS[@]}" --phase EMPLOYMENT_DOC_UPLOAD --segment_index 1 --total_segments "${PHASE_1_SEGMENTS}"
sep "EMPLOYMENT_DOC_UPLOAD (segment 2/${PHASE_1_SEGMENTS})"
python -m "${PIPELINE}" "${COMMON[@]}" "${SAMPLE_FLAGS[@]}" --phase EMPLOYMENT_DOC_UPLOAD --segment_index 2 --total_segments "${PHASE_1_SEGMENTS}"
sep "EMPLOYMENT_DOC_UPLOAD (segment 3/${PHASE_1_SEGMENTS})"
python -m "${PIPELINE}" "${COMMON[@]}" "${SAMPLE_FLAGS[@]}" --phase EMPLOYMENT_DOC_UPLOAD --segment_index 3 --total_segments "${PHASE_1_SEGMENTS}"
sep "EMPLOYMENT_DOC_UPLOAD (segment 4/${PHASE_1_SEGMENTS})"
python -m "${PIPELINE}" "${COMMON[@]}" "${SAMPLE_FLAGS[@]}" --phase EMPLOYMENT_DOC_UPLOAD --segment_index 4 --total_segments "${PHASE_1_SEGMENTS}"
sep "EMPLOYMENT_DOC_UPLOAD (segment 5/${PHASE_1_SEGMENTS})"
python -m "${PIPELINE}" "${COMMON[@]}" "${SAMPLE_FLAGS[@]}" --phase EMPLOYMENT_DOC_UPLOAD --segment_index 5 --total_segments "${PHASE_1_SEGMENTS}"
sep "EMPLOYMENT_DOC_UPLOAD (segment 6/${PHASE_1_SEGMENTS})"
python -m "${PIPELINE}" "${COMMON[@]}" "${SAMPLE_FLAGS[@]}" --phase EMPLOYMENT_DOC_UPLOAD --segment_index 6 --total_segments "${PHASE_1_SEGMENTS}"
sep "EMPLOYMENT_DOC_UPLOAD (segment 7/${PHASE_1_SEGMENTS})"
python -m "${PIPELINE}" "${COMMON[@]}" "${SAMPLE_FLAGS[@]}" --phase EMPLOYMENT_DOC_UPLOAD --segment_index 7 --total_segments "${PHASE_1_SEGMENTS}"
sep "EMPLOYMENT_DOC_UPLOAD (segment 8/${PHASE_1_SEGMENTS})"
python -m "${PIPELINE}" "${COMMON[@]}" "${SAMPLE_FLAGS[@]}" --phase EMPLOYMENT_DOC_UPLOAD --segment_index 8 --total_segments "${PHASE_1_SEGMENTS}"
sep "EMPLOYMENT_DOC_UPLOAD (segment 9/${PHASE_1_SEGMENTS})"
python -m "${PIPELINE}" "${COMMON[@]}" "${SAMPLE_FLAGS[@]}" --phase EMPLOYMENT_DOC_UPLOAD --segment_index 9 --total_segments "${PHASE_1_SEGMENTS}"

# Phase 2: Extract employment info (no sampling — extracts all uploaded docs)
sep "EMPLOYMENT_EXTRACTION (segment 0/${PHASE_1_SEGMENTS})"
python -m "${PIPELINE}" "${COMMON[@]}" --phase EMPLOYMENT_EXTRACTION --segment_index 0 --total_segments "${PHASE_1_SEGMENTS}"
sep "EMPLOYMENT_EXTRACTION (segment 1/${PHASE_1_SEGMENTS})"
python -m "${PIPELINE}" "${COMMON[@]}" --phase EMPLOYMENT_EXTRACTION --segment_index 1 --total_segments "${PHASE_1_SEGMENTS}"
sep "EMPLOYMENT_EXTRACTION (segment 2/${PHASE_1_SEGMENTS})"
python -m "${PIPELINE}" "${COMMON[@]}" --phase EMPLOYMENT_EXTRACTION --segment_index 2 --total_segments "${PHASE_1_SEGMENTS}"
sep "EMPLOYMENT_EXTRACTION (segment 3/${PHASE_1_SEGMENTS})"
python -m "${PIPELINE}" "${COMMON[@]}" --phase EMPLOYMENT_EXTRACTION --segment_index 3 --total_segments "${PHASE_1_SEGMENTS}"
sep "EMPLOYMENT_EXTRACTION (segment 4/${PHASE_1_SEGMENTS})"
python -m "${PIPELINE}" "${COMMON[@]}" --phase EMPLOYMENT_EXTRACTION --segment_index 4 --total_segments "${PHASE_1_SEGMENTS}"
sep "EMPLOYMENT_EXTRACTION (segment 5/${PHASE_1_SEGMENTS})"
python -m "${PIPELINE}" "${COMMON[@]}" --phase EMPLOYMENT_EXTRACTION --segment_index 5 --total_segments "${PHASE_1_SEGMENTS}"
sep "EMPLOYMENT_EXTRACTION (segment 6/${PHASE_1_SEGMENTS})"
python -m "${PIPELINE}" "${COMMON[@]}" --phase EMPLOYMENT_EXTRACTION --segment_index 6 --total_segments "${PHASE_1_SEGMENTS}"
sep "EMPLOYMENT_EXTRACTION (segment 7/${PHASE_1_SEGMENTS})"
python -m "${PIPELINE}" "${COMMON[@]}" --phase EMPLOYMENT_EXTRACTION --segment_index 7 --total_segments "${PHASE_1_SEGMENTS}"
sep "EMPLOYMENT_EXTRACTION (segment 8/${PHASE_1_SEGMENTS})"
python -m "${PIPELINE}" "${COMMON[@]}" --phase EMPLOYMENT_EXTRACTION --segment_index 8 --total_segments "${PHASE_1_SEGMENTS}"
sep "EMPLOYMENT_EXTRACTION (segment 9/${PHASE_1_SEGMENTS})"
python -m "${PIPELINE}" "${COMMON[@]}" --phase EMPLOYMENT_EXTRACTION --segment_index 9 --total_segments "${PHASE_1_SEGMENTS}"

# Phase 3: Deploy employment extraction views
sep "EMPLOYMENT_VIEW_DEPLOY"
python -m "${PIPELINE}" "${COMMON[@]}" --phase EMPLOYMENT_VIEW_DEPLOY

# Phase 4: Upload ER context docs (no sampling — scoped by extraction results)
sep "EMPLOYER_ER_DOC_UPLOAD (segment 0/${PHASE_2_SEGMENTS})"
python -m "${PIPELINE}" "${COMMON[@]}" --phase EMPLOYER_ER_DOC_UPLOAD --segment_index 0 --total_segments "${PHASE_2_SEGMENTS}"
sep "EMPLOYER_ER_DOC_UPLOAD (segment 1/${PHASE_2_SEGMENTS})"
python -m "${PIPELINE}" "${COMMON[@]}" --phase EMPLOYER_ER_DOC_UPLOAD --segment_index 1 --total_segments "${PHASE_2_SEGMENTS}"
sep "EMPLOYER_ER_DOC_UPLOAD (segment 2/${PHASE_2_SEGMENTS})"
python -m "${PIPELINE}" "${COMMON[@]}" --phase EMPLOYER_ER_DOC_UPLOAD --segment_index 2 --total_segments "${PHASE_2_SEGMENTS}"
sep "EMPLOYER_ER_DOC_UPLOAD (segment 3/${PHASE_2_SEGMENTS})"
python -m "${PIPELINE}" "${COMMON[@]}" --phase EMPLOYER_ER_DOC_UPLOAD --segment_index 3 --total_segments "${PHASE_2_SEGMENTS}"
sep "EMPLOYER_ER_DOC_UPLOAD (segment 4/${PHASE_2_SEGMENTS})"
python -m "${PIPELINE}" "${COMMON[@]}" --phase EMPLOYER_ER_DOC_UPLOAD --segment_index 4 --total_segments "${PHASE_2_SEGMENTS}"

# Phase 5: Extract employer entity resolution (no sampling)
sep "EMPLOYER_ER_EXTRACTION (segment 0/${PHASE_2_SEGMENTS})"
python -m "${PIPELINE}" "${COMMON[@]}" --phase EMPLOYER_ER_EXTRACTION --segment_index 0 --total_segments "${PHASE_2_SEGMENTS}"
sep "EMPLOYER_ER_EXTRACTION (segment 1/${PHASE_2_SEGMENTS})"
python -m "${PIPELINE}" "${COMMON[@]}" --phase EMPLOYER_ER_EXTRACTION --segment_index 1 --total_segments "${PHASE_2_SEGMENTS}"
sep "EMPLOYER_ER_EXTRACTION (segment 2/${PHASE_2_SEGMENTS})"
python -m "${PIPELINE}" "${COMMON[@]}" --phase EMPLOYER_ER_EXTRACTION --segment_index 2 --total_segments "${PHASE_2_SEGMENTS}"
sep "EMPLOYER_ER_EXTRACTION (segment 3/${PHASE_2_SEGMENTS})"
python -m "${PIPELINE}" "${COMMON[@]}" --phase EMPLOYER_ER_EXTRACTION --segment_index 3 --total_segments "${PHASE_2_SEGMENTS}"
sep "EMPLOYER_ER_EXTRACTION (segment 4/${PHASE_2_SEGMENTS})"
python -m "${PIPELINE}" "${COMMON[@]}" --phase EMPLOYER_ER_EXTRACTION --segment_index 4 --total_segments "${PHASE_2_SEGMENTS}"

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
