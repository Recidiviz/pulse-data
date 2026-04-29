#!/bin/bash
# Runs the full employment extraction pipeline for a specific list of person IDs.
#
# Intended for branch comparisons: run once on each branch with a different
# SANDBOX_PREFIX to produce independently comparable BQ datasets.
#
# Phases run sequentially (no segmentation needed for targeted PII runs).
#
# Usage: edit the variables below, then run:
#   bash recidiviz/NOT_FOR_PRODUCTION_USE/tools/document_extraction/run_person_ids_sandbox_employment_pipeline.sh

set -e

# ──────────────────────────────────────────────────────────────────────────────
# Configuration — edit these before running
# ──────────────────────────────────────────────────────────────────────────────

PROJECT_ID="recidiviz-staging"
STATE_CODE="US_IX"
SANDBOX_BUCKET="recidiviz-staging-anna-scratch"

# Change this between runs to isolate results per branch:
#   simrann_main        → run from main branch
#   simrann_org_change  → run from simrann/employer-org-change branch
SANDBOX_PREFIX="simrann_main"

# Comma-separated list of person IDs (integers) to process
PERSON_IDS=""

# Only include documents from the last N days
LOOKBACK_DAYS="365"

# ──────────────────────────────────────────────────────────────────────────────

PIPELINE="recidiviz.NOT_FOR_PRODUCTION_USE.tools.document_extraction.run_sandbox_employment_pipeline"

COMMON=(
  --project_id "${PROJECT_ID}"
  --state_code "${STATE_CODE}"
  --sandbox_dataset_prefix "${SANDBOX_PREFIX}"
  --sandbox_documents_bucket "${SANDBOX_BUCKET}"
)

# Person ID and lookback flags — only applied to DOC_UPLOAD phases.
# Extraction phases process everything already in the metadata table.
PERSON_FLAGS=()
[[ -n "${PERSON_IDS:-}" ]]    && PERSON_FLAGS+=("--person_ids" "${PERSON_IDS}")
[[ -n "${LOOKBACK_DAYS:-}" ]] && PERSON_FLAGS+=("--lookback_days" "${LOOKBACK_DAYS}")

sep() { echo; printf '=%.0s' {1..60}; echo; echo "  $1"; printf '=%.0s' {1..60}; echo; }

# Phase 1: Upload case note docs for the specified person IDs
sep "EMPLOYMENT_DOC_UPLOAD"
python -m "${PIPELINE}" "${COMMON[@]}" "${PERSON_FLAGS[@]}" --phase EMPLOYMENT_DOC_UPLOAD

# Phase 2: Extract employment info (processes all uploaded docs)
sep "EMPLOYMENT_EXTRACTION"
python -m "${PIPELINE}" "${COMMON[@]}" --phase EMPLOYMENT_EXTRACTION

# Phase 3: Deploy employment extraction views
sep "EMPLOYMENT_VIEW_DEPLOY"
python -m "${PIPELINE}" "${COMMON[@]}" --phase EMPLOYMENT_VIEW_DEPLOY

# Phase 4: Upload employer context docs (derived from extraction results)
sep "EMPLOYER_ER_DOC_UPLOAD"
python -m "${PIPELINE}" "${COMMON[@]}" --phase EMPLOYER_ER_DOC_UPLOAD

# Phase 5: Run employer entity resolution
sep "EMPLOYER_ER_EXTRACTION"
python -m "${PIPELINE}" "${COMMON[@]}" --phase EMPLOYER_ER_EXTRACTION

# Phase 6: Deploy ER extraction views
sep "EMPLOYER_ER_VIEW_DEPLOY"
python -m "${PIPELINE}" "${COMMON[@]}" --phase EMPLOYER_ER_VIEW_DEPLOY

# Phase 7: Deploy summary views + set table expiration
sep "SUMMARY_VIEW_DEPLOY"
python -m "${PIPELINE}" "${COMMON[@]}" --phase SUMMARY_VIEW_DEPLOY

# Status check
sep "Final status"
python -m "${PIPELINE}" "${COMMON[@]}"

sep "Done! Results in BQ datasets prefixed: ${SANDBOX_PREFIX}"
