#!/bin/bash
# Runs the full US_IX employment + housing extraction pipeline including
# entity resolution post-processing in a sandbox.
#
# Requires: gcloud auth application-default login
#
# Steps:
#   1. Upload US_IX supervision case notes
#   2. Run employment extractor
#   3. Run housing extractor
#   4. Refresh derived employer context document collection
#   5. Refresh derived housing context document collection
#   6. Run employer entity resolution extractor
#   7. Run housing entity resolution extractor

set -e  # Exit on any error

PROJECT_ID="recidiviz-staging"
SANDBOX_PREFIX="mayukas_RYAN_MATTHEWS"
SANDBOX_BUCKET="recidiviz-staging-anna-scratch"
RYANMATTHEWS_PERSON_IDS="9068821127370000799,9071361806830618526,9062044949964515542,9065009282244419136,9070000262904142218,9061587440710644238,9068339853523326204,9063679296077361895,9056827422982385891,9033355623316899506"

# ── Step 1: Upload source case notes ──────────────────────────────────────────
echo "=== Step 1: Refreshing US_IX supervision case notes ==="
python -m recidiviz.NOT_FOR_PRODUCTION_USE.tools.document_extraction.refresh_sandbox_document_collection \
  --project_id "${PROJECT_ID}" \
  --collection_name US_IX_SUPERVISION_CASE_NOTES \
  --sandbox_dataset_prefix "${SANDBOX_PREFIX}" \
  --sandbox_bucket "${SANDBOX_BUCKET}" \
  --state_codes US_IX \
  --person_ids "${RYANMATTHEWS_PERSON_IDS}" 
  # --sample_entity_count 5 \
  # --lookback_days 180 \
  # --active_in_compartment SUPERVISION

# ── Step 2: Employment first-layer extractor ──────────────────────────────────
echo "=== Step 2: Running employment extractor ==="
python -m recidiviz.NOT_FOR_PRODUCTION_USE.tools.document_extraction.run_sandbox_document_extraction_job \
  --project_id "${PROJECT_ID}" \
  --extractor_id US_IX_CASE_NOTE_EMPLOYMENT_INFO \
  --sandbox_dataset_prefix "${SANDBOX_PREFIX}" \
  --sandbox_documents_bucket "${SANDBOX_BUCKET}" \
  concurrent

# ── Step 3: Housing first-layer extractor ─────────────────────────────────────
echo "=== Step 3: Running housing extractor ==="
python -m recidiviz.NOT_FOR_PRODUCTION_USE.tools.document_extraction.run_sandbox_document_extraction_job \
  --project_id "${PROJECT_ID}" \
  --extractor_id US_IX_CASE_NOTE_HOUSING_INFO \
  --sandbox_dataset_prefix "${SANDBOX_PREFIX}" \
  --sandbox_documents_bucket "${SANDBOX_BUCKET}" \
  concurrent

# ── Step 4: Refresh derived employer context collection ───────────────────────
echo "=== Step 4: Refreshing employer context document collection ==="
python -m recidiviz.NOT_FOR_PRODUCTION_USE.tools.document_extraction.refresh_sandbox_document_collection \
  --project_id "${PROJECT_ID}" \
  --collection_name US_IX_CASE_NOTE_EMPLOYER_CONTEXT \
  --sandbox_dataset_prefix "${SANDBOX_PREFIX}" \
  --sandbox_bucket "${SANDBOX_BUCKET}"

# ── Step 5: Refresh derived housing context collection ────────────────────────
echo "=== Step 5: Refreshing housing context document collection ==="
python -m recidiviz.NOT_FOR_PRODUCTION_USE.tools.document_extraction.refresh_sandbox_document_collection \
  --project_id "${PROJECT_ID}" \
  --collection_name US_IX_CASE_NOTE_HOUSING_CONTEXT \
  --sandbox_dataset_prefix "${SANDBOX_PREFIX}" \
  --sandbox_bucket "${SANDBOX_BUCKET}"

# ── Step 6: Employer entity resolution extractor ──────────────────────────────
echo "=== Step 6: Running employer entity resolution extractor ==="
python -m recidiviz.NOT_FOR_PRODUCTION_USE.tools.document_extraction.run_sandbox_document_extraction_job \
  --project_id "${PROJECT_ID}" \
  --extractor_id US_IX_CASE_NOTE_EMPLOYER_ENTITY_RESOLUTION \
  --sandbox_dataset_prefix "${SANDBOX_PREFIX}" \
  --sandbox_documents_bucket "${SANDBOX_BUCKET}" \
  concurrent

# ── Step 7: Housing entity resolution extractor ───────────────────────────────
echo "=== Step 7: Running housing entity resolution extractor ==="
python -m recidiviz.NOT_FOR_PRODUCTION_USE.tools.document_extraction.run_sandbox_document_extraction_job \
  --project_id "${PROJECT_ID}" \
  --extractor_id US_IX_CASE_NOTE_HOUSING_ENTITY_RESOLUTION \
  --sandbox_dataset_prefix "${SANDBOX_PREFIX}" \
  --sandbox_documents_bucket "${SANDBOX_BUCKET}" \
  concurrent

echo "=== Pipeline complete ==="
