#!/bin/bash
# Example commands for running document extraction jobs
# Copy/paste any of these into your terminal

# ============================================================
# Single extractor: run_sandbox_document_extraction_job
# ============================================================

# Fake mode (for testing without real LLM calls)
python -m recidiviz.NOT_FOR_PRODUCTION_USE.tools.document_extraction.run_sandbox_document_extraction_job \
  --project_id recidiviz-staging \
  --extractor_id US_IX_CASE_NOTE_EMPLOYMENT_INFO \
  --sandbox_dataset_prefix my_prefix \
  --sandbox_documents_bucket recidiviz-staging-my-scratch \
  --sample_size 20 \
  fake

# Concurrent mode (local processing with real LLM calls via Vertex AI)
# Requires: gcloud auth application-default login
python -m recidiviz.NOT_FOR_PRODUCTION_USE.tools.document_extraction.run_sandbox_document_extraction_job \
  --project_id recidiviz-staging \
  --extractor_id US_IX_CASE_NOTE_EMPLOYMENT_INFO \
  --sandbox_dataset_prefix my_prefix \
  --sandbox_documents_bucket recidiviz-staging-my-scratch \
  --sample_size 20 \
  concurrent

# Batch mode (server-side processing via Vertex AI)
python -m recidiviz.NOT_FOR_PRODUCTION_USE.tools.document_extraction.run_sandbox_document_extraction_job \
  --project_id recidiviz-staging \
  --extractor_id US_IX_CASE_NOTE_EMPLOYMENT_INFO \
  --sandbox_dataset_prefix my_prefix \
  --sandbox_documents_bucket recidiviz-staging-my-scratch \
  --sample_size 20 \
  batch \
  --sandbox_llm_job_artifact_bucket recidiviz-staging-my-scratch

# Sample by entity count (5 people, all their documents)
python -m recidiviz.NOT_FOR_PRODUCTION_USE.tools.document_extraction.run_sandbox_document_extraction_job \
  --project_id recidiviz-staging \
  --extractor_id US_IX_CASE_NOTE_EMPLOYMENT_INFO \
  --sandbox_dataset_prefix my_prefix \
  --sandbox_documents_bucket recidiviz-staging-my-scratch \
  --sample_entity_count 5 \
  concurrent

# Filter to people on active supervision, last 90 days of documents
python -m recidiviz.NOT_FOR_PRODUCTION_USE.tools.document_extraction.run_sandbox_document_extraction_job \
  --project_id recidiviz-staging \
  --extractor_id US_IX_CASE_NOTE_EMPLOYMENT_INFO \
  --sandbox_dataset_prefix my_prefix \
  --sandbox_documents_bucket recidiviz-staging-my-scratch \
  --sample_entity_count 10 \
  --active_in_compartment SUPERVISION \
  --lookback_days 90 \
  concurrent

# ============================================================
# Full collection: run_sandbox_collection_extraction_job
# ============================================================

# Run all extractors in a collection (all states)
python -m recidiviz.NOT_FOR_PRODUCTION_USE.tools.document_extraction.run_sandbox_collection_extraction_job \
  --project_id recidiviz-staging \
  --collection_name CASE_NOTE_EMPLOYMENT_INFO \
  --sandbox_dataset_prefix my_prefix \
  --sandbox_documents_bucket recidiviz-staging-my-scratch \
  concurrent

# Run specific states with entity sampling
python -m recidiviz.NOT_FOR_PRODUCTION_USE.tools.document_extraction.run_sandbox_collection_extraction_job \
  --project_id recidiviz-staging \
  --collection_name CASE_NOTE_EMPLOYMENT_INFO \
  --sandbox_dataset_prefix my_prefix \
  --sandbox_documents_bucket recidiviz-staging-my-scratch \
  --state_codes US_IX \
  --sample_entity_count_per_state 5 \
  --active_in_compartment SUPERVISION \
  --lookback_days 90 \
  concurrent
