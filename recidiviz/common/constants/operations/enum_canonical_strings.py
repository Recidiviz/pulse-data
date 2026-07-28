# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2021 Recidiviz, Inc.
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program.  If not, see <https://www.gnu.org/licenses/>.
# =============================================================================
"""Defines constants for use in the operations context."""

# DirectIngestInstance enum
direct_ingest_instance_primary = "PRIMARY"
direct_ingest_instance_secondary = "SECONDARY"

# IngestPipelineType enum
ingest_pipeline_type_activity = "ACTIVITY"
ingest_pipeline_type_identity = "IDENTITY"

# DirectIngestLockActor
direct_ingest_lock_actor_process = "PROCESS"
direct_ingest_lock_actor_adhoc = "ADHOC"

# DirectIngestLockResource
direct_ingest_lock_resource_bucket = "BUCKET"
direct_ingest_lock_resource_operations_database = "OPERATIONS_DATABASE"
direct_ingest_lock_resource_big_query_raw_data_dataset = "BIG_QUERY_RAW_DATA_DATASET"

# DirectIngestRawFileImportStatus
direct_ingest_raw_file_import_status_started = "STARTED"
direct_ingest_raw_file_import_status_deferred = "DEFERRED"
direct_ingest_raw_file_import_status_succeeded = "SUCCEEDED"
direct_ingest_raw_file_import_status_failed_dag_level = "FAILED_DAG_LEVEL"
direct_ingest_raw_file_import_status_failed_pre_import_normalization_step = (
    "FAILED_PRE_IMPORT_NORMALIZATION_STEP"
)
direct_ingest_raw_file_import_status_failed_load_step = "FAILED_LOAD_STEP"
direct_ingest_raw_file_import_status_failed_validation_step = "FAILED_VALIDATION_STEP"
direct_ingest_raw_file_import_status_failed_import_blocked = "FAILED_IMPORT_BLOCKED"

# LLMExtractionJobResultType
llm_extraction_job_result_type_success = "SUCCESS"
llm_extraction_job_result_type_partial_failure = "PARTIAL_FAILURE"
llm_extraction_job_result_type_failure = "FAILURE"

# LLMExtractionJobDocumentResultType
llm_extraction_job_document_result_type_success = "SUCCESS"
llm_extraction_job_document_result_type_job_level_failure = "JOB_LEVEL_FAILURE"
llm_extraction_job_document_result_type_document_level_failure_transient = (
    "DOCUMENT_LEVEL_FAILURE_TRANSIENT"
)
llm_extraction_job_document_result_type_document_level_failure_permanent = (
    "DOCUMENT_LEVEL_FAILURE_PERMANENT"
)
llm_extraction_job_document_result_type_document_level_failure_retries_exhausted = (
    "DOCUMENT_LEVEL_FAILURE_RETRIES_EXHAUSTED"
)

# LLMDocumentExtractionErrorType
llm_document_extraction_error_type_llm_request_malformed_response = (
    "LLM_REQUEST_MALFORMED_RESPONSE"
)
llm_document_extraction_error_type_llm_request_empty_response = (
    "LLM_REQUEST_EMPTY_RESPONSE"
)
llm_document_extraction_error_type_llm_request_content_filtered = (
    "LLM_REQUEST_CONTENT_FILTERED"
)
llm_document_extraction_error_type_llm_request_timeout = "LLM_REQUEST_TIMEOUT"
llm_document_extraction_error_type_llm_request_rate_limited = "LLM_REQUEST_RATE_LIMITED"
llm_document_extraction_error_type_llm_request_server_error = "LLM_REQUEST_SERVER_ERROR"
llm_document_extraction_error_type_llm_request_unknown_error = (
    "LLM_REQUEST_UNKNOWN_ERROR"
)
