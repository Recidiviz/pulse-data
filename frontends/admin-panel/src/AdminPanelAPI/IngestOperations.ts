// Recidiviz - a data platform for criminal justice reform
// Copyright (C) 2021 Recidiviz, Inc.
//
// This program is free software: you can redistribute it and/or modify
// it under the terms of the GNU General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// This program is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU General Public License for more details.
//
// You should have received a copy of the GNU General Public License
// along with this program.  If not, see <https://www.gnu.org/licenses/>.
// =============================================================================
import {
  DirectIngestInstance,
  QueueState,
} from "../components/IngestDataflow/constants";
import { getResource, postWithURLAndBody } from "./utils";

// Fetch states with ingest
export const fetchIngestStateCodes = async (): Promise<Response> => {
  return postWithURLAndBody("/api/ingest_operations/fetch_ingest_state_codes");
};

//  Trigger task scheduler
export const triggerTaskScheduler = async (
  regionCode: string,
  instance: DirectIngestInstance
): Promise<Response> => {
  return postWithURLAndBody(
    `/api/ingest_operations/${regionCode}/trigger_task_scheduler`,
    { instance }
  );
};

// TODO(#24652): delete once dataflow is fully enabled
//  Start Ingest Rerun
export const startIngestRerun = async (
  regionCode: string,
  instance: DirectIngestInstance,
  rawDataSourceInstance: DirectIngestInstance
): Promise<Response> => {
  return postWithURLAndBody(
    `/api/ingest_operations/${regionCode}/start_ingest_rerun`,
    {
      instance,
      rawDataSourceInstance,
    }
  );
};

//  Start Raw Data Reimport
export const startRawDataReimport = async (
  regionCode: string
): Promise<Response> => {
  return postWithURLAndBody(
    `/api/ingest_operations/${regionCode}/start_raw_data_reimport`
  );
};

// Update ingest queue states
export const updateIngestQueuesState = async (
  regionCode: string,
  newQueueState: QueueState
): Promise<Response> => {
  return postWithURLAndBody(
    `/api/ingest_operations/${regionCode}/update_ingest_queues_state`,
    { new_queue_state: newQueueState }
  );
};

// Get ingest queue states
export const getIngestQueuesState = async (
  regionCode: string
): Promise<Response> => {
  return fetch(
    `/admin/api/ingest_operations/${regionCode}/get_ingest_queue_states`,
    {
      headers: {
        "Content-Type": "application/json",
      },
    }
  );
};

// Get ingest instance resources
export const getIngestInstanceResources = async (
  regionCode: string,
  ingestInstance: DirectIngestInstance,
  controller: AbortController
): Promise<Response> => {
  return fetch(
    `/admin/api/ingest_operations/${regionCode}/get_ingest_instance_resources/${ingestInstance}`,
    {
      headers: {
        "Content-Type": "application/json",
      },
      signal: controller.signal,
    }
  );
};

// Get ingest view summaries
export const getIngestViewSummaries = async (
  regionCode: string,
  ingestInstance: DirectIngestInstance,
  controller: AbortController
): Promise<Response> => {
  return fetch(
    `/admin/api/ingest_operations/${regionCode}/get_ingest_view_summaries/${ingestInstance}`,
    {
      headers: {
        "Content-Type": "application/json",
      },
      signal: controller.signal,
    }
  );
};

// Get ingest raw file processing status
export const getIngestRawFileProcessingStatus = async (
  regionCode: string,
  ingestInstance: DirectIngestInstance,
  controller: AbortController
): Promise<Response> => {
  return fetch(
    `/admin/api/ingest_operations/${regionCode}/get_ingest_raw_file_processing_status/${ingestInstance}`,
    {
      headers: {
        "Content-Type": "application/json",
      },
      signal: controller.signal,
    }
  );
};

// Start CloudSQL export to GCS
export const exportDatabaseToGCS = async (
  stateCode: string,
  ingestInstance: DirectIngestInstance
): Promise<Response> => {
  return postWithURLAndBody(`/api/ingest_operations/export_database_to_gcs`, {
    stateCode,
    ingestInstance,
  });
};

// Start CloudSQL import from GCS
export const importDatabaseFromGCS = async (
  stateCode: string,
  importToDatabaseInstance: DirectIngestInstance,
  exportedDatabaseInstance: DirectIngestInstance
): Promise<Response> => {
  return postWithURLAndBody(`/api/ingest_operations/import_database_from_gcs`, {
    stateCode,
    importToDatabaseInstance,
    exportedDatabaseInstance,
  });
};

// Clean up CloudSQL import files
export const deleteDatabaseImportGCSFiles = async (
  stateCode: string,
  exportedDatabaseInstance: DirectIngestInstance
): Promise<Response> => {
  return postWithURLAndBody(
    `/api/ingest_operations/delete_database_import_gcs_files`,
    {
      stateCode,
      exportedDatabaseInstance,
    }
  );
};

// Acquire BQ Export lock for the STATE database
export const acquireBQExportLock = async (
  stateCode: string,
  ingestInstance: DirectIngestInstance
): Promise<Response> => {
  return postWithURLAndBody(`/api/ingest_operations/acquire_ingest_lock`, {
    stateCode,
    ingestInstance,
  });
};

// Release BQ Export lock for the STATE database
export const releaseBQExportLock = async (
  stateCode: string,
  ingestInstance: DirectIngestInstance
): Promise<Response> => {
  return postWithURLAndBody(`/api/ingest_operations/release_ingest_lock`, {
    stateCode,
    ingestInstance,
  });
};

// Import raw files to BigQuery Sandbox
export const importRawDataToSandbox = async (
  stateCode: string,
  sandboxDatasetPrefix: string,
  sourceBucket: string,
  fileTagFilters: string[] | undefined
): Promise<Response> => {
  return postWithURLAndBody(
    `/api/ingest_operations/direct/sandbox_raw_data_import`,
    {
      stateCode,
      sandboxDatasetPrefix,
      sourceBucket,
      fileTagFilters,
    }
  );
};

// Get list of sandbox buckets
export const listSandboxBuckets = async (): Promise<Response> => {
  return postWithURLAndBody(
    `/api/ingest_operations/direct/list_sandbox_buckets`
  );
};

// Get list of raw files and dates in sandbox bucket
export const listRawFilesInSandboxBucket = async (
  stateCode: string,
  sourceBucket: string
): Promise<Response> => {
  return postWithURLAndBody(`/api/ingest_operations/direct/list_raw_files`, {
    stateCode,
    sourceBucket,
  });
};

// Move ingest view results to backup dataset
export const moveIngestViewResultsToBackup = async (
  stateCode: string,
  ingestInstance: DirectIngestInstance
): Promise<Response> => {
  return postWithURLAndBody(
    "/api/ingest_operations/flash_primary_db/move_ingest_view_results_to_backup",
    {
      stateCode,
      ingestInstance,
    }
  );
};

// Move ingest view results between instances
export const moveIngestViewResultsBetweenInstances = async (
  stateCode: string,
  srcIngestInstance: DirectIngestInstance,
  destIngestInstance: DirectIngestInstance
): Promise<Response> => {
  return postWithURLAndBody(
    "/api/ingest_operations/flash_primary_db/move_ingest_view_results_between_instances",
    {
      stateCode,
      srcIngestInstance,
      destIngestInstance,
    }
  );
};

export const deleteContentsInSecondaryIngestViewDataset = async (
  stateCode: string
): Promise<Response> => {
  return postWithURLAndBody(
    "/api/ingest_operations/flash_primary_db/delete_contents_in_secondary_ingest_view_dataset",
    {
      stateCode,
    }
  );
};

// Mark instance ingest view data as invalidated
export const markInstanceIngestViewDataInvalidated = async (
  stateCode: string,
  ingestInstance: DirectIngestInstance
): Promise<Response> => {
  return postWithURLAndBody(
    "/api/ingest_operations/flash_primary_db/mark_instance_ingest_view_data_invalidated",
    {
      stateCode,
      ingestInstance,
    }
  );
};

// Transfer ingest view metadata to new instance
export const transferIngestViewMetadataToNewInstance = async (
  stateCode: string,
  srcIngestInstance: DirectIngestInstance,
  destIngestInstance: DirectIngestInstance
): Promise<Response> => {
  return postWithURLAndBody(
    "/api/ingest_operations/flash_primary_db/transfer_ingest_view_metadata_to_new_instance",
    {
      stateCode,
      srcIngestInstance,
      destIngestInstance,
    }
  );
};

// Invalidate ingest pipeline runs for a state and instance
export const invalidateIngestPipelineRuns = async (
  stateCode: string,
  ingestInstance: DirectIngestInstance
): Promise<Response> => {
  return postWithURLAndBody(
    "/api/ingest_operations/flash_primary_db/invalidate_ingest_pipeline_runs",
    {
      stateCode,
      ingestInstance,
    }
  );
};

// Get all ingest instance statuses
export const getAllIngestInstanceStatuses = async (): Promise<Response> => {
  return getResource("/api/ingest_operations/all_ingest_instance_statuses");
};

// Get all ingest dataflow pipeline enabled statuses
export const getAllIngestInstanceDataflowEnabledStatuses =
  async (): Promise<Response> => {
    return getResource(
      "/api/ingest_operations/all_ingest_instance_dataflow_enabled_status"
    );
  };

// Get all latest ingest dataflow pipeline statuses
export const getAllLatestDataflowJobs = async (): Promise<Response> => {
  return getResource(
    "/api/ingest_operations/get_all_latest_ingest_dataflow_jobs"
  );
};

// Get latest ingest dataflow pipeline status for the state and instance
export const getLatestDataflowJobByInstance = async (
  stateCode: string,
  instance: string
): Promise<Response> => {
  return getResource(
    `/api/ingest_operations/get_latest_ingest_dataflow_job_by_instance/${stateCode}/${instance}`
  );
};

// Get ingest dataflow pipeline output dataset names for state and instance
export const getDataflowJobAdditionalMetadataByInstance = async (
  stateCode: string,
  instance: string
): Promise<Response> => {
  return getResource(
    `/api/ingest_operations/get_dataflow_job_additional_metadata_by_instance/${stateCode}/${instance}`
  );
};

// Get latest ingest dataflow raw data watermarks for the state and instance
export const getLatestDataflowRawDataWatermarks = async (
  stateCode: string,
  instance: string
): Promise<Response> => {
  return getResource(
    `/api/ingest_operations/get_latest_ingest_dataflow_raw_data_watermarks/${stateCode}/${instance}`
  );
};

// Get latest raw data tags not meeting watermarks for the latest ingest dataflow pipeline run
export const getLatestRawDataTagsNotMeetingWatermark = async (
  stateCode: string,
  instance: string
): Promise<Response> => {
  return getResource(
    `/api/ingest_operations/get_latest_raw_data_tags_not_meeting_watermark/${stateCode}/${instance}`
  );
};

// Get latest run ingest view results for the state and instance
export const getLatestRunIngestViewResults = async (
  stateCode: string,
  instance: string
): Promise<Response> => {
  return getResource(
    `/api/ingest_operations/get_latest_run_ingest_view_results/${stateCode}/${instance}`
  );
};

// Get latest run state dataset row counts
export const getLatestRunStateDatasetRowCounts = async (
  stateCode: string,
  instance: string
): Promise<Response> => {
  return getResource(
    `/api/ingest_operations/get_latest_run_state_results/${stateCode}/${instance}`
  );
};

export const getRecentIngestInstanceStatusHistory = async (
  stateCode: string
): Promise<Response> => {
  return getResource(
    `/api/ingest_operations/get_recent_ingest_instance_status_history/${stateCode}`
  );
};

// Get current ingest instance status
export const getCurrentIngestInstanceStatus = async (
  stateCode: string,
  ingestInstance: DirectIngestInstance
): Promise<Response> => {
  return postWithURLAndBody(
    "/api/ingest_operations/get_current_ingest_instance_status",
    {
      stateCode,
      ingestInstance,
    }
  );
};

// Gets the raw data source instance of the most recent rerun (if present)
export const getRawDataSourceInstance = async (
  stateCode: string,
  ingestInstance: DirectIngestInstance
): Promise<Response> => {
  return postWithURLAndBody(
    "/api/ingest_operations/get_raw_data_source_instance",
    {
      stateCode,
      ingestInstance,
    }
  );
};

// Get current ingest instance status and associated information
export const getCurrentIngestInstanceStatusInformation = async (
  stateCode: string,
  ingestInstance: DirectIngestInstance
): Promise<Response> => {
  return postWithURLAndBody(
    "/api/ingest_operations/get_current_ingest_instance_status_information",
    {
      stateCode,
      ingestInstance,
    }
  );
};

// Set the specified instance status
export const changeIngestInstanceStatus = async (
  stateCode: string,
  ingestInstance: DirectIngestInstance,
  ingestInstanceStatus: string
): Promise<Response> => {
  return postWithURLAndBody(
    "/api/ingest_operations/change_ingest_instance_status",
    {
      stateCode,
      ingestInstance,
      ingestInstanceStatus,
    }
  );
};

// Copy raw data to backup dataset
export const copyRawDataToBackup = async (
  stateCode: string,
  ingestInstance: DirectIngestInstance
): Promise<Response> => {
  return postWithURLAndBody(
    "/api/ingest_operations/flash_primary_db/copy_raw_data_to_backup",
    {
      stateCode,
      ingestInstance,
    }
  );
};

// Copy raw data between instances
export const copyRawDataBetweenInstances = async (
  stateCode: string,
  srcIngestInstance: DirectIngestInstance,
  destIngestInstance: DirectIngestInstance
): Promise<Response> => {
  return postWithURLAndBody(
    "/api/ingest_operations/flash_primary_db/copy_raw_data_between_instances",
    {
      stateCode,
      srcIngestInstance,
      destIngestInstance,
    }
  );
};

// Delete contents of raw data tables
export const deleteContentsOfRawDataTables = async (
  stateCode: string,
  ingestInstance: DirectIngestInstance
): Promise<Response> => {
  return postWithURLAndBody(
    "/api/ingest_operations/flash_primary_db/delete_contents_of_raw_data_tables",
    {
      stateCode,
      ingestInstance,
    }
  );
};

// Mark instance raw data as invalidated
export const markInstanceRawDataInvalidated = async (
  stateCode: string,
  ingestInstance: DirectIngestInstance
): Promise<Response> => {
  return postWithURLAndBody(
    "/api/ingest_operations/flash_primary_db/mark_instance_raw_data_invalidated",
    {
      stateCode,
      ingestInstance,
    }
  );
};

// Transfer raw data metadata to new instance
export const transferRawDataMetadataToNewInstance = async (
  stateCode: string,
  srcIngestInstance: DirectIngestInstance,
  destIngestInstance: DirectIngestInstance
): Promise<Response> => {
  return postWithURLAndBody(
    "/api/ingest_operations/flash_primary_db/transfer_raw_data_metadata_to_new_instance",
    {
      stateCode,
      srcIngestInstance,
      destIngestInstance,
    }
  );
};

// Purge the ingest queues for a given state
export const purgeIngestQueues = async (
  stateCode: string
): Promise<Response> => {
  return postWithURLAndBody("/api/ingest_operations/purge_ingest_queues", {
    stateCode,
  });
};

// Determine if ingest in dataflow is enabled
export const isIngestInDataflowEnabled = async (
  stateCode: string,
  instance: DirectIngestInstance
): Promise<Response> => {
  return postWithURLAndBody(
    "/api/ingest_operations/is_ingest_in_dataflow_enabled",
    {
      stateCode,
      instance,
    }
  );
};

// Delete tables in the datasets related to raw data pruning
export const deleteTablesInPruningDatasets = async (
  stateCode: string,
  instance: DirectIngestInstance
): Promise<Response> => {
  return postWithURLAndBody(
    "/api/ingest_operations/delete_tables_in_pruning_datasets",
    {
      stateCode,
      instance,
    }
  );
};

// Run Calculation DAG For State
export const runCalculationDAGForState = async (
  stateCode: string
): Promise<Response> => {
  return postWithURLAndBody("/api/ingest_operations/trigger_calculation_dag", {
    stateCode,
  });
};

// Run Ingest DAG For State
export const runIngestDAGForState = async (
  stateCode: string
): Promise<Response> => {
  return postWithURLAndBody("/api/ingest_operations/trigger_ingest_dag", {
    stateCode,
  });
};
