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
} from "../components/IngestOperationsView/constants";
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

// Get ingest instance summary
export const getIngestInstanceSummary = async (
  regionCode: string,
  ingestInstance: DirectIngestInstance
): Promise<Response> => {
  return fetch(
    `/admin/api/ingest_operations/${regionCode}/get_ingest_instance_summary/${ingestInstance}`,
    {
      headers: {
        "Content-Type": "application/json",
      },
    }
  );
};

// Get ingest raw file processing status
export const getIngestRawFileProcessingStatus = async (
  regionCode: string,
  ingestInstance: DirectIngestInstance
): Promise<Response> => {
  return fetch(
    `/admin/api/ingest_operations/${regionCode}/get_ingest_raw_file_processing_status/${ingestInstance}`,
    {
      headers: {
        "Content-Type": "application/json",
      },
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

// Pauses direct ingest instance
export const pauseDirectIngestInstance = async (
  stateCode: string,
  ingestInstance: DirectIngestInstance
): Promise<Response> => {
  return postWithURLAndBody(
    `/api/ingest_operations/pause_direct_ingest_instance`,
    {
      stateCode,
      ingestInstance,
    }
  );
};

// Unpauses direct ingest instance
export const unpauseDirectIngestInstance = async (
  stateCode: string,
  ingestInstance: DirectIngestInstance
): Promise<Response> => {
  return postWithURLAndBody(
    `/api/ingest_operations/unpause_direct_ingest_instance`,
    {
      stateCode,
      ingestInstance,
    }
  );
};

// Import raw files to BiqQuery Sandbox
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

// Transer ingest view metadata to new instance
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

// Get all ingest instance statuses
export const getAllIngestInstanceStatuses = async (): Promise<Response> => {
  return getResource("/api/ingest_operations/all_ingest_instance_statuses");
};
