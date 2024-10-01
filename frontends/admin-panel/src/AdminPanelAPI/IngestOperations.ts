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
} from "../components/IngestStatus/constants";
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

// Get all latest ingest dataflow pipeline statuses
export const getAllLatestDataflowJobs = async (): Promise<Response> => {
  return getResource(
    "/api/ingest_operations/get_all_latest_ingest_dataflow_jobs"
  );
};

// Get latest ingest dataflow pipeline status for the state and instance
export const getLatestDataflowJob = async (
  stateCode: string
): Promise<Response> => {
  return getResource(
    `/api/ingest_operations/get_latest_ingest_dataflow_job/${stateCode}`
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

// Get latest ingest dataflow pipeline run ingest view results for the state and instance
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

// Run Ingest DAG For State
export const triggerCalculationDAGForState = async (
  stateCode: string
): Promise<Response> => {
  return postWithURLAndBody("/api/ingest_operations/trigger_calculation_dag", {
    stateCode,
  });
};

// TODO(#28239) remove call once raw data DAG is enabled for all states
// Determine if raw data import DAG is enabled
export const isRawDataImportDagEnabled = async (
  stateCode: string,
  rawDataInstance: string
): Promise<Response> => {
  return getResource(
    `/api/ingest_operations/is_raw_data_import_dag_enabled/${stateCode}/${rawDataInstance}`
  );
};

export const rawDataImportDagEnabledForAllStates =
  async (): Promise<Response> => {
    return getResource(
      `/api/ingest_operations/is_raw_data_import_dag_enabled_all`
    );
  };

// Get all latest ingest raw data import run info
export const getAllLatestRawDataImportRunInfo = async (): Promise<Response> => {
  return getResource(
    "/api/ingest_operations/all_latest_raw_data_import_run_info"
  );
};

// Get all latest raw data resource lock info
export const getAllLatestRawDataResourceLockInfo =
  async (): Promise<Response> => {
    return getResource("/api/ingest_operations/all_current_lock_summaries");
  };

// Get latest raw data import runs for a specific file tag
export const getLatestRawDataImportRunsForFileTag = async (
  stateCode: string,
  rawDataInstance: string,
  fileTag: string
): Promise<Response> => {
  return getResource(
    `/api/ingest_operations/get_latest_raw_data_imports/${stateCode}/${rawDataInstance}/${fileTag}`
  );
};

// Get raw file config summary
export const getRawFileConfigSummary = async (
  stateCode: string,
  fileTag: string
): Promise<Response> => {
  return getResource(
    `/api/ingest_operations/raw_file_config/${stateCode}/${fileTag}`
  );
};

// Get current flash status row
export const getIsFlashingInProgress = async (
  stateCode: string
): Promise<Response> => {
  return getResource(
    `/api/ingest_operations/is_flashing_in_progress/${stateCode}`
  );
};

// Update current flash status row
export const updateIsFlashingInProgress = async (
  stateCode: string,
  isFlashing: boolean
): Promise<Response> => {
  return postWithURLAndBody(
    `/api/ingest_operations/is_flashing_in_progress/update`,
    {
      stateCode,
      isFlashing,
    }
  );
};

// Get info about whether secondary is stale
export const getStaleSecondaryRawData = async (
  stateCode: string
): Promise<Response> => {
  return getResource(`/api/ingest_operations/stale_secondary/${stateCode}`);
};

// Acquire all resource locks for state + raw data instance pair
export const acquireResourceLocksForStateAndInstance = async (
  stateCode: string,
  rawDataInstance: DirectIngestInstance,
  description: string,
  ttlSeconds: number | null
): Promise<Response> => {
  return postWithURLAndBody(
    `/api/ingest_operations/resource_locks/acquire_all`,
    {
      stateCode,
      rawDataInstance,
      description,
      ttlSeconds,
    }
  );
};

// Release provided resource locks by id
export const releaseResourceLocksForStateById = async (
  stateCode: string,
  rawDataInstance: DirectIngestInstance,
  lockIds: number[]
): Promise<Response> => {
  return postWithURLAndBody(
    `/api/ingest_operations/resource_locks/release_all`,
    {
      stateCode,
      rawDataInstance,
      lockIds,
    }
  );
};

// Get latest raw data lock statuses
export const getRawDataInstanceLockStatuses = async (
  stateCode: string,
  rawDataInstance: DirectIngestInstance
): Promise<Response> => {
  return getResource(
    `/api/ingest_operations/resource_locks/list_all/${stateCode}/${rawDataInstance}`
  );
};

export const getRawDataResourceLockMetadata = async (): Promise<Response> => {
  return getResource(`/api/ingest_operations/resource_locks/metadata`);
};

// Mark instance raw data as invalidated
export const markInstanceRawDataV2Invalidated = async (
  stateCode: string,
  rawDataInstance: DirectIngestInstance
): Promise<Response> => {
  return postWithURLAndBody(
    "/api/ingest_operations/flash_primary_db/mark_instance_raw_data_v2_invalidated",
    {
      stateCode,
      rawDataInstance,
    }
  );
};

// Transfer raw data metadata to new instance
export const transferRawDataV2MetadataToNewInstance = async (
  stateCode: string,
  srcIngestInstance: DirectIngestInstance,
  destIngestInstance: DirectIngestInstance
): Promise<Response> => {
  return postWithURLAndBody(
    "/api/ingest_operations/flash_primary_db/transfer_raw_data_v2_metadata_to_new_instance",
    {
      stateCode,
      srcIngestInstance,
      destIngestInstance,
    }
  );
};
