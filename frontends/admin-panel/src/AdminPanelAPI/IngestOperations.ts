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
import { postWithURLAndBody } from "./utils";

// Fetch states with ingest
export const fetchIngestStateCodes = async (): Promise<Response> => {
  return postWithURLAndBody("/api/ingest_operations/fetch_ingest_state_codes");
};

// Start Ingest
export const startIngestRun = async (
  regionCode: string,
  instance: DirectIngestInstance
): Promise<Response> => {
  return postWithURLAndBody(
    `/api/ingest_operations/${regionCode}/start_ingest_run`,
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

// Get ingest instance summaries
export const getIngestInstanceSummaries = async (
  regionCode: string
): Promise<Response> => {
  return fetch(
    `/admin/api/ingest_operations/${regionCode}/get_ingest_instance_summaries`,
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

// Pauses direct ingest instances
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
