// Recidiviz - a data platform for criminal justice reform
// Copyright (C) 2020 Recidiviz, Inc.
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

import MetadataDataset from "./models/MetadataDatasets";
import {
  DirectIngestInstance,
  QueueState,
} from "./components/IngestOperationsView/constants";

const postWithURLAndBody = async (
  url: string,
  body: Record<string, unknown>
): Promise<Response> => {
  return fetch(`/admin${url}`, {
    method: "POST",
    body: JSON.stringify(body),
    headers: {
      "Content-Type": "application/json",
    },
  });
};

// Fetch dataset metadata
export const fetchColumnObjectCountsByValue = async (
  metadataDataset: MetadataDataset,
  table: string,
  column: string
): Promise<Response> => {
  return postWithURLAndBody(
    `/api/${metadataDataset}/fetch_column_object_counts_by_value`,
    { table, column }
  );
};

export const fetchTableNonNullCountsByColumn = async (
  metadataDataset: MetadataDataset,
  table: string
): Promise<Response> => {
  return postWithURLAndBody(
    `/api/${metadataDataset}/fetch_table_nonnull_counts_by_column`,
    { table }
  );
};

export const fetchObjectCountsByTable = async (
  metadataDataset: MetadataDataset
): Promise<Response> => {
  return postWithURLAndBody(
    `/api/${metadataDataset}/fetch_object_counts_by_table`,
    {}
  );
};

// Fetch data freshness
export const fetchDataFreshness = async (): Promise<Response> => {
  return postWithURLAndBody("/api/ingest_metadata/data_freshness", {});
};

// Cloud SQL -> GCS CSV Export
export const generateCaseUpdatesExport = async (): Promise<Response> => {
  return postWithURLAndBody(
    "/api/case_triage/generate_case_updates_export",
    {}
  );
};

// GCS CSV -> Cloud SQL Import
export const fetchETLViewIds = async (): Promise<Response> => {
  return postWithURLAndBody("/api/case_triage/fetch_etl_view_ids", {});
};

export const runCloudSQLImport = async (
  viewIds: string[]
): Promise<Response> => {
  return postWithURLAndBody("/api/case_triage/run_gcs_import", {
    viewIds,
  });
};

// PO Feedback
export const getPOFeedback = async (): Promise<Response> => {
  return postWithURLAndBody("/api/case_triage/get_po_feedback", {});
};

// Ingest Operations Actions
export const fetchIngestStateCodes = async (): Promise<Response> => {
  return postWithURLAndBody(
    "/api/ingest_operations/fetch_ingest_state_codes",
    {}
  );
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

// Ingest Operations Actions
export const fetchRegionCodeFiles = async (
  regionCode: string
): Promise<Response> => {
  return postWithURLAndBody("/data_discovery/files", {
    region_code: regionCode,
  });
};

// Data Discovery
export interface Message {
  cursor: number;
  data: string;
  kind: string;
}

export const fetchDiscoveryStatus = async (
  discoveryId: string,
  messageCursor: number
): Promise<Response> => {
  return postWithURLAndBody("/data_discovery/discovery_status", {
    message_cursor: messageCursor,
    discovery_id: discoveryId,
  });
};

export const pollDiscoveryStatus = async (
  discoveryId: string,
  latestMessage: Message | null,
  onMessageReceived: CallableFunction
): Promise<void> => {
  const response = await fetchDiscoveryStatus(
    discoveryId,
    latestMessage ? latestMessage.cursor : 0
  );

  if (response.status === 200) {
    // Get and show the message
    const receivedMessage = await response.json();

    onMessageReceived(receivedMessage);

    if (receivedMessage.kind === "close") {
      return;
    }

    await pollDiscoveryStatus(discoveryId, receivedMessage, onMessageReceived);
  }

  if (response.status === 500) {
    onMessageReceived(null);
  }
};

export const createDiscovery = async (
  body: Record<string, unknown>
): Promise<Response> => {
  return postWithURLAndBody("/data_discovery/create_discovery", body);
};
