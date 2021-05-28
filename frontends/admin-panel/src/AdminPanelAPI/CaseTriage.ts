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
import { postWithURLAndBody } from "./utils";

// Cloud SQL -> GCS CSV Export
export const generateCaseUpdatesExport = async (): Promise<Response> => {
  return postWithURLAndBody("/api/case_triage/generate_case_updates_export");
};

// GCS CSV -> Cloud SQL Import
export const fetchETLViewIds = async (): Promise<Response> => {
  return postWithURLAndBody("/api/case_triage/fetch_etl_view_ids");
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
  return postWithURLAndBody("/api/case_triage/get_po_feedback");
};

// Fetch states for po monthly reports
export const fetchEmailStateCodes = async (): Promise<Response> => {
  return postWithURLAndBody(
    // change to fetch_email_state_codes
    "/api/ingest_operations/fetch_ingest_state_codes"
  );
};
