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
import { postWithURLAndBody, getAuthResource } from "./utils";

// Cloud SQL -> GCS CSV Export
export const generateNonETLExports = async (): Promise<Response> => {
  return postWithURLAndBody("/api/line_staff_tools/generate_non_etl_exports");
};

// GCS CSV -> Cloud SQL Import
export const fetchETLViewIds = async (): Promise<Response> => {
  return postWithURLAndBody("/api/line_staff_tools/fetch_etl_view_ids");
};

export const runCloudSQLImport = async (
  viewIds: string[]
): Promise<Response> => {
  return postWithURLAndBody("/api/line_staff_tools/run_gcs_import", {
    viewIds,
  });
};

// PO Feedback
export const getPOFeedback = async (): Promise<Response> => {
  return postWithURLAndBody("/api/line_staff_tools/get_po_feedback");
};

// Fetch states for po monthly reports
export const fetchEmailStateCodes = async (): Promise<Response> => {
  return postWithURLAndBody("/api/line_staff_tools/fetch_email_state_codes");
};

export const fetchRosterStateCodes = async (): Promise<Response> => {
  return postWithURLAndBody("/api/line_staff_tools/fetch_roster_state_codes");
};

export const fetchRawFilesStateCodes = async (): Promise<Response> => {
  return postWithURLAndBody(
    "/api/line_staff_tools/fetch_raw_files_state_codes"
  );
};

export const fetchReportTypes = async (): Promise<Response> => {
  return postWithURLAndBody("/api/line_staff_tools/fetch_report_types");
};

// Generate PO Monthly Report Emails
export const generateEmails = async (
  stateCode: string,
  reportType: string,
  testAddress: string | undefined,
  regionCode: string | undefined,
  messageBodyOverride: string | undefined,
  emailAllowlist: string[] | undefined
): Promise<Response> => {
  return postWithURLAndBody(
    `/api/line_staff_tools/${stateCode}/generate_emails`,
    {
      testAddress,
      regionCode,
      messageBodyOverride,
      reportType,
      emailAllowlist,
    }
  );
};

// Send PO Monthly Report Emails
export const sendEmails = async (
  stateCode: string,
  reportType: string,
  batchId: string,
  redirectAddress: string | undefined,
  ccAddresses: string[] | undefined,
  subjectOverride: string | undefined,
  emailAllowlist: string[] | undefined
): Promise<Response> => {
  return postWithURLAndBody(`/api/line_staff_tools/${stateCode}/send_emails`, {
    batchId,
    redirectAddress,
    reportType,
    ccAddresses,
    subjectOverride,
    emailAllowlist,
  });
};

export const getListBatchInfo = async (
  stateCode: string,
  reportType: string
): Promise<Response> => {
  return postWithURLAndBody("/api/line_staff_tools/list_batch_info", {
    stateCode,
    reportType,
  });
};

// State User Permissions
export const getStateUserPermissions = async (): Promise<Response> => {
  return getAuthResource("/users");
};
