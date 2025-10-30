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
import { AddUserRequest, FeatureVariants, Routes } from "../types";
import {
  deleteResource,
  deleteWithUrlAndBody,
  getAuthResource,
  patchAuthWithURLAndBody,
  postAuthWithURLAndBody,
  postWithURLAndBody,
  putAuthWithURLAndBody,
} from "./utils";

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

// State Role Permissions
export const getStateRoleDefaultPermissions = async (): Promise<Response> => {
  return getAuthResource("/states");
};

export const createNewUser = async (
  request: AddUserRequest
): Promise<Response> => {
  return postAuthWithURLAndBody(
    `/users`,
    request as unknown as Record<string, unknown>
  );
};

type UserUpdate = {
  userHash: string;
  stateCode: string;
  externalId?: string;
  roles?: string[];
  district?: string;
  firstName?: string;
  lastName?: string;
  blockedOn?: string | null;
  reason: string;
};

export const updateUser = async (update: UserUpdate): Promise<Response> => {
  const { userHash } = update;
  return patchAuthWithURLAndBody(
    `/users/${encodeURIComponent(userHash)}`,
    update
  );
};

export const updateUsers = async (
  userUpdates: UserUpdate[]
): Promise<Response> => {
  return patchAuthWithURLAndBody(`/users`, userUpdates);
};

export const updateUserPermissions = async (
  userHash: string,
  reason: string,
  routes?: Partial<Routes>,
  featureVariants?: Partial<FeatureVariants>
): Promise<Response> => {
  return putAuthWithURLAndBody(
    `/users/${encodeURIComponent(userHash)}/permissions`,
    {
      routes,
      featureVariants,
      reason,
    }
  );
};

export const deleteCustomUserPermissions = async (
  userHash: string,
  reason: string
): Promise<Response> => {
  return deleteResource(`/users/${encodeURIComponent(userHash)}/permissions`, {
    reason,
  });
};

export const blockUser = async (
  userHash: string,
  reason: string
): Promise<Response> => {
  return deleteResource(`/users/${encodeURIComponent(userHash)}`, { reason });
};

export const createStateRolePermissions = async (
  stateCode: string,
  role: string,
  reason: string,
  routes?: Partial<Routes>,
  featureVariants?: Partial<FeatureVariants>
): Promise<Response> => {
  return postAuthWithURLAndBody(`/states/${stateCode}/roles/${role}`, {
    routes,
    featureVariants,
    reason,
  });
};

export const updateStateRolePermissions = async (
  stateCode: string,
  role: string,
  reason: string,
  routes?: Partial<Routes>,
  featureVariants?: Partial<FeatureVariants>
): Promise<Response> => {
  return patchAuthWithURLAndBody(`/states/${stateCode}/roles/${role}`, {
    routes,
    featureVariants,
    reason,
  });
};

export const deleteStateRole = async (
  stateCode: string,
  role: string,
  reason: string
): Promise<Response> => {
  return deleteResource(`/states/${stateCode}/roles/${role}`, { reason });
};

export const deleteDemoClientUpdatesV2 = async (
  stateCode?: string
): Promise<Response> => {
  let url = "/api/line_staff_tools/demo_client_updates_v2";
  if (stateCode) url += `/${stateCode.toLowerCase()}`;
  return deleteWithUrlAndBody(url);
};

export const deleteFeatureVariant = async ({
  fvName,
  reason,
  entityType,
}: {
  fvName: string;
  reason: string;
  entityType: "ROLES" | "USERS";
}): Promise<Response> => {
  let url = `/feature_variants/${fvName}`;
  if (entityType === "USERS") url = `/users${url}`;
  return deleteResource(url, { reason });
};
