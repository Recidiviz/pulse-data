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

import { User } from "../components/JusticeCountsTools/constants";
import { getResource, postWithURLAndBody, putWithURLAndBody } from "./utils";

// Agency Provisioning
export const getAgencies = async (): Promise<Response> => {
  return getResource(`/api/justice_counts_tools/agencies`);
};

export const createAgency = async (
  name: string,
  systems: string[],
  stateCode: string,
  fipsCountyCode?: string
): Promise<Response> => {
  return postWithURLAndBody(`/api/justice_counts_tools/agencies`, {
    name,
    systems,
    state_code: stateCode,
    fips_county_code: fipsCountyCode,
  });
};

// User Provisioning
export const getUsers = async (): Promise<Response> => {
  return getResource(`/api/justice_counts_tools/users`);
};

export const updateUser = async (
  user: User,
  name: string | null,
  agencyIds: number[] | null
): Promise<Response> => {
  return putWithURLAndBody(`/api/justice_counts_tools/users`, {
    name,
    auth0_user_id: user.auth0_user_id,
    agency_ids: agencyIds,
  });
};

export const bulkUpload = async (formData: FormData): Promise<Response> => {
  // Don't use postWithURLAndBody because that will JSONify payload,
  // and we don't want to do that because it contains a file
  const url = `/api/justice_counts_tools/bulk_upload`;
  return fetch(`/admin${url}`, {
    method: "POST",
    body: formData,
  });
};
