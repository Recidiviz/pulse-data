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
import {
  deleteWithUrlAndBody,
  getResource,
  patchWithURLAndBody,
  postWithURLAndBody,
  putWithURLAndBody,
} from "./utils";

// Agency Provisioning
export const getAgencies = async (): Promise<Response> => {
  return getResource(`/api/justice_counts_tools/agencies`);
};

export const getAgency = (agencyId: string) => (): Promise<Response> => {
  return getResource(`/api/justice_counts_tools/agency/${agencyId}`);
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

export const addUserToAgencyOrUpdateRole = async (
  agencyId: string,
  email: string,
  role: string
): Promise<Response> => {
  return patchWithURLAndBody(
    `/api/justice_counts_tools/agency/${agencyId}/users`,
    {
      email,
      role,
    }
  );
};

// User Provisioning
export const getUsers = async (): Promise<Response> => {
  return getResource(`/api/justice_counts_tools/users`);
};

export const createUser = async (
  email: string,
  name: string
): Promise<Response> => {
  return postWithURLAndBody(`/api/justice_counts_tools/users`, {
    email,
    name,
  });
};

export const updateUser = async (
  user: User,
  name: string | null,
  agencyIds: number[] | null,
  role: string | null
): Promise<Response> => {
  return putWithURLAndBody(`/api/justice_counts_tools/users`, {
    name,
    auth0_user_id: user.auth0_user_id,
    agency_ids: agencyIds,
    role,
  });
};

export const removeUsersFromAgency = async (
  agencyId: number,
  emails: string[]
): Promise<Response> => {
  return deleteWithUrlAndBody(
    `/api/justice_counts_tools/agency/${agencyId}/users`,
    {
      emails,
    }
  );
};

export const updateAgency = async (
  name: string | null,
  systems: string[] | null,
  agencyId: number
): Promise<Response> => {
  return putWithURLAndBody(`/api/justice_counts_tools/agency/${agencyId}`, {
    name,
    systems,
  });
};

export const removeChildAgenciesFromSuperAgency = async (
  superAgencyId: number,
  childAgencyIds: number[]
): Promise<Response> => {
  return deleteWithUrlAndBody(
    `/api/justice_counts_tools/agency/${superAgencyId}`,
    {
      child_agency_ids: childAgencyIds,
    }
  );
};

export const addChildAgencyToSuperAgency = async (
  superAgencyId: number,
  childAgencyId: number
): Promise<Response> => {
  return putWithURLAndBody(
    `/api/justice_counts_tools/agency/${superAgencyId}`,
    {
      child_agency_id: childAgencyId,
    }
  );
};
