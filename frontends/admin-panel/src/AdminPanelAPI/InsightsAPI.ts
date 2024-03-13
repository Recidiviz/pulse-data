// Recidiviz - a data platform for criminal justice reform
// Copyright (C) 2024 Recidiviz, Inc.
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
import { StateCodeInfo } from "../components/general/constants";
import {
  InsightsConfiguration,
  insightsConfigurationSchema,
} from "../InsightsStore/models/InsightsConfiguration";
import { get, post, put } from "./utils";

// Get enabled states for Insights
export const getInsightsStateCodeInfo = async (): Promise<StateCodeInfo[]> => {
  return get("/admin/outliers/enabled_state_codes");
};

export const getInsightsConfigs = async (
  stateCode: string
): Promise<InsightsConfiguration[]> => {
  const response = await get(`/admin/outliers/${stateCode}/configurations`);
  const configs = response as Array<unknown>;
  return configs.map((c) => {
    return insightsConfigurationSchema.parse(c);
  });
};

export const createNewConfig = async (
  request: InsightsConfiguration,
  stateCode: string
): Promise<Response> => {
  return post(`/admin/outliers/${stateCode}/configurations`, request);
};

export const promoteConfigToProduction = async (
  configId: number,
  stateCode: string
): Promise<Response> => {
  return post(
    `/admin/outliers/${stateCode}/configurations/${configId}/promote/production`
  );
};

export const promoteConfigToDefault = async (
  configId: number,
  stateCode: string
): Promise<Response> => {
  return post(
    `/admin/outliers/${stateCode}/configurations/${configId}/promote/default`
  );
};

export const reactivateConfig = async (
  configId: number,
  stateCode: string
): Promise<Response> => {
  return post(
    `/admin/outliers/${stateCode}/configurations/${configId}/reactivate`
  );
};

export const deactivateConfig = async (
  configId: number,
  stateCode: string
): Promise<Response> => {
  return put(
    `/admin/outliers/${stateCode}/configurations/${configId}/deactivate`
  );
};
