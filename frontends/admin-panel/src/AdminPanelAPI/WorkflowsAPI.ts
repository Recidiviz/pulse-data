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
  Opportunity,
  opportunitySchema,
} from "../WorkflowsStore/models/Opportunity";
import {
  BabyOpportunityConfiguration,
  OpportunityConfiguration,
  opportunityConfigurationSchema,
} from "../WorkflowsStore/models/OpportunityConfiguration";
import { get, post } from "./utils";

// Get enabled states for Workflows
export const getWorkflowsStateCodeInfo = async (): Promise<StateCodeInfo[]> => {
  return get("/admin/workflows/enabled_state_codes");
};

export const getOpportunities = async (
  stateCode: string
): Promise<Opportunity[]> => {
  const response = await get(`/admin/workflows/${stateCode}/opportunities`);
  return opportunitySchema.array().parse(response);
};

export const getOpportunityConfigurations = async (
  stateCode: string,
  opportunityType: string
): Promise<OpportunityConfiguration[]> => {
  const response = await get(
    `/admin/workflows/${stateCode}/opportunities/${opportunityType}/configurations`
  );
  return opportunityConfigurationSchema.array().parse(response);
};

export const getOpportunityConfiguration = async (
  stateCode: string,
  opportunityType: string,
  configId: number
): Promise<OpportunityConfiguration[]> => {
  const response = await get(
    `/admin/workflows/${stateCode}/opportunities/${opportunityType}/configurations/${configId}`
  );
  return opportunityConfigurationSchema.array().parse(response);
};

export const postOpportunityConfiguration = async (
  stateCode: string,
  opportunityType: string,
  config: BabyOpportunityConfiguration
): Promise<Response> => {
  return post(
    `/admin/workflows/${stateCode}/opportunities/${opportunityType}/configurations`,
    config
  );
};

export const deactivateOpportunityConfiguration = async (
  stateCode: string,
  opportunityType: string,
  configId: number
): Promise<OpportunityConfiguration> => {
  const response = await post(
    `/admin/workflows/${stateCode}/opportunities/${opportunityType}/configurations/${configId}/deactivate`
  );
  return opportunityConfigurationSchema.parse(response);
};

export const activateOpportunityConfiguration = async (
  stateCode: string,
  opportunityType: string,
  configId: number
): Promise<OpportunityConfiguration> => {
  const response = await post(
    `/admin/workflows/${stateCode}/opportunities/${opportunityType}/configurations/${configId}/activate`
  );
  return opportunityConfigurationSchema.parse(response);
};

export const promoteOpportunityConfiguration = async (
  stateCode: string,
  opportunityType: string,
  configId: number
): Promise<Response> => {
  return post(
    `/admin/workflows/${stateCode}/opportunities/${opportunityType}/configurations/${configId}/promote`
  );
};
