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
import {
  ALLOWED_APPS_LABELS,
  ROUTES_PERMISSIONS_LABELS,
} from "./components/constants";

export interface MetadataRecord<T> {
  name: string;
  resultsByState: {
    [stateCode: string]: T;
  };
}

// Ingest Metadata Column Counts
export interface MetadataAPIResult {
  [name: string]: {
    [stateCode: string]: MetadataCount;
  };
}

export interface MetadataCount {
  placeholderCount?: number;
  totalCount: number;
}

// Ingest Metadata Freshness
export interface DataFreshnessResult {
  state: string;
  date: string;
  lastRefreshDate?: string;
  ingestPaused: boolean;
}

// PO Feedback responses
export interface POFeedbackResponse {
  officerExternalId: string;
  personExternalId: string;
  actionType: string;
  comment: string;
  timestamp: string;
}

export type User = {
  email: string;
  stateCode: string;
  externalId?: string;
  roles: string[];
  district?: string;
  firstName?: string;
  lastName?: string;
};

// State User Permissions
export interface AddUserRequest {
  emailAddress: string;
  stateCode: string;
  externalId: string;
  roles: string[];
  district: string;
  firstName: string;
  lastName: string;
  reason: string;
}

export interface StateUserPermissionsResponse {
  emailAddress: string;
  stateCode: string;
  allowedSupervisionLocationIds: string;
  allowedSupervisionLocationLevel: string;
  allowedApps: AllowedAppRecord | null;
  routes: RouteRecord | null;
  blockedOn: string | null;
  firstName: string;
  lastName: string;
  externalId: string;
  roles: string[];
  district: string;
  featureVariants: Partial<FeatureVariants> | null;
  userHash: string;
}

export interface StateUserForm
  extends StateUserPermissionsResponse,
    Routes,
    AllowedApps {
  useCustomPermissions: boolean;
  reason: string;
  featureVariant: {
    name: string;
    enabled: boolean;
    activeDate?: string;
  }[];
}

// State Role Permissions
export interface StateRolePermissionsResponse {
  stateCode: string;
  role: string;
  routes: RouteRecord;
  featureVariants: FeatureVariants;
  allowedApps: AllowedAppRecord;
}

export interface StateRoleForm
  extends StateRolePermissionsResponse,
    Routes,
    AllowedApps {
  reason: string;
  featureVariant: {
    name: string;
    enabled: boolean;
    activeDate?: string;
  }[];
}

export type FeatureVariantValue =
  | { activeDate?: Date; variant?: string }
  | false;
/*
 * For each feature, an optional activeDate can control when the user gets access.
 * If this is missing, access will be granted immediately.
 * The `variant` property can be used to segment users to different variants of the feature,
 * e.g. for A/B testing.
 */
export type FeatureVariants = Record<string, FeatureVariantValue>;
export type FeatureVariantRecord = Partial<FeatureVariants>;

export type Route = keyof typeof ROUTES_PERMISSIONS_LABELS;
export type Routes = Record<Route, boolean>;
export type RouteRecord = Partial<Routes>;

export type AllowedApp = keyof typeof ALLOWED_APPS_LABELS;
export type AllowedApps = Record<AllowedApp, boolean>;
export type AllowedAppRecord = Partial<AllowedApps>;

export interface RemoveFeatureVariantForm {
  fvName: string;
  reason: string;
}
