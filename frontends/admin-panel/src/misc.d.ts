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

interface MetadataRecord<T> {
  name: string;
  resultsByState: {
    [stateCode: string]: T;
  };
}

// Ingest Metadata Column Counts
interface MetadataAPIResult {
  [name: string]: {
    [stateCode: string]: MetadataCount;
  };
}

interface MetadataCount {
  placeholderCount?: number;
  totalCount: number;
}

// Ingest Metadata Freshness
interface DataFreshnessResult {
  state: string;
  date: string;
  lastRefreshDate?: string;
  ingestPaused: boolean;
}

// PO Feedback responses
interface POFeedbackResponse {
  officerExternalId: string;
  personExternalId: string;
  actionType: string;
  comment: string;
  timestamp: string;
}

// State User Permissions
interface StateUserPermissionsResponse {
  emailAddress: string;
  stateCode: string;
  allowedSupervisionLocationIds: string;
  allowedSupervisionLocationLevel: string;
  canAccessLeadershipDashboard: boolean;
  canAccessCaseTriage: boolean;
  routes: {
    [route: string]: boolean;
  };
  shouldSeeBetaCharts: boolean;
  blocked: boolean;
  firstName: string;
  lastName: string;
  externalId: string;
  role: string;
  district: string;
}
