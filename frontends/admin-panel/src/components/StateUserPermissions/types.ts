// Recidiviz - a data platform for criminal justice reform
// Copyright (C) 2022 Recidiviz, Inc.
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
/* eslint-disable camelcase */

export type Routes = {
  operations?: boolean;
  workflows?: boolean;
  system_libertyToPrison?: boolean;
  system_prison?: boolean;
  system_prisonToSupervision?: boolean;
  system_supervision?: boolean;
  system_supervisionToPrison?: boolean;
  system_supervisionToLiberty?: boolean;
};

export const FEATURE_VARIANTS_LABELS = {
  usTnExpiration: "US_TN Expiration",
  CompliantReportingAlmostEligible: "Compliant Reporting Almost Eligible",
  usTnSupervisionLevelDowngrade: "US_TN Supervision Level Downgrade",
};

export const WORKFLOWS_PERMISSIONS_LABELS = {
  workflows: "Workflows",
};

export const VITALS_PERMISSIONS_LABELS = {
  operations: "Vitals",
};

export const PATHWAYS_PERMISSIONS_LABELS = {
  system_libertyToPrison: "Liberty to Prison",
  system_prison: "Prison",
  system_prisonToSupervision: "Prison to Supervision",
  system_supervision: "Supervision",
  system_supervisionToPrison: "Supervision to Prison",
  system_supervisionToLiberty: "Supervision to Liberty",
};

export const ROUTES_PERMISSIONS_LABELS = {
  ...WORKFLOWS_PERMISSIONS_LABELS,
  ...VITALS_PERMISSIONS_LABELS,
  ...PATHWAYS_PERMISSIONS_LABELS,
};

export const GENERAL_PERMISSIONS_LABELS = {
  canAccessLeadershipDashboard: "Can access Leadership Dashboard",
  canAccessCaseTriage: "Can access Case Triage",
  shouldSeeBetaCharts: "Should see beta charts",
};
