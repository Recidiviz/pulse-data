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

export const formLayout = {
  labelCol: { span: 4 },
  wrapperCol: { span: 20 },
};
export const formTailLayout = {
  wrapperCol: { offset: 4, span: 20 },
};

export const WORKFLOWS_PERMISSIONS_LABELS = {
  workflowsSupervision: "Supervision Workflows",
  workflowsFacilities: "Facilities Workflows",
};

export const VITALS_PERMISSIONS_LABELS = {
  operations: "Vitals",
};

export const OUTLIERS_PERMISSIONS_LABELS = {
  insights: "Outliers",
  "insights_supervision_supervisors-list": "Outliers Leadership Navigator",
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
  ...OUTLIERS_PERMISSIONS_LABELS,
  ...PATHWAYS_PERMISSIONS_LABELS,
};
