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

export const WORKFLOWS_PERMISSIONS_LABELS = {
  workflowsSupervision: "Supervision Workflows",
  workflowsFacilities: "Facilities Workflows",
  tasks: "Tasks",
};

export const VITALS_PERMISSIONS_LABELS = {
  operations: "Vitals",
};

export const INSIGHTS_PERMISSIONS_LABELS = {
  insights: "Supervisor Homepage (Insights)",
  "insights_supervision_supervisors-list":
    "Supervisor Homepage Leadership Navigator",
};

export const PATHWAYS_PERMISSIONS_LABELS = {
  system_libertyToPrison: "Liberty to Prison",
  system_prison: "Prison",
  system_prisonToSupervision: "Prison to Supervision",
  system_supervision: "Supervision",
  system_supervisionToPrison: "Supervision to Prison",
  system_supervisionToLiberty: "Supervision to Liberty",
};

export const PSI_PERMISSIONS_LABELS = {
  psi: "PSI",
  psiSupervision: "PSI Supervisor View",
};

export const CPA_PERMISSIONS_LABELS = {
  cpa: "CPA",
};

export const LANTERN_PERMISSIONS_LABELS = {
  lantern: "Lantern (legacy)",
};

export const ROUTES_PERMISSIONS_LABELS = {
  ...WORKFLOWS_PERMISSIONS_LABELS,
  ...VITALS_PERMISSIONS_LABELS,
  ...INSIGHTS_PERMISSIONS_LABELS,
  ...PATHWAYS_PERMISSIONS_LABELS,
  ...PSI_PERMISSIONS_LABELS,
  ...CPA_PERMISSIONS_LABELS,
  ...LANTERN_PERMISSIONS_LABELS,
} satisfies Record<string, string>;

export const ALLOWED_APPS_LABELS = {
  staff: "Staff (dashboard.recidiviz.org)",
  jii: "JII (opportunities.app)",
} satisfies Record<string, string>;

export const STATE_CODES_TO_NAMES = {
  US_AK: "Alaska",
  US_AL: "Alabama",
  US_AR: "Arkansas",
  US_AS: "American Samoa",
  US_AZ: "Arizona",
  US_CA: "California",
  US_CO: "Colorado",
  US_CT: "Connecticut",
  US_DC: "District of Columbia",
  US_DE: "Delaware",
  US_FL: "Florida",
  US_GA: "Georgia",
  US_GU: "Guam",
  US_HI: "Hawaii",
  US_IA: "Iowa",
  US_ID: "Idaho",
  US_IL: "Illinois",
  US_IN: "Indiana",
  US_KS: "Kansas",
  US_KY: "Kentucky",
  US_LA: "Louisiana",
  US_MA: "Massachusetts",
  US_MD: "Maryland",
  US_ME: "Maine",
  US_MI: "Michigan",
  US_MN: "Minnesota",
  US_MO: "Missouri",
  US_MP: "Northern Mariana Islands",
  US_MS: "Mississippi",
  US_MT: "Montana",
  US_NC: "North Carolina",
  US_ND: "North Dakota",
  US_NE: "Nebraska",
  US_NH: "New Hampshire",
  US_NJ: "New Jersey",
  US_NM: "New Mexico",
  US_NV: "Nevada",
  US_NY: "New York",
  US_OH: "Ohio",
  US_OK: "Oklahoma",
  US_OR: "Oregon",
  US_PA: "Pennsylvania",
  US_PR: "Puerto Rico",
  US_RI: "Rhode Island",
  US_SC: "South Carolina",
  US_SD: "South Dakota",
  US_TN: "Tennessee",
  US_TX: "Texas",
  US_UT: "Utah",
  US_VA: "Virginia",
  US_VI: "U.S. Virgin Islands",
  US_VT: "Vermont",
  US_WA: "Washington",
  US_WI: "Wisconsin",
  US_WV: "West Virginia",
  US_WY: "Wyoming",
};
