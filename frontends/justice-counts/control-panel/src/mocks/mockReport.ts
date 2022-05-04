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

export const mockOverview = {
  id: 0,
  year: 2022,
  month: 4,
  frequency: "MONTHLY",
  last_modified_at: "April 12 2022",
  editors: ["Editor #1", "Editor #2"],
  status: "DRAFT",
};

export const mockMetrics = [
  {
    key: "PROSECUTION_STAFF",
    display_name: "Staff",
    description:
      "Measures the number of full-time staff employed by the agency.",
    reporting_note: "DOCs report only correctional institution staff.",
    value: null,
    unit: "people",
    category: "CAPACITY_AND_COST",
    label: "Total Staff",
    definitions: [
      {
        term: "full-time staff",
        definition: "definition of full-time staff",
      },
    ],
    contexts: [
      {
        key: "PROGRAMMATIC_OR_MEDICAL_STAFF",
        display_name: "Does this include programmatic or medical staff? ",
        reporting_note: null,
        required: false,
        type: "BOOLEAN",
        value: null,
      },
      {
        key: "ADDITIONAL_CONTEXT",
        display_name: "Additional Context",
        reporting_note:
          "Add any additional context that you would like to provide here.",
        required: false,
        type: "TEXT",
        value: null,
      },
    ],
    disaggregations: [
      {
        key: "PROSECUTION_STAFF_TYPE",
        display_name: "Staff Types",
        dimensions: [
          {
            key: "SUPPORT",
            label: "Support",
            value: null,
            reporting_note: "Staff: Support",
          },
          {
            key: "SECURITY",
            label: "Security",
            value: null,
            reporting_note: "Staff: Security",
          },
          {
            key: "OTHER",
            label: "Other",
            value: null,
            reporting_note: "Staff: Other",
          },
          {
            key: "UNKNOWN",
            label: "Unknown",
            value: null,
            reporting_note: "Staff: Unknown",
          },
        ],
        required: false,
        helper_text: "Break down the metric by NIBRS offense types.",
      },
    ],
  },
  {
    key: "READMISSION_KEY",
    display_name: "Readmission Rate",
    description:
      "Measure the number of individuals admitted who had at least one other prison admission within the prior year.",
    reporting_note:
      "Exclude re-entry after a temporary exit (escape, work release, appointment, etc).",
    value: null,
    unit: "readmissions",
    category: "CAPACITY_AND_COST",
    label: "Readmission Rate",
    definitions: [
      {
        term: "full-time staff",
        definition: "definition of full-time staff",
      },
    ],
    contexts: [
      {
        key: "DEFINITION_OF_ADMISSION",
        display_name: "Definition of Admission",
        reporting_note: "",
        required: false,
        type: "TEXT",
        value: null,
      },
      {
        key: "ADDITIONAL_CONTEXT",
        display_name: "Additional Context",
        reporting_note:
          "Add any additional context that you would like to provide here.",
        required: false,
        type: "TEXT",
        value: null,
      },
    ],
    disaggregations: [
      {
        key: "READMISSION_KEY_TYPE",
        display_name: "Readmission Types",
        dimensions: [
          {
            key: "SUPPORT2",
            label: "Support",
            value: null,
            reporting_note: "Staff: Support",
          },
          {
            key: "SECURITY2",
            label: "Security",
            value: null,
            reporting_note: "Staff: Security",
          },
          {
            key: "OTHER2",
            label: "Other",
            value: null,
            reporting_note: "Staff: Other",
          },
          {
            key: "UNKNOWN2",
            label: "Unknown",
            value: null,
            reporting_note: "Staff: Unknown",
          },
        ],
        required: false,
        helper_text: "Break down the metric by NIBRS offense types.",
      },
    ],
  },
  {
    key: "ANNUAL_BUDGET_KEY",
    display_name: "Annual Budget",
    reporting_note:
      "Sheriff offices report on budget for patrol and detention separately",
    description: "Measures the total annual budget (in dollars) of the agency.",
    definitions: [],
    category: "CAPACITY AND COST",
    value: null,
    unit: "USD",
    label: "Total Annual Budget",
    contexts: [
      {
        key: "PRIMARY_FUNDING_SOURCE",
        display_name: "Primary Funding Source",
        reporting_note: "",
        required: false,
        type: "TEXT",
        value: null,
      },
      {
        key: "ADDITIONAL_CONTEXT",
        display_name: "Additional Context",
        reporting_note: null,
        required: false,
        type: "TEXT",
        value: null,
      },
    ],
    disaggregations: [
      {
        key: "metric/law_enforcement/budget/type",
        display_name: "Budget Breakdown",
        required: false,
        should_sum_to_total: true,
        helper_text: null,
        dimensions: [
          {
            key: "DETENTION",
            label: "Detention",
            value: null,
            reporting_note: "Sheriff Budget: Detention",
          },
          {
            key: "PATROL",
            label: "Patrol",
            value: null,
            reporting_note: "Sheriff Budget: Patrol",
          },
        ],
      },
    ],
  },
];

export const mockReport = {
  ...mockOverview,
  metrics: mockMetrics,
};

export default mockReport;
