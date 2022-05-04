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
    value: 1000,
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
    key: "PROSECUTION_STAFF2",
    display_name: "Staff2",
    description:
      "Measures the number of full-time staff employed by the agency.",
    reporting_note: "DOCs report only correctional institution staff.",
    value: 1000,
    unit: "people",
    category: "CAPACITY_AND_COST",
    label: "Total Staff2",
    definitions: [
      {
        term: "full-time staff",
        definition: "definition of full-time staff",
      },
    ],
    contexts: [
      {
        key: "PROGRAMMATIC_OR_MEDICAL_STAFF2",
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
        key: "PROSECUTION_STAFF_TYPE2",
        display_name: "Staff Types",
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
];

export const mockReport = {
  ...mockOverview,
  metrics: mockMetrics,
};

export default mockReport;
