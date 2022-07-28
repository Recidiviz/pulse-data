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

export const metricsViewMockResponse = [
  {
    key: "LAW_ENFORCEMENT_BUDGET__metric/law_enforcement/budget/type",
    display_name: "Annual Budget",
    description: "Measures the total annual budget (in dollars) of the agency.",
    frequency: "ANNUAL",
    enabled: true,
    contexts: [
      {
        key: "PRIMARY_FUNDING_SOURCE",
        display_name: "Binary question?",
        reporting_note: "put your primary funding source here",
        required: false,
        type: "BOOLEAN",
        value: "government funding",
      },
      {
        key: "PRIMARY_FUNDING_SOURCE2",
        display_name: "Multiple choice",
        reporting_note: "put your primary funding source here",
        required: false,
        type: "MULTIPLE_CHOICE",
        multiple_choice_options: [
          "Choice 1",
          "Choice 2",
          "Choice 3",
          "Choice 4",
        ],
        value: "government funding",
      },
      {
        key: "ADDITIONAL_CONTEXT",
        display_name: "Additional context",
        reporting_note: "Any additional context you want to provide",
        required: false,
        type: "TEXT",
        value: "we are special, here's why",
      },
    ],
    disaggregations: [
      {
        key: "law_enforcement/staff/type",
        display_name: "Staff Types",
        enabled: false,
        dimensions: [
          {
            key: "SUPPORT",
            label: "Support",
            reporting_note: "Staff: Support",
            enabled: false,
          },
          {
            key: "SECURITY",
            label: "Security",
            reporting_note: "Staff: Security",
            enabled: true,
          },
        ],
      },
    ],
  },
  {
    key: "READMISSION_RATE",
    display_name: "Readmission Rate",
    description:
      "Measure the number of individuals admitted who had at least one other prison admission within the prior year.",
    frequency: "MONTHLY",
    enabled: false,
    contexts: [
      {
        key: "DEFINITION_OF_READMISSION",
        display_name: "Definition of Readmission",
        reporting_note: "Agency's definition of readmission.",
        required: false,
        type: "NUMBER",
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
        enabled: true,
        dimensions: [
          {
            key: "NEW_OFFENSE",
            label: "New Offense",
            reporting_note: "Readmission: New Offense",
            enabled: true,
          },
          {
            key: "VIOLATION_OF_CONDITIONS",
            label: "Violation of Conditions",
            reporting_note: "Readmission: Violation of Conditions",
            enabled: true,
          },
          {
            key: "OTHER",
            label: "Other",
            reporting_note: "Readmission: Other",
            enabled: true,
          },
          {
            key: "UNKNOWN",
            label: "Unknown",
            reporting_note: "Readmission: Unknown",
            enabled: true,
          },
          {
            key: "NEW_OFFENSE2",
            label: "New Offense",
            reporting_note: "Readmission: New Offense",
            enabled: true,
          },
          {
            key: "VIOLATION_OF_CONDITIONS2",
            label: "Violation of Conditions",
            reporting_note: "Readmission: Violation of Conditions",
            enabled: true,
          },
          {
            key: "OTHER2",
            label: "Other",
            reporting_note: "Readmission: Other",
            enabled: true,
          },
          {
            key: "UNKNOWN2",
            label: "Unknown",
            reporting_note: "Readmission: Unknown",
            enabled: true,
          },
        ],
      },
    ],
  },
];
