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
    key: "READMISSION_RATE",
    display_name: "Readmission Rate",
    description:
      "Measure the number of individuals admitted who had at least one other prison admission within the prior year.",
    reporting_note:
      "Exclude re-entry after a temporary exit (escape, work release, appointment, etc).",
    value: null,
    unit: "readmissions",
    category: "READMISSION_RATE",
    label: "Readmission Rate",
    definitions: [
      {
        term: "",
        definition: "",
      },
    ],
    contexts: [
      {
        key: "DEFINITION_OF_READMISSION",
        display_name: "Definition of Readmission",
        reporting_note: "Agency's definition of readmission.",
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
            key: "NEW_OFFENSE",
            label: "New Offense",
            value: null,
            reporting_note: "Readmission: New Offense",
          },
          {
            key: "VIOLATION_OF_CONDITIONS",
            label: "Violation of Conditions",
            value: null,
            reporting_note: "Readmission: Violation of Conditions",
          },
          {
            key: "OTHER",
            label: "Other",
            value: null,
            reporting_note: "Readmission: Other",
          },
          {
            key: "UNKNOWN",
            label: "Unknown",
            value: null,
            reporting_note: "Readmission: Unknown",
          },
        ],
        required: false,
        helper_text: "Break down the metric by NIBRS offense types.",
      },
    ],
  },
  {
    key: "ADMISSIONS",
    display_name: "Admissions",
    description:
      "Measure the number of new admission to the state corrections system.",
    reporting_note:
      "Report individual in the most serious category (new sentence > vilation > hold).",
    value: null,
    unit: "admissions",
    category: "ADMISSIONS",
    label: "Admissions",
    definitions: [
      {
        term: "",
        definition: "",
      },
    ],
    contexts: [
      {
        key: "DEFINITION_OF_ADMISSION",
        display_name: "Definition of Admission",
        reporting_note: "Agency's definition of admission.",
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
        key: "ADMISSIONS_KEY_TYPE",
        display_name: "Population Type",
        dimensions: [
          {
            key: "NEW_SENTENCE",
            label: "New Sentence",
            value: null,
            reporting_note: "Admissions: New Sentence",
          },
          {
            key: "TRANSFER_HOLD",
            label: "Transfer/Hold",
            value: null,
            reporting_note: "Admissions: Transfer/Hold",
          },
          {
            key: "SUPERVISION_VIOLATION_REVOCATION",
            label: "Supervision Violation/Revocation",
            value: null,
            reporting_note: "Admissions: Supervision Violation/Revocation",
          },
          {
            key: "OTHER",
            label: "Other",
            value: null,
            reporting_note: "Admissions: Other",
          },
          {
            key: "UNKNOWN",
            label: "Unknown",
            value: null,
            reporting_note: "Admissions: Unknown",
          },
        ],
        required: false,
        helper_text: "",
      },
    ],
  },
  {
    key: "AVERAGE_DAILY_POPULATION",
    display_name: "Average Daily Population",
    description:
      "Measures the average daily population held in the state corrections system.",
    reporting_note:
      "Calculate the average against a 30-day month. Report individual in the most serious category (new sentence > vilation > hold).",
    value: null,
    unit: "people",
    category: "PEOPLE",
    label: "People",
    definitions: [
      {
        term: "",
        definition: "",
      },
    ],
    contexts: [
      {
        key: "VOC_COUNT_QUESTION",
        display_name:
          "Are individuals admitted for violation of conditions counted within the above population categories?",
        reporting_note:
          "Whether individual admitted for violation of conditions are counted within or separate from the above population categories.",
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
        key: "AVG_DAILY_POP_KEY_TYPE",
        display_name: "Population Type",
        dimensions: [
          {
            key: "NEW_SENTENCE",
            label: "New Sentence",
            value: null,
            reporting_note: "Average Daily Population: New Sentence",
          },
          {
            key: "TRANSFER_HOLD",
            label: "Transfer/Hold",
            value: null,
            reporting_note: "Average Daily Population: Transfer/Hold",
          },
          {
            key: "SUPERVISION_VIOLATION_REVOCATION",
            label: "Supervision Violation/Revocation",
            value: null,
            reporting_note:
              "Average Daily Population: Supervision Violation/Revocation",
          },
          {
            key: "OTHER",
            label: "Other",
            value: null,
            reporting_note: "Average Daily Population: Other",
          },
          {
            key: "UNKNOWN",
            label: "Unknown",
            value: null,
            reporting_note: "Average Daily Population: Unknown",
          },
        ],
        required: false,
        helper_text: "",
      },
      {
        key: "RACE_ETHNICITY_KEY_TYPE",
        display_name: "Race & Ethnicity",
        dimensions: [
          {
            key: "WHITE",
            label: "White",
            value: null,
            reporting_note: "Race/Ethnicity: White",
          },
          {
            key: "BLACK",
            label: "Black",
            value: null,
            reporting_note: "Race/Ethnicity: Black",
          },
          {
            key: "HISPANIC_LATINX",
            label: "Hispanic/Latinx",
            value: null,
            reporting_note: "Race/Ethnicity: Hispanic/Latinx",
          },
          {
            key: "ASIAN",
            label: "Asian",
            value: null,
            reporting_note: "Race/Ethnicity: Asian",
          },
          {
            key: "NATIVE_AMERICAN",
            label: "Native American",
            value: null,
            reporting_note: "Race/Ethnicity: Native American",
          },
          {
            key: "NATIVE_HAWAIIAN_PI",
            label: "Native Hawaiian or Pacific Islander",
            value: null,
            reporting_note:
              "Race/Ethnicity: Native Hawaiian or Pacific Islander",
          },
          {
            key: "OTHER",
            label: "Other",
            value: null,
            reporting_note: "Race/Ethnicity: Other",
          },
          {
            key: "UNKNOWN",
            label: "Unknown",
            value: null,
            reporting_note: "Race/Ethnicity: Unknown",
          },
        ],
        required: false,
        helper_text:
          "Measure the average daily correctional population of each race/ethnic group. This is the average daily population for each group. Calculate the average against a 30-day month.",
      },
      {
        key: "GENDER_KEY_TYPE",
        display_name: "Gender",
        dimensions: [
          {
            key: "WHITE",
            label: "Male",
            value: null,
            reporting_note: "Gender: Male",
          },
          {
            key: "BLACK",
            label: "Female",
            value: null,
            reporting_note: "Gender: Female",
          },
          {
            key: "HISPANIC_LATINX",
            label: "Non-binary",
            value: null,
            reporting_note: "Gender: Non-binary",
          },
          {
            key: "OTHER",
            label: "Other",
            value: null,
            reporting_note: "Gender: Other",
          },
          {
            key: "UNKNOWN",
            label: "Unknown",
            value: null,
            reporting_note: "Gender: Unknown",
          },
        ],
        required: false,
        helper_text:
          "Measure the average daily correctional population of each gender group. This is the average daily population for each group. Calculate the average against a 30-day month.",
      },
    ],
  },
  {
    key: "RELEASES",
    display_name: "Releases",
    description: "Measure the number of releases from the facility.",
    reporting_note:
      "Exclude temporary release (work release, appointment, court hearing, etc).",
    value: null,
    unit: "releases",
    category: "RELEASES",
    label: "Releases",
    definitions: [
      {
        term: "",
        definition: "",
      },
    ],
    contexts: [
      {
        key: "DEFINITION_OF_SUPERVISION",
        display_name: "Definition of Supervision",
        reporting_note:
          "Agency's definition of supervision (probation, parole, either).",
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
        key: "RELEASE_KEY_TYPE",
        display_name: "Release Types",
        dimensions: [
          {
            key: "SENTENCE_COMPLETION",
            label: "Sentence Completion",
            value: null,
            reporting_note: "Releases: New Sentence",
          },
          {
            key: "PRETRIAL_RELEASE",
            label: "Pretrial Release",
            value: null,
            reporting_note: "Releases: Pretrial Release",
          },
          {
            key: "TRANSFER",
            label: "Transfer",
            value: null,
            reporting_note: "Releases: Transfer",
          },
          {
            key: "UNAPPROVED ABSENCE",
            label: "Unapproved Absence",
            value: null,
            reporting_note: "Releases: Unapproved Absence",
          },
          {
            key: "COMPASSIONATE",
            label: "Compassionate",
            value: null,
            reporting_note: "Releases: Compassionate",
          },
          {
            key: "OTHER",
            label: "Other",
            value: null,
            reporting_note: "Releases: Other",
          },
          {
            key: "UNKNOWN",
            label: "Unknown",
            value: null,
            reporting_note: "Releases: Unknown",
          },
        ],
        required: false,
        helper_text: "",
      },
    ],
  },
];

export const mockReport = {
  ...mockOverview,
  metrics: mockMetrics,
};

export default mockReport;
