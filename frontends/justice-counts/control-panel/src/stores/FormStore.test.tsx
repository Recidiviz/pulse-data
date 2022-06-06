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

import { runInAction } from "mobx";

import { rootStore } from ".";

const { reportStore, formStore } = rootStore;

beforeEach(() => {
  runInAction(() => {
    reportStore.reportOverviews = {
      0: {
        id: 0,
        year: 2022,
        month: 4,
        frequency: "MONTHLY",
        last_modified_at: "April 12 2022",
        editors: ["Editor #1", "Editor #2"],
        status: "DRAFT",
      },
    };

    reportStore.reportMetrics = {
      0: [
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
              display_name: "Does this include programmatic or medical staff?",
              reporting_note: null,
              required: false,
              type: "MULTIPLE_CHOICE",
              multiple_choice_options: ["YES", "NO"],
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
              ],
              required: false,
              helper_text: "Break down the metric by NIBRS offense types.",
            },
          ],
        },
      ],
    };
  });
});

test("metrics value handler updates the metric value", () => {
  formStore.updateMetricsValues(0, "PROSECUTION_STAFF", "2000");

  expect(formStore.metricsValues[0].PROSECUTION_STAFF.value).toEqual("2000");

  expect.hasAssertions();
});

test("disaggregation dimension value handler updates the disaggregation dimension value", () => {
  formStore.updateDisaggregationDimensionValue(
    0,
    "PROSECUTION_STAFF",
    "PROSECUTION_STAFF_TYPE",
    "SUPPORT",
    "200",
    false
  );

  expect(
    formStore.disaggregations[0].PROSECUTION_STAFF.PROSECUTION_STAFF_TYPE
      .SUPPORT.value
  ).toEqual("200");

  expect.hasAssertions();
});

test("context value handler updates the context value", () => {
  formStore.updateContextValue(
    0,
    "PROSECUTION_STAFF",
    "PROGRAMMATIC_OR_MEDICAL_STAFF",
    "100",
    false,
    "NUMBER"
  );

  expect(
    formStore.contexts[0].PROSECUTION_STAFF.PROGRAMMATIC_OR_MEDICAL_STAFF.value
  ).toEqual("100");

  expect.hasAssertions();
});

test("updatedReportValues maps all updated (and not updated) input values into required data structure", () => {
  expect(JSON.stringify(formStore.reportUpdatedValuesForBackend(0))).toEqual(
    JSON.stringify([
      {
        key: "PROSECUTION_STAFF",
        value: 2000,
        contexts: [{ key: "PROGRAMMATIC_OR_MEDICAL_STAFF", value: 100 }],
        disaggregations: [
          {
            key: "PROSECUTION_STAFF_TYPE",
            dimensions: [{ key: "SUPPORT", value: 200 }],
          },
        ],
      },
    ])
  );
});
