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

import { fireEvent, render, screen } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { runInAction } from "mobx";
import React from "react";
import { MemoryRouter, Route, Routes } from "react-router-dom";

import { rootStore, StoreProvider } from "../../stores";
import ReportDataEntry from "./ReportDataEntry";

test("display loading when no reports are loaded", () => {
  // render(
  //   <StoreProvider>
  //     <ReportDataEntry />
  //   </StoreProvider>
  // );
  // const loadingText = screen.getByText(/Loading.../i);
  // expect(loadingText).toBeInTheDocument();
  // expect.hasAssertions();
});

describe("test data entry form", () => {
  runInAction(() => {
    rootStore.reportStore.reportOverviews = {
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

    rootStore.reportStore.reportMetrics = {
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
              type: "BOOLEAN",
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

  test("displays data entry form based on reports", () => {
    render(
      <StoreProvider>
        <MemoryRouter initialEntries={["/reports/0"]}>
          <Routes>
            <Route path="/reports/:id" element={<ReportDataEntry />} />
          </Routes>{" "}
        </MemoryRouter>
      </StoreProvider>
    );

    const reportDate = screen.getByText("April 2022");
    const displayName = screen.getAllByText("Staff")[0];
    const metricDescription = screen.getAllByText(
      "Measures the number of full-time staff employed by the agency."
    )[0];
    const context = screen.getAllByText(
      "Does this include programmatic or medical staff?"
    )[0];

    expect(reportDate).toBeInTheDocument();
    expect(displayName).toBeInTheDocument();
    expect(metricDescription).toBeInTheDocument();
    expect(context).toBeInTheDocument();

    expect.hasAssertions();
  });

  test("toggle switch shows and hides disaggregation dimensions", () => {
    render(
      <StoreProvider>
        <MemoryRouter initialEntries={["/reports/0"]}>
          <Routes>
            <Route path="/reports/:id" element={<ReportDataEntry />} />
          </Routes>
        </MemoryRouter>
      </StoreProvider>
    );

    const disaggregationToggle = screen.getByRole("checkbox");
    const disaggregationDimensionField =
      screen.getAllByText("Staff: Support")[0];

    expect(disaggregationDimensionField).toBeInTheDocument();

    userEvent.click(disaggregationToggle);

    expect(disaggregationDimensionField).not.toBeInTheDocument();

    expect.hasAssertions();
  });
});

test("expect positive number value to not add field error (formErrors should be an empty object)", () => {
  render(
    <StoreProvider>
      <MemoryRouter initialEntries={["/reports/0"]}>
        <Routes>
          <Route path="/reports/:id" element={<ReportDataEntry />} />
        </Routes>
      </MemoryRouter>
    </StoreProvider>
  );

  const input = screen.getAllByLabelText("Total Staff")[0];

  fireEvent.change(input, { target: { value: "1000" } });

  expect(JSON.stringify(rootStore.formStore.formErrors)).toBe(
    JSON.stringify({})
  );
});

test("expect negative number value to add field error (formErrors should contain an error property for the field)", () => {
  render(
    <StoreProvider>
      <MemoryRouter initialEntries={["/reports/0"]}>
        <Routes>
          <Route path="/reports/:id" element={<ReportDataEntry />} />
        </Routes>
      </MemoryRouter>
    </StoreProvider>
  );

  const input = screen.getAllByLabelText("Total Staff")[0];

  fireEvent.change(input, { target: { value: "-1000" } });

  expect(JSON.stringify(rootStore.formStore.formErrors)).toBe(
    JSON.stringify({ PROSECUTION_STAFF: { PROSECUTION_STAFF: "Error" } })
  );
});
