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

import { render, screen, waitFor } from "@testing-library/react";
import { runInAction } from "mobx";
import React from "react";
import { MemoryRouter, Route, Routes } from "react-router-dom";

import { rootStore, StoreProvider } from "../../stores";
import ReportDataEntry from "./ReportDataEntry";

beforeEach(() => {
  const mockIntersectionObserver = jest.fn();
  mockIntersectionObserver.mockReturnValue({
    observe: () => null,
    unobserve: () => null,
    disconnect: () => null,
  });
  window.IntersectionObserver = mockIntersectionObserver;
});

test("display loading when no reports are loaded", async () => {
  render(
    <StoreProvider>
      <ReportDataEntry />
    </StoreProvider>
  );
  await waitFor(async () => {
    const loading = screen.getByTestId("loading");
    expect(loading).toBeInTheDocument();
  });
  expect.hasAssertions();
});

test("display error when report fails to load", async () => {
  render(
    <StoreProvider>
      <ReportDataEntry />
    </StoreProvider>
  );

  runInAction(() => {
    rootStore.userStore.userInfoLoaded = true;
  });

  const errorText = await screen.findByText(
    /Error: No auth client initialized./i
  );
  expect(errorText).toBeInTheDocument();
  expect.hasAssertions();
});

describe("test data entry form", () => {
  runInAction(() => {
    rootStore.userStore.userInfoLoaded = true;

    rootStore.reportStore.reportOverviews = {
      0: {
        id: 0,
        agency_id: 0,
        year: 2022,
        month: 4,
        frequency: "MONTHLY",
        last_modified_at: "April 12 2022",
        last_modified_at_timestamp: null,
        editors: ["Editor #1", "Editor #2"],
        status: "DRAFT",
      },
    };

    rootStore.reportStore.reportMetricsBySystem = {
      0: {
        "Law Enforcement": [
          {
            key: "PROSECUTION_STAFF",
            system: "Law Enforcement",
            display_name: "Staff",
            description:
              "Measures the number of full-time staff employed by the agency.",
            reporting_note: "DOCs report only correctional institution staff.",
            value: 1000,
            unit: "people",
            category: "CAPACITY_AND_COST",
            label: "Total Staff",
            enabled: true,
            definitions: [
              {
                term: "full-time staff",
                definition: "definition of full-time staff",
              },
            ],
            contexts: [
              {
                key: "PROGRAMMATIC_OR_MEDICAL_STAFF",
                display_name:
                  "Does this include programmatic or medical staff?",
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
      },
    };

    rootStore.reportStore.reportMetrics = {
      0: Object.values(rootStore.reportStore.reportMetricsBySystem[0]).flat(),
    };
  });

  test("displays data entry form based on reports", async () => {
    render(
      <StoreProvider>
        <MemoryRouter initialEntries={["/reports/0"]}>
          <Routes>
            <Route path="/reports/:id" element={<ReportDataEntry />} />
          </Routes>{" "}
        </MemoryRouter>
      </StoreProvider>
    );

    const reportDate = await screen.findByText("April 2022");

    expect(reportDate).toBeInTheDocument();

    const displayName = screen.getAllByText("Staff")[0];
    const metricDescription = screen.getAllByText(
      "Measures the number of full-time staff employed by the agency."
    )[0];
    const context = screen.getAllByText(
      "Does this include programmatic or medical staff?"
    )[0];
    expect(displayName).toBeInTheDocument();
    expect(metricDescription).toBeInTheDocument();
    expect(context).toBeInTheDocument();

    expect.hasAssertions();
  });
});

// TODO(#13325) JSDOM does not recognize `.animate` as a function. Will need to refactor toast or this test.

// test("expect positive number value to not add field error (formErrors should be an empty object)", async () => {
//   render(
//     <StoreProvider>
//       <MemoryRouter initialEntries={["/reports/0"]}>
//         <Routes>
//           <Route path="/reports/:id" element={<ReportDataEntry />} />
//         </Routes>
//       </MemoryRouter>
//     </StoreProvider>
//   );

//   const labels = await screen.findAllByLabelText("Total Staff");
//   fireEvent.change(labels[0], { target: { value: "100" } });
//   expect(
//     rootStore.formStore.metricsValues[0].PROSECUTION_STAFF.error
//   ).toBeUndefined();
// });

// test("expect negative number value to add field error (formErrors should contain an error property for the field)", async () => {
//   render(
//     <StoreProvider>
//       <MemoryRouter initialEntries={["/reports/0"]}>
//         <Routes>
//           <Route path="/reports/:id" element={<ReportDataEntry />} />
//         </Routes>
//       </MemoryRouter>
//     </StoreProvider>
//   );

//   const labels = await screen.findAllByLabelText("Total Staff");
//   fireEvent.change(labels[0], { target: { value: "-100" } });
//   expect(rootStore.formStore.metricsValues[0].PROSECUTION_STAFF.error).toBe(
//     "Please enter a valid number."
//   );
// });

// test("expect empty value in required field to add field error (formErrors should contain an error property for the field)", async () => {
//   render(
//     <StoreProvider>
//       <MemoryRouter initialEntries={["/reports/0"]}>
//         <Routes>
//           <Route path="/reports/:id" element={<ReportDataEntry />} />
//         </Routes>
//       </MemoryRouter>
//     </StoreProvider>
//   );

//   const labels = await screen.findAllByLabelText("Total Staff");
//   fireEvent.change(labels[0], { target: { value: "" } });
//   expect(rootStore.formStore.metricsValues[0].PROSECUTION_STAFF.error).toBe(
//     "This is a required field."
//   );
// });
