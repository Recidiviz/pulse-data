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

import { render, screen } from "@testing-library/react";
import { runInAction } from "mobx";
import React from "react";
import { BrowserRouter } from "react-router-dom";

import Reports from "../../pages/Reports";
import { rootStore, StoreProvider } from "../../stores";

const mockedUseNavigate = jest.fn();
const mockedUseLocation = jest.fn();
jest.mock("react-router-dom", () => ({
  ...jest.requireActual("react-router-dom"),
  useNavigate: () => mockedUseNavigate,
  useLocation: () => mockedUseLocation,
}));

beforeEach(() => {
  rootStore.reportStore.reportOverviews = {};
  rootStore.reportStore.loadingOverview = false;
});

test("displayed created reports", async () => {
  render(
    <StoreProvider>
      <Reports />
    </StoreProvider>
  );

  await runInAction(() => {
    rootStore.reportStore.reportOverviews = {
      0: {
        id: 0,
        agency_id: 0,
        month: 11,
        year: 2022,
        frequency: "MONTHLY",
        last_modified_at: null,
        editors: ["Editor #1"],
        status: "NOT_STARTED",
      },
    };
  });

  const jan2022 = screen.getByText(/November 2022/i);
  const editor1 = screen.getByText(/Editor #1/i);

  expect(jan2022).toBeInTheDocument();
  expect(editor1).toBeInTheDocument();

  await runInAction(() => {
    rootStore.reportStore.reportOverviews[1] = {
      id: 1,
      agency_id: 0,
      month: 11,
      year: 2020,
      frequency: "ANNUAL",
      last_modified_at: null,
      editors: ["Editor #2"],
      status: "NOT_STARTED",
    };
  });

  const annualReport2020 = screen.getByText(/Annual Report 2020/i);
  const editor2 = screen.getByText(/Editor #2/i);

  expect(annualReport2020).toBeInTheDocument();
  expect(editor2).toBeInTheDocument();

  expect.hasAssertions();
});

describe("test create report button", () => {
  test("created reports button should not be displayed if user does not have permission", () => {
    render(
      <StoreProvider>
        <BrowserRouter>
          <Reports />
        </BrowserRouter>
      </StoreProvider>
    );

    runInAction(() => {
      rootStore.userStore.permissions = [""];
    });

    const selectButton = screen.queryByText(/Select/i);
    const createNewReportButton = screen.queryByText(/New/i);

    expect(selectButton).not.toBeInTheDocument();
    expect(createNewReportButton).not.toBeInTheDocument();
  });

  test("created reports button should be displayed if user has permission", () => {
    render(
      <StoreProvider>
        <BrowserRouter>
          <Reports />
        </BrowserRouter>
      </StoreProvider>
    );

    runInAction(() => {
      rootStore.userStore.permissions = ["create:report:all"];
    });

    const selectButton = screen.queryByText(/Select/i);
    const createNewReportButton = screen.queryByText(/New/i);

    expect(selectButton).toBeInTheDocument();
    expect(createNewReportButton).toBeInTheDocument();
  });
});
