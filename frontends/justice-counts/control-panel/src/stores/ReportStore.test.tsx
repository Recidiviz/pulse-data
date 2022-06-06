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

import mockJSON from "../mocks/reportOverviews.json";
import Reports from "../pages/Reports";
import { ReportOverview } from "../shared/types";
import { rootStore, StoreProvider } from ".";

const mockUnorderedReportsMap: { [reportID: string]: ReportOverview } = {};
(mockJSON.unorderedReports as ReportOverview[]).forEach((report) => {
  mockUnorderedReportsMap[report.id] = report;
});

const mockedUseNavigate = jest.fn();
const mockedUseLocation = jest.fn();
jest.mock("react-router-dom", () => ({
  ...jest.requireActual("react-router-dom"),
  useNavigate: () => mockedUseNavigate,
  useLocation: () => mockedUseLocation,
}));

beforeEach(() => {
  rootStore.reportStore.reportOverviews = {};
  rootStore.reportStore.getReportOverviews = () => Promise.resolve();
});

test("sort in reportOverviewList", () => {
  runInAction(() => {
    rootStore.reportStore.reportOverviews = mockUnorderedReportsMap;
  });

  expect(JSON.stringify(rootStore.reportStore.reportOverviewList)).toEqual(
    JSON.stringify(mockJSON.orderedReports)
  );
});

test("loading reports", async () => {
  render(
    <StoreProvider>
      <Reports />
    </StoreProvider>
  );

  const noReportsLoaded = await screen.findByText(/Loading data.../i);
  expect(noReportsLoaded).toBeInTheDocument();

  expect.hasAssertions();
});

test("no reports to display", async () => {
  render(
    <StoreProvider>
      <Reports />
    </StoreProvider>
  );

  runInAction(() => {
    rootStore.userStore.userInfoLoaded = true;
    rootStore.reportStore.loadingOverview = false;
    rootStore.reportStore.reportOverviews = {};
  });

  const noReportsLoaded = await screen.findByText(/No reports to display./i);
  expect(noReportsLoaded).toBeInTheDocument();
  expect.hasAssertions();
});

test("displayed reports", async () => {
  render(
    <StoreProvider>
      <Reports />
    </StoreProvider>
  );

  runInAction(() => {
    rootStore.reportStore.loadingOverview = false;
    rootStore.reportStore.reportOverviews = mockUnorderedReportsMap;
  });

  // Arbitrary report dates included in mockJSON
  const april2022 = await screen.findByText(/April 2022/i);
  const december2020 = await screen.findByText(/December 2020/i);
  const annualReport2019 = await screen.findByText(/Annual Report 2019/i);
  expect(april2022).toBeInTheDocument();
  expect(december2020).toBeInTheDocument();
  expect(annualReport2019).toBeInTheDocument();

  expect.hasAssertions();
});
