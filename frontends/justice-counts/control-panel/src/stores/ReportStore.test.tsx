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

import mockJSON from "../../public/mocks/reports.json";
import Reports from "../pages/Reports";
import { rootStore, StoreProvider } from ".";
import { ReportOverview } from "./ReportStore";

beforeEach(() => {
  rootStore.reportStore.reports = [];
});

test("displayed reports", () => {
  render(
    <StoreProvider>
      <Reports />
    </StoreProvider>
  );

  runInAction(() => {
    rootStore.reportStore.reports = mockJSON.reports as ReportOverview[];
  });

  // Arbitrary report dates included in mockJSON
  const april2022 = screen.getByText(/April 2022/i);
  const december2020 = screen.getByText(/December 2020/i);
  const annualReport2019 = screen.getByText(/Annual Report 2019/i);

  expect(april2022).toBeInTheDocument();
  expect(december2020).toBeInTheDocument();
  expect(annualReport2019).toBeInTheDocument();

  expect.hasAssertions();
});

test("no reports to display", () => {
  render(
    <StoreProvider>
      <Reports />
    </StoreProvider>
  );

  const noReportsLoaded = screen.getByText(/No reports to display./i);
  expect(noReportsLoaded).toBeInTheDocument();

  expect.hasAssertions();
});
