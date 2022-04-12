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

import { makeAutoObservable, runInAction } from "mobx";

import API from "./API";
import UserStore from "./UserStore";

export interface ReportOverview {
  id: number;
  month: number;
  year: number;
  frequency: "MONTHLY" | "ANNUAL";
  last_modified_at: string;
  editors: string[];
  status: "NOT_STARTED" | "DRAFT" | "PUBLISHED";
}

class ReportStore {
  userStore: UserStore;

  api: API;

  reports: ReportOverview[];

  filteredReports: ReportOverview[];

  constructor(userStore: UserStore, api: API) {
    makeAutoObservable(this);

    this.api = api;
    this.userStore = userStore;
    this.reports = [];
    this.filteredReports = [];
  }

  async getReports(): Promise<void> {
    try {
      const response = (await this.api.request({
        path: "./mocks/reports.json",
        method: "GET",
      })) as Response;
      const allReports = await response.json();

      runInAction(() => {
        this.reports = allReports.reports; // maintains a master copy of all reports retrieved
        this.filteredReports = this.reports; // what will be updated & rendered in the view
      });
    } catch (error) {
      if (error instanceof Error) throw new Error(error.message);
    }
  }
}

export default ReportStore;
