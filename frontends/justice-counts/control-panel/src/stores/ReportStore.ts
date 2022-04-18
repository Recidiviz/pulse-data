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
  last_modified_at: string | null;
  editors: string[];
  status: "NOT_STARTED" | "DRAFT" | "PUBLISHED";
}

export interface CreateReportFormValuesType extends Record<string, unknown> {
  month: 1 | 2 | 3 | 4 | 5 | 6 | 7 | 8 | 9 | 10 | 11 | 12;
  year: number;
  frequency: "MONTHLY" | "ANNUAL";
}

class ReportStore {
  userStore: UserStore;

  api: API;

  reports: ReportOverview[];

  constructor(userStore: UserStore, api: API) {
    makeAutoObservable(this);

    this.api = api;
    this.userStore = userStore;
    this.reports = [];
  }

  async getReports(): Promise<void | Error> {
    try {
      const { userID, userAgencies } = this.userStore;

      if (
        userID !== undefined &&
        userAgencies !== undefined &&
        userAgencies.length > 0
      ) {
        const response = (await this.api.request({
          // TODO(#12262): Will need to revisit and update request path to handle multiple agencies
          path: `/api/reports?agency_id=${userAgencies[0].id}`,
          method: "GET",
        })) as Response;
        const allReports = await response.json();

        runInAction(() => {
          this.reports = allReports;
        });
      } else {
        Promise.reject(new Error("No user or agency information initialized."));
      }
    } catch (error) {
      if (error instanceof Error) return new Error(error.message);
    }
  }

  async createReport(
    body: Record<string, unknown>
  ): Promise<Response | Error | undefined> {
    try {
      const { userID, userAgencies } = this.userStore;
      const agencyID =
        userAgencies && userAgencies.length && userAgencies[0].id;

      if (userID !== undefined && agencyID !== undefined) {
        const response = (await this.api.request({
          path: "/api/reports",
          method: "POST",
          body: { user_id: userID, agency_id: agencyID, ...body },
        })) as Response;

        return response;
      }

      throw new Error(
        "Either invalid user/agency information or no user or agency information initialized."
      );
    } catch (error) {
      if (error instanceof Error) return new Error(error.message);
    }
  }
}

export default ReportStore;
