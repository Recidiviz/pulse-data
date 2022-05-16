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

import {
  Metric,
  Report,
  ReportOverview,
  ReportStatus,
  UpdatedMetricsValues,
} from "../shared/types";
import API from "./API";
import UserStore from "./UserStore";

class ReportStore {
  userStore: UserStore;

  api: API;

  reportOverviews: { [reportID: string]: ReportOverview }; // key by report ID

  reportMetrics: { [reportID: string]: Metric[] }; // key by report ID

  loadingOverview: boolean;

  constructor(userStore: UserStore, api: API) {
    makeAutoObservable(this);

    this.api = api;
    this.userStore = userStore;
    this.reportOverviews = {};
    this.reportMetrics = {};
    this.loadingOverview = true;
  }

  get reportOverviewList(): ReportOverview[] {
    return Object.values(this.reportOverviews).sort((a, b) => {
      const dateA = new Date(a.year, a.month - 1);
      const dateB = new Date(b.year, b.month - 1);
      return dateB.getTime() - dateA.getTime();
    });
  }

  async getReportOverviews(): Promise<void | Error> {
    try {
      const { userAgency } = this.userStore;

      if (userAgency !== undefined) {
        const response = (await this.api.request({
          // TODO(#12262): Will need to revisit and update request path to handle multiple agencies
          path: `/api/reports?agency_id=${userAgency.id}`,
          method: "GET",
        })) as Response;
        const allReports = await response.json();

        runInAction(() => {
          allReports.forEach((report: ReportOverview) => {
            this.reportOverviews[report.id] = report;
          });
          this.loadingOverview = false;
        });
      } else {
        throw new Error("No user or agency information initialized.");
      }
    } catch (error) {
      runInAction(() => {
        this.loadingOverview = false;
      });
      if (error instanceof Error) return new Error(error.message);
    }
  }

  async getReport(reportID: number): Promise<void | Error> {
    try {
      const response = (await this.api.request({
        path: `/api/reports/${reportID}`,
        method: "GET",
      })) as Response;

      if (response.status !== 200) {
        throw new Error("There was an issue getting this report.");
      }

      const report = (await response.json()) as Report;
      const { metrics, ...overview } = report;

      runInAction(() => {
        this.reportOverviews[reportID] = overview;
        this.reportMetrics[reportID] = metrics;
        // this.reportMetrics[reportID] = mockReport.metrics;
      });
    } catch (error) {
      if (error instanceof Error) return new Error(error.message);
    }
  }

  async createReport(
    body: Record<string, unknown>
  ): Promise<Response | Error | undefined> {
    try {
      const { userAgency } = this.userStore;

      if (userAgency !== undefined) {
        const response = (await this.api.request({
          path: "/api/reports",
          method: "POST",
          body: { agency_id: userAgency.id, ...body },
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

  async updateReport(
    reportID: number,
    updatedMetrics: UpdatedMetricsValues[],
    status: ReportStatus
  ): Promise<Response | Error | undefined> {
    try {
      const response = (await this.api.request({
        path: `/api/reports/${reportID}`,
        body: { status, metrics: updatedMetrics },
        method: "POST",
      })) as Response;

      if (response.status !== 200) {
        throw new Error("There was an issue updating this report.");
      }

      return response;
    } catch (error) {
      if (error instanceof Error) return new Error(error.message);
    }
  }
}

export default ReportStore;
