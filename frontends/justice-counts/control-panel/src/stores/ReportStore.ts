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
import { groupBy } from "../utils/helperUtils";
import API from "./API";
import UserStore from "./UserStore";

class ReportStore {
  userStore: UserStore;

  api: API;

  reportOverviews: { [reportID: string]: ReportOverview }; // key by report ID

  reportMetrics: { [reportID: string]: Metric[] }; // key by report ID

  reportMetricsBySystem: { [reportID: string]: { [system: string]: Metric[] } }; // key by report ID, then by system

  loadingOverview: boolean;

  constructor(userStore: UserStore, api: API) {
    makeAutoObservable(this);

    this.api = api;
    this.userStore = userStore;
    this.reportOverviews = {};
    this.reportMetrics = {};
    this.reportMetricsBySystem = {};
    this.loadingOverview = true;
  }

  get reportOverviewList(): ReportOverview[] {
    return Object.values(this.reportOverviews).sort((a, b) => {
      const dateA = new Date(a.year, a.month - 1).getTime();
      const dateB = new Date(b.year, b.month - 1).getTime();
      if (a.year === b.year) {
        // Annual reports should always be sorted before Monthly reports,
        // regardless of their month
        if (a.frequency === "ANNUAL") {
          return -1;
        }
        if (b.frequency === "ANNUAL") {
          return 1;
        }
      }
      return dateB - dateA;
    });
  }

  async getReportOverviews(): Promise<void | Error> {
    try {
      const { currentAgency } = this.userStore;
      if (currentAgency === undefined) {
        // If user is not attached to an agency,
        // no need to bother trying to load reports.
        runInAction(() => {
          this.loadingOverview = false;
        });
      }
      if (currentAgency !== undefined) {
        const response = (await this.api.request({
          path: `/api/reports?agency_id=${currentAgency.id}`,
          method: "GET",
        })) as Response;
        if (response.status === 200) {
          const allReports = await response.json();

          runInAction(() => {
            allReports.forEach((report: ReportOverview) => {
              this.reportOverviews[report.id] = report;
            });
            this.loadingOverview = false;
          });
        } else {
          const error = await response.json();
          throw new Error(error.description);
        }
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
        const metricsBySystem = groupBy(metrics, (metric) => metric.system);
        this.reportMetricsBySystem[reportID] = metricsBySystem;
        // ensure that the order of the metrics in reportMetricsBySystem
        // matches the order of the metrics in reportMetrics
        this.reportMetrics[reportID] = Object.values(metricsBySystem).flat();
      });
    } catch (error) {
      if (error instanceof Error) return new Error(error.message);
    }
  }

  async createReport(
    body: Record<string, unknown>
  ): Promise<Response | Error | undefined> {
    try {
      const { currentAgency } = this.userStore;

      if (currentAgency === undefined) {
        throw new Error(
          "Either invalid user/agency information or no user or agency information initialized."
        );
      }

      const response = (await this.api.request({
        path: "/api/reports",
        method: "POST",
        body: { agency_id: currentAgency.id, ...body },
      })) as Response;

      return response;
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

      /** Update the editor details (editors & last modified details) in real time within the report after autosave. */
      runInAction(() => this.getReportOverviews());

      return response;
    } catch (error) {
      if (error instanceof Error) return new Error(error.message);
    }
  }

  async deleteReports(
    reportIDs: number[]
  ): Promise<Response | Error | undefined> {
    try {
      const response = (await this.api.request({
        path: `/api/reports`,
        body: { report_ids: reportIDs },
        method: "DELETE",
      })) as Response;

      if (response.status !== 200) {
        throw new Error("There was an issue deleting these reports.");
      }

      runInAction(() => {
        this.resetState();
        this.getReportOverviews();
      });

      return response;
    } catch (error) {
      if (error instanceof Error) return new Error(error.message);
    }
  }

  resetState() {
    // reset the state when switching agencies
    runInAction(() => {
      this.reportOverviews = {};
      this.reportMetrics = {};
      this.reportMetricsBySystem = {};
      this.loadingOverview = true;
    });
  }
}

export default ReportStore;
