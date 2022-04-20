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

export interface Report extends ReportOverview {
  metrics: Metric[];
}

export interface Metric {
  key: string;
  display_name: string;
  description: string;
  reporting_note: string;
  value: string | undefined;
  contexts: MetricContext[];
  disaggregations: MetricDisaggregations[];
}

export interface MetricContext {
  key: string;
  description: string;
  required: boolean;
  value: string | undefined;
}

export interface MetricDisaggregations {
  name: string;
  dimensions: { [name: string]: string | number | boolean };
  required: boolean;
  should_sum_to_total: boolean;
}

export interface CreateReportFormValuesType extends Record<string, unknown> {
  month: 1 | 2 | 3 | 4 | 5 | 6 | 7 | 8 | 9 | 10 | 11 | 12;
  year: number;
  frequency: "MONTHLY" | "ANNUAL";
}

class ReportStore {
  userStore: UserStore;

  api: API;

  reportOverviews: { [reportID: string]: ReportOverview }; // key by report ID

  reportMetrics: { [reportID: string]: Metric[] }; // key by report ID

  constructor(userStore: UserStore, api: API) {
    makeAutoObservable(this);

    this.api = api;
    this.userStore = userStore;
    this.reportOverviews = {};
    this.reportMetrics = {};
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
      const { userID, userAgency } = this.userStore;

      if (userID !== undefined && userAgency !== undefined) {
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
        });
      } else {
        throw new Error("No user or agency information initialized.");
      }
    } catch (error) {
      if (error instanceof Error) return new Error(error.message);
    }
  }

  async getReport(reportID: number): Promise<void | Error> {
    try {
      const { userID, userAgency } = this.userStore;

      if (userID !== undefined && userAgency !== undefined) {
        const response = (await this.api.request({
          // TODO(#12262): Will need to revisit and update request path to handle multiple agencies
          path: `/api/reports/${reportID}?agency_id=${userAgency.id}`,
          method: "GET",
        })) as Response;
        const report = await response.json();

        runInAction(() => {
          const { metrics, ...info } = report;
          this.reportOverviews[reportID] = info;
          this.reportMetrics[reportID] = metrics;
        });
        return report;
      }
      throw new Error(
        "Either invalid user/agency information or no user or agency information initialized."
      );
    } catch (error) {
      if (error instanceof Error) return new Error(error.message);
    }
  }

  async updateReport(reportID: number, body: Partial<Report>) {
    try {
      const { userID, userAgency } = this.userStore;

      if (userID !== undefined && userAgency !== undefined) {
        const response = (await this.api.request({
          path: `/api/reports/${reportID}`,
          method: "PUT",
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

  async createReport(
    body: Record<string, unknown>
  ): Promise<Response | Error | undefined> {
    try {
      const { userID, userAgency } = this.userStore;

      if (userID !== undefined && userAgency !== undefined) {
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
}

export default ReportStore;
