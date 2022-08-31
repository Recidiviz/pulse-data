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
  DatapointsByMetric,
  DataVizAggregateName,
  DimensionNamesByMetricAndDisaggregation,
  RawDatapoint,
} from "../shared/types";
import { isPositiveNumber } from "../utils";
import API from "./API";
import UserStore from "./UserStore";

class DatapointsStore {
  userStore: UserStore;

  api: API;

  rawDatapoints: RawDatapoint[];

  dimensionNamesByMetricAndDisaggregation: DimensionNamesByMetricAndDisaggregation;

  loading: boolean;

  constructor(userStore: UserStore, api: API) {
    makeAutoObservable(this);

    this.api = api;
    this.userStore = userStore;
    this.rawDatapoints = [];
    this.dimensionNamesByMetricAndDisaggregation = {};
    this.loading = true;
  }

  /**
   * Transforms raw data from the server into Datapoints keyed by metric,
   * grouped by aggregate values and disaggregations.
   * Aggregate is an array of objects each containing start_date, end_date, and the aggregate value.
   * Disaggregations are keyed by disaggregation name and each value is an object
   * with the key being the start_date and the value being an object
   * containing start_date, end_date and key value pairs for each dimension and their values.
   * See the DatapointsByMetric type for details.
   */
  get datapointsByMetric(): DatapointsByMetric {
    return this.rawDatapoints.reduce((res: DatapointsByMetric, dp) => {
      if (!res[dp.metric_definition_key]) {
        res[dp.metric_definition_key] = {
          aggregate: [],
          disaggregations: {},
        };
      }

      const sanitizedValue =
        dp.value !== null && isPositiveNumber(dp.value)
          ? Number(dp.value)
          : null;

      if (
        dp.disaggregation_display_name === null ||
        dp.dimension_display_name === null
      ) {
        res[dp.metric_definition_key].aggregate.push({
          [DataVizAggregateName]: sanitizedValue,
          start_date: dp.start_date,
          end_date: dp.end_date,
          frequency: dp.frequency,
          dataVizMissingData: 0,
        });
      } else {
        if (
          !res[dp.metric_definition_key].disaggregations[
            dp.disaggregation_display_name
          ]
        ) {
          res[dp.metric_definition_key].disaggregations[
            dp.disaggregation_display_name
          ] = {};
        }
        res[dp.metric_definition_key].disaggregations[
          dp.disaggregation_display_name
        ][dp.start_date] = {
          ...res[dp.metric_definition_key].disaggregations[
            dp.disaggregation_display_name
          ][dp.start_date],
          start_date: dp.start_date,
          end_date: dp.end_date,
          [dp.dimension_display_name]: sanitizedValue,
          frequency: dp.frequency,
          dataVizMissingData: 0,
        };
      }
      return res;
    }, {});
  }

  async getDatapoints(): Promise<void | Error> {
    try {
      const { currentAgency } = this.userStore;
      if (currentAgency === undefined) {
        // If user is not attached to an agency,
        // no need to bother trying to load this data.
        runInAction(() => {
          this.loading = false;
        });
        return;
      }
      const response = (await this.api.request({
        path: `/api/agencies/${currentAgency.id}/datapoints`,
        method: "GET",
      })) as Response;
      if (response.status === 200) {
        const result = await response.json();
        runInAction(() => {
          this.rawDatapoints = result.datapoints;
          this.dimensionNamesByMetricAndDisaggregation =
            result.dimension_names_by_metric_and_disaggregation;
        });
      } else {
        const error = await response.json();
        throw new Error(error.description);
      }
    } catch (error) {
      runInAction(() => {
        this.loading = false;
      });
      if (error instanceof Error) return new Error(error.message);
    }
  }

  resetState() {
    // reset the state
    runInAction(() => {
      this.rawDatapoints = [];
      this.loading = true;
    });
  }
}

export default DatapointsStore;
