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

import { makeAutoObservable } from "mobx";

import { FormContexts, FormDisaggregations, Metric } from "../shared/types";
import { combineTwoKeyNames } from "../utils";
import ReportStore from "./ReportStore";

class FormStore {
  reportStore: ReportStore;

  metricsValues: { [metricID: string]: number | string };

  contexts: { [metricID: string]: FormContexts };

  disaggregations: { [metricID: string]: FormDisaggregations };

  formErrors: { [metricID: string]: { [fieldKey: string]: string } };

  constructor(reportStore: ReportStore) {
    makeAutoObservable(this);

    this.reportStore = reportStore;
    this.metricsValues = {};
    this.contexts = {};
    this.disaggregations = {};
    this.formErrors = {};
  }

  fullMetricsFromFormValues(reportID: number): Metric[] {
    const updatedMetrics = this.reportStore.reportMetrics[reportID]?.map(
      (metric) => {
        if (
          this.metricsValues[metric.key] ||
          this.contexts[metric.key] ||
          this.disaggregations[metric.key]
        ) {
          return {
            ...metric,
            value: this.metricsValues?.[metric.key],
            contexts: metric.contexts.map((context) => {
              return {
                ...context,
                value: this.contexts?.[metric.key]?.[context.key],
              };
            }),
            disaggregations: metric.disaggregations.map((disaggregation) => {
              return {
                ...disaggregation,
                dimensions: disaggregation.dimensions?.map((dimension) => {
                  return {
                    ...dimension,
                    value:
                      this.disaggregations?.[metric.key]?.[
                        disaggregation.key
                      ]?.[dimension.key],
                  };
                }),
              };
            }),
          };
        }

        return metric;
      }
    );

    return updatedMetrics || [];
  }

  /** Simple validation to flesh out as we solidify necessary validations */
  validate = (
    e: React.ChangeEvent<HTMLInputElement>,
    metricID: string,
    key?: string
  ) => {
    const fieldKey = key || e.target.name;
    const numbersOnlyRegex = /^[0-9]*$/g;
    const isInputNumber =
      typeof e.target.value === "number" ||
      e.target.value.match(numbersOnlyRegex);

    if (!this.formErrors[metricID]) {
      this.formErrors[metricID] = {};
    }

    /** Raise error if value is not a number OR the field is required and the input is empty */
    if (
      !isInputNumber ||
      (e.target.hasAttribute("required") && !e.target.value)
    ) {
      this.formErrors[metricID][fieldKey] = "Error";
    }

    /** Remove error if value is a number AND (required input and input is not empty OR optional input and input is empty) */
    if (
      isInputNumber &&
      ((e.target.hasAttribute("required") && e.target.value) ||
        !e.target.hasAttribute("required"))
    ) {
      delete this.formErrors[metricID][fieldKey];
    }
  };

  /** Form Handlers */
  updateMetricsValues = (
    metricID: string,
    e: React.ChangeEvent<HTMLInputElement>
  ): void => {
    this.validate(e, metricID);
    this.metricsValues[metricID] = e.target.value;
  };

  updateDisaggregationDimensionValue = (
    metricID: string,
    disaggregationKey: string,
    e: React.ChangeEvent<HTMLInputElement>
  ): void => {
    const disaggregationDimensionKey = combineTwoKeyNames(
      disaggregationKey,
      e.target.name
    );
    this.validate(e, metricID, disaggregationDimensionKey);

    if (!this.disaggregations[metricID]) {
      this.disaggregations[metricID] = {
        [disaggregationKey]: {},
      };
    }
    this.disaggregations[metricID][disaggregationKey][e.target.name] =
      e.target.value;
  };

  updateContextValue = (
    metricID: string,
    e: React.ChangeEvent<HTMLInputElement>
  ): void => {
    if (!this.contexts[metricID]) {
      this.contexts[metricID] = {};
    }
    this.contexts[metricID][e.target.name] = e.target.value;
  };

  resetBinaryInput = (
    metricID: string,
    e: React.MouseEvent<HTMLDivElement>
  ): void => {
    const fieldKey = e.currentTarget.dataset.name as string;
    this.contexts[metricID][fieldKey] = "";
  };

  submitReport = (reportID: number) => {
    /** Submit Report Logic Goes Here */
    /** Final Validation Logic Goes Here (users should not be able to publish an empty/invalid report) */

    /** Temporarily Returning Final Object For Testing Purposes */
    return {
      ...this.reportStore.reportOverviews[reportID],
      metrics: this.fullMetricsFromFormValues(reportID),
    };
  };
}

export default FormStore;
