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

interface FormStoreMetricValue {
  [metricKey: string]: number | string;
}
interface FormStoreMetricValues {
  [reportID: string]: FormStoreMetricValue;
}

interface FormStoreContextValue {
  [metricKey: string]: FormContexts;
}
interface FormStoreContextValues {
  [reportID: string]: FormStoreContextValue;
}

interface FormStoreDisaggregationValue {
  [metricKey: string]: FormDisaggregations;
}
interface FormStoreDisaggregationValues {
  [reportID: string]: FormStoreDisaggregationValue;
}

interface FormStoreError {
  [metricKey: string]: { [fieldKey: string]: string };
}
interface FormStoreErrors {
  [reportID: string]: FormStoreError;
}
class FormStore {
  reportStore: ReportStore;

  metricsValues: FormStoreMetricValues;

  contexts: FormStoreContextValues;

  disaggregations: FormStoreDisaggregationValues;

  formErrors: FormStoreErrors;

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
          this.metricsValues[reportID]?.[metric.key] ||
          this.contexts[reportID]?.[metric.key] ||
          this.disaggregations[reportID]?.[metric.key]
        ) {
          return {
            ...metric,
            value: this.metricsValues?.[reportID]?.[metric.key],
            contexts: metric.contexts.map((context) => {
              return {
                ...context,
                value: this.contexts?.[reportID]?.[metric.key]?.[context.key],
              };
            }),
            disaggregations: metric.disaggregations.map((disaggregation) => {
              return {
                ...disaggregation,
                dimensions: disaggregation.dimensions?.map((dimension) => {
                  return {
                    ...dimension,
                    value:
                      this.disaggregations?.[reportID]?.[metric.key]?.[
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
    reportID: number,
    metricKey: string,
    key?: string
  ) => {
    const fieldKey = key || e.target.name;
    const numbersOnlyRegex = /^[0-9]*$/g;
    const isInputNumber =
      typeof e.target.value === "number" ||
      e.target.value.match(numbersOnlyRegex);

    if (!this.formErrors[reportID]) {
      this.formErrors[reportID] = {};
    }

    if (!this.formErrors[reportID][metricKey]) {
      this.formErrors[reportID][metricKey] = {};
    }

    /** Raise error if value is not a number OR the field is required and the input is empty */
    if (
      !isInputNumber ||
      (e.target.hasAttribute("required") && !e.target.value)
    ) {
      this.formErrors[reportID][metricKey][fieldKey] = "Error";
    }

    /** Remove error if value is a number AND (required input and input is not empty OR optional input and input is empty) */
    if (
      isInputNumber &&
      ((e.target.hasAttribute("required") && e.target.value) ||
        !e.target.hasAttribute("required"))
    ) {
      delete this.formErrors[reportID][metricKey][fieldKey];
    }
  };

  /** Form Handlers */
  updateMetricsValues = (
    reportID: number,
    metricKey: string,
    e: React.ChangeEvent<HTMLInputElement>
  ): void => {
    this.validate(e, reportID, metricKey);

    if (!this.metricsValues[reportID]) {
      this.metricsValues[reportID] = {};
    }

    this.metricsValues[reportID][metricKey] = e.target.value;
  };

  updateDisaggregationDimensionValue = (
    reportID: number,
    metricKey: string,
    disaggregationKey: string,
    e: React.ChangeEvent<HTMLInputElement>
  ): void => {
    const disaggregationDimensionKey = combineTwoKeyNames(
      disaggregationKey,
      e.target.name
    );
    this.validate(e, reportID, metricKey, disaggregationDimensionKey);

    if (!this.disaggregations[reportID]) {
      this.disaggregations[reportID] = {};
    }

    if (!this.disaggregations[reportID][metricKey]) {
      this.disaggregations[reportID][metricKey] = {
        [disaggregationKey]: {},
      };
    }
    this.disaggregations[reportID][metricKey][disaggregationKey][
      e.target.name
    ] = e.target.value;
  };

  updateContextValue = (
    reportID: number,
    metricKey: string,
    e: React.ChangeEvent<HTMLInputElement>
  ): void => {
    if (!this.contexts[reportID]) {
      this.contexts[reportID] = {};
    }

    if (!this.contexts[reportID][metricKey]) {
      this.contexts[reportID][metricKey] = {};
    }

    this.contexts[reportID][metricKey][e.target.name] = e.target.value;
  };

  resetBinaryInput = (
    reportID: number,
    metricKey: string,
    e: React.MouseEvent<HTMLDivElement>
  ): void => {
    const fieldKey = e.currentTarget.dataset.name as string;
    this.contexts[reportID][metricKey][fieldKey] = "";
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
