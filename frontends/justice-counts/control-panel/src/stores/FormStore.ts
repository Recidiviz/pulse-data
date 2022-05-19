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

import {
  FormStoreContextValues,
  FormStoreDisaggregationValues,
  FormStoreErrors,
  FormStoreMetricValues,
  Metric,
  UpdatedMetricsValues,
} from "../shared/types";
import {
  combineTwoKeyNames,
  removeCommaSpaceAndTrim,
  sanitizeInputValue,
} from "../utils";
import ReportStore from "./ReportStore";

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
        const metricValue = this.metricsValues[reportID]?.[metric.key];
        const contexts = this.contexts[reportID]?.[metric.key];
        const disaggregationForMetric =
          this.disaggregations[reportID]?.[metric.key];

        if (metricValue || contexts || disaggregationForMetric) {
          return {
            ...metric,
            value: sanitizeInputValue(metricValue, metric.value),
            contexts: metric.contexts.map((context) => {
              return {
                ...context,
                value: sanitizeInputValue(
                  contexts?.[context.key],
                  context.value
                ),
              };
            }),
            disaggregations: metric.disaggregations.map((disaggregation) => {
              return {
                ...disaggregation,
                dimensions: disaggregation.dimensions?.map((dimension) => {
                  return {
                    ...dimension,
                    value: sanitizeInputValue(
                      disaggregationForMetric?.[disaggregation.key]?.[
                        dimension.key
                      ],
                      dimension.value
                    ),
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

  /**
   * Maps updated values into data structure required by the backend.
   * Backend requires a combination of updated values on updated fields,
   * and default values (the ones retrieved from the backend on load) for
   * fields that have not been updated.
   *
   * @returns updated array of metrics (in the required data structure)
   */

  reportUpdatedValuesForBackend(reportID: number): UpdatedMetricsValues[] {
    const updatedMetricValues = this.reportStore.reportMetrics[reportID]?.map(
      (metric) => {
        /** Note: all empty inputs will be represented by null */
        const metricValue = sanitizeInputValue(
          this.metricsValues[reportID]?.[metric.key],
          metric.value
        );

        const combinedMetricValues: UpdatedMetricsValues = {
          key: metric.key,
          value: metricValue,
          contexts: [],
          disaggregations: [],
        };

        metric.contexts.forEach((context) => {
          const contextValue = sanitizeInputValue(
            this.contexts[reportID]?.[metric.key]?.[context.key],
            context.value
          );

          combinedMetricValues.contexts.push({
            key: context.key,
            value: contextValue,
          });
        });

        metric.disaggregations.forEach((disaggregation) => {
          combinedMetricValues.disaggregations.push({
            key: disaggregation.key,
            dimensions: disaggregation.dimensions.map((dimension) => {
              const dimensionValue = sanitizeInputValue(
                this.disaggregations[reportID]?.[metric.key]?.[
                  disaggregation.key
                ]?.[dimension.key],
                dimension.value
              );

              return {
                key: dimension.key,
                value: dimensionValue,
              };
            }),
          });
        });

        return combinedMetricValues;
      }
    );

    return updatedMetricValues || [];
  }

  validateNumber = (
    value: string,
    reportID: number,
    metricKey: string,
    key?: string,
    required?: boolean
  ) => {
    const fieldKey = key || metricKey;
    const cleanValue = removeCommaSpaceAndTrim(value);
    const isPositiveNumber =
      (cleanValue !== "" && Number(cleanValue) === 0) || Number(cleanValue) > 0;
    const isRequiredButEmpty = required && cleanValue === "";

    /** Raise error if value is not a positive number OR the field is required and the input is empty */
    if (!isPositiveNumber || isRequiredButEmpty) {
      if (!this.formErrors[reportID]) {
        this.formErrors[reportID] = {};
      }

      if (!this.formErrors[reportID][metricKey]) {
        this.formErrors[reportID][metricKey] = {};
      }

      if (!isPositiveNumber) {
        this.formErrors[reportID][metricKey][fieldKey] =
          "Please enter a valid number.";
      }

      if (isRequiredButEmpty) {
        this.formErrors[reportID][metricKey][fieldKey] =
          "This is a required field. Please enter a valid number.";
      }
    }

    if (this.formErrors[reportID]?.[metricKey]?.[fieldKey]) {
      /** Remove error if (value is a positive number AND required input and input is not empty) OR optional input and input is empty) */
      if (
        (isPositiveNumber && required && cleanValue !== "") ||
        (!required && (cleanValue === "" || isPositiveNumber))
      ) {
        delete this.formErrors[reportID][metricKey][fieldKey];
      }
    }
  };

  /** Form Handlers */
  updateMetricsValues = (
    reportID: number,
    metricKey: string,
    updatedValue: string
  ): void => {
    this.validateNumber(updatedValue, reportID, metricKey, undefined, true);

    /**
     * Create an empty object within the property if none exist to improve access
     * speed and to help with isolating re-renders for each form component.
     */
    if (!this.metricsValues[reportID]) {
      this.metricsValues[reportID] = {};
    }

    this.metricsValues[reportID][metricKey] = updatedValue;
  };

  updateDisaggregationDimensionValue = (
    reportID: number,
    metricKey: string,
    disaggregationKey: string,
    dimensionKey: string,
    updatedValue: string,
    required: boolean
  ): void => {
    const disaggregationDimensionKey = combineTwoKeyNames(
      disaggregationKey,
      dimensionKey
    );
    this.validateNumber(
      updatedValue,
      reportID,
      metricKey,
      disaggregationDimensionKey,
      required
    );

    /**
     * Create empty objects within the properties if none exist to improve access
     * speed and to help with isolating re-renders for each form component.
     */
    if (!this.disaggregations[reportID]) {
      this.disaggregations[reportID] = {};
    }

    if (!this.disaggregations[reportID][metricKey]) {
      this.disaggregations[reportID][metricKey] = {};
    }

    if (!this.disaggregations[reportID][metricKey][disaggregationKey]) {
      this.disaggregations[reportID][metricKey][disaggregationKey] = {};
    }

    this.disaggregations[reportID][metricKey][disaggregationKey][dimensionKey] =
      updatedValue;
  };

  updateContextValue = (
    reportID: number,
    metricKey: string,
    contextKey: string,
    updatedValue: string,
    required: boolean,
    contextType?: string
  ): void => {
    if (contextType === "NUMBER") {
      this.validateNumber(
        updatedValue,
        reportID,
        metricKey,
        contextKey,
        required
      );
    }

    /**
     * Create an empty object within the property if none exist to improve access
     * speed and to help with isolating re-renders for each form component.
     */
    if (!this.contexts[reportID]) {
      this.contexts[reportID] = {};
    }

    if (!this.contexts[reportID][metricKey]) {
      this.contexts[reportID][metricKey] = {};
    }

    this.contexts[reportID][metricKey][contextKey] = updatedValue;
  };

  resetBinaryInput = (
    reportID: number,
    metricKey: string,
    contextKey: string
  ): void => {
    this.contexts[reportID][metricKey][contextKey] = "";
  };
}

export default FormStore;
