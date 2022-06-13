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
  FormStoreMetricValues,
  Metric,
  UpdatedMetricsValues,
} from "../shared/types";
import {
  normalizeToString,
  removeCommaSpaceAndTrim,
  sanitizeInputValue,
} from "../utils";
import ReportStore from "./ReportStore";

class FormStore {
  reportStore: ReportStore;

  metricsValues: FormStoreMetricValues;

  contexts: FormStoreContextValues;

  disaggregations: FormStoreDisaggregationValues;

  constructor(reportStore: ReportStore) {
    makeAutoObservable(this);

    this.reportStore = reportStore;
    this.metricsValues = {};
    this.contexts = {};
    this.disaggregations = {};
  }

  validatePreviouslySavedInputs(reportID: number) {
    /** Runs validation of previously saved inputs on load */
    this.reportStore.reportMetrics[reportID].forEach((metric) => {
      if (metric.value !== null && metric.value !== undefined) {
        this.updateMetricsValues(
          reportID,
          metric.key,
          normalizeToString(metric.value)
        );
      }

      metric.disaggregations.forEach((disaggregation) => {
        disaggregation.dimensions.forEach((dimension) => {
          if (dimension.value !== null && dimension.value !== undefined) {
            this.updateDisaggregationDimensionValue(
              reportID,
              metric.key,
              disaggregation.key,
              dimension.key,
              normalizeToString(dimension.value),
              disaggregation.required
            );
          }
        });
      });

      metric.contexts.forEach((context) => {
        if (context.value !== null && context.value !== undefined) {
          this.updateContextValue(
            reportID,
            metric.key,
            context.key,
            normalizeToString(context.value),
            context.required,
            context.type
          );
        }
      });
    });
  }

  validateAndGetAllMetricFormValues(reportID: number): {
    metrics: Metric[];
    isPublishable: boolean;
  } {
    /**
     * Converts value to normalized string
     * @returns "" empty strings for falsy values or value converted to string type if truthy
     */
    let isPublishable = true;

    const updatedMetrics = this.reportStore.reportMetrics[reportID]?.map(
      (metric) => {
        const metricValues = this.metricsValues[reportID]?.[metric.key];
        const contexts = this.contexts[reportID]?.[metric.key];
        const disaggregationForMetric =
          this.disaggregations[reportID]?.[metric.key];

        /** Touch & validate metric field */
        this.updateMetricsValues(
          reportID,
          metric.key,
          normalizeToString(metricValues?.value) ||
            normalizeToString(metric.value)
        );

        const metricError = this.metricsValues[reportID]?.[metric.key]?.error;
        if (metricError) {
          isPublishable = false;
        }

        return {
          ...metric,
          value: sanitizeInputValue(metricValues?.value, metric.value),
          error: metricError,
          contexts: metric.contexts.map((context) => {
            /** Touch & validate context field */
            this.updateContextValue(
              reportID,
              metric.key,
              context.key,
              normalizeToString(contexts?.[context.key]?.value) ||
                normalizeToString(context.value),
              context.required,
              context.type
            );

            const contextError =
              this.contexts[reportID]?.[metric.key]?.[context.key]?.error;
            if (contextError) {
              isPublishable = false;
            }

            return {
              ...context,
              value: sanitizeInputValue(
                contexts?.[context.key]?.value,
                context.value,
                context.type
              ),
              error: contextError,
            };
          }),
          disaggregations: metric.disaggregations.map((disaggregation) => {
            return {
              ...disaggregation,
              dimensions: disaggregation.dimensions?.map((dimension) => {
                /** Touch & validate disaggregation dimension field */
                this.updateDisaggregationDimensionValue(
                  reportID,
                  metric.key,
                  disaggregation.key,
                  dimension.key,
                  normalizeToString(
                    disaggregationForMetric?.[disaggregation.key]?.[
                      dimension.key
                    ]?.value
                  ) || normalizeToString(dimension.value),
                  disaggregation.required
                );

                const disaggregationError =
                  this.disaggregations[reportID]?.[metric.key]?.[
                    disaggregation.key
                  ]?.[dimension.key]?.error;
                if (disaggregationError) {
                  isPublishable = false;
                }

                return {
                  ...dimension,
                  value: sanitizeInputValue(
                    disaggregationForMetric?.[disaggregation.key]?.[
                      dimension.key
                    ]?.value,
                    dimension.value
                  ),
                  error: disaggregationError,
                };
              }),
            };
          }),
        };
      }
    );

    return { metrics: updatedMetrics || [], isPublishable };
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
          this.metricsValues[reportID]?.[metric.key]?.value,
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
            this.contexts[reportID]?.[metric.key]?.[context.key]?.value,
            context.value,
            context.type
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
                ]?.[dimension.key]?.value,
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

  validate = (
    validationType: string,
    value: string,
    required: boolean,
    reportID: number,
    metricKey: string,
    key1?: string,
    key2?: string
  ) => {
    const cleanValue = removeCommaSpaceAndTrim(value);
    const isPositiveNumber =
      (cleanValue !== "" && Number(cleanValue) === 0) || Number(cleanValue) > 0;
    const isRequiredButEmpty = required && cleanValue === "";

    /** Need to find a cleaner & clearer way to do this */
    const updateFieldErrorMessage = (
      operation: "ADD" | "DELETE",
      message?: string
    ) => {
      /**
       * Overall metric: !key1 && !key2
       * Context: key1 && !key2
       * Disaggregation Dimension: key1 && key2
       */
      if (operation === "ADD") {
        if (key1 && key2) {
          this.disaggregations[reportID][metricKey][key1][key2].error = message;
        } else if (key1 && !key2) {
          this.contexts[reportID][metricKey][key1].error = message;
        } else {
          this.metricsValues[reportID][metricKey].error = message;
        }
      }
      if (operation === "DELETE") {
        if (key1 && key2) {
          delete this.disaggregations[reportID][metricKey][key1][key2].error;
        } else if (key1 && !key2) {
          delete this.contexts[reportID][metricKey][key1].error;
        } else {
          delete this.metricsValues[reportID][metricKey].error;
        }
      }
    };

    /** Raise Error */
    if (isRequiredButEmpty) {
      updateFieldErrorMessage("ADD", "This is a required field.");
      return;
    }

    if (!required && cleanValue === "") {
      /** Remove Error */
      updateFieldErrorMessage("DELETE");
      return;
    }

    if (validationType === "NUMBER") {
      /** Raise Error */
      if (!isPositiveNumber) {
        updateFieldErrorMessage("ADD", "Please enter a valid number.");
        return;
      }
    }

    /** Remove Error */
    updateFieldErrorMessage("DELETE");
  };

  updateMetricsValues = (
    reportID: number,
    metricKey: string,
    updatedValue: string
  ): void => {
    /**
     * Create an empty object within the property if none exist to improve access
     * speed and to help with isolating re-renders for each form component.
     */
    if (!this.metricsValues[reportID]) {
      this.metricsValues[reportID] = {};
    }
    if (!this.metricsValues[reportID][metricKey]) {
      this.metricsValues[reportID][metricKey] = {};
    }

    this.validate("NUMBER", updatedValue, true, reportID, metricKey);
    this.metricsValues[reportID][metricKey].value = updatedValue;
  };

  updateDisaggregationDimensionValue = (
    reportID: number,
    metricKey: string,
    disaggregationKey: string,
    dimensionKey: string,
    updatedValue: string,
    required: boolean
  ): void => {
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

    if (
      !this.disaggregations[reportID][metricKey][disaggregationKey][
        dimensionKey
      ]
    ) {
      this.disaggregations[reportID][metricKey][disaggregationKey][
        dimensionKey
      ] = {};
    }

    this.validate(
      "NUMBER",
      updatedValue,
      required,
      reportID,
      metricKey,
      disaggregationKey,
      dimensionKey
    );

    this.disaggregations[reportID][metricKey][disaggregationKey][
      dimensionKey
    ].value = updatedValue;
  };

  updateContextValue = (
    reportID: number,
    metricKey: string,
    contextKey: string,
    updatedValue: string,
    required: boolean,
    contextType: string
  ): void => {
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

    if (!this.contexts[reportID][metricKey][contextKey]) {
      this.contexts[reportID][metricKey][contextKey] = {};
    }

    this.validate(
      contextType,
      updatedValue,
      required,
      reportID,
      metricKey,
      contextKey
    );

    this.contexts[reportID][metricKey][contextKey].value = updatedValue;
  };

  resetBinaryInput = (
    reportID: number,
    metricKey: string,
    contextKey: string,
    required: boolean
  ): void => {
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

    if (!this.contexts[reportID][metricKey][contextKey]) {
      this.contexts[reportID][metricKey][contextKey] = {};
    }

    this.validate("TEXT", "", required, reportID, metricKey, contextKey);
    this.contexts[reportID][metricKey][contextKey].value = "";
  };
}

export default FormStore;
