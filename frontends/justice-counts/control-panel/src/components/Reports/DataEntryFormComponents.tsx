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

import { observer } from "mobx-react-lite";
import React from "react";

import {
  Metric,
  MetricContext,
  MetricDisaggregationDimensions,
  MetricDisaggregations,
} from "../../shared/types";
import { useStore } from "../../stores";
import { formatNumberInput } from "../../utils";
import { BinaryRadioButton, TextInput } from "../Forms";

interface MetricTextInputProps {
  reportID: number;
  metric: Metric;
  autoFocus?: boolean;
  updateFieldDescription?: () => void;
  clearFieldDescription?: () => void;
}

export const MetricTextInput = observer(
  ({
    reportID,
    metric,
    autoFocus,
    updateFieldDescription,
    clearFieldDescription,
  }: MetricTextInputProps) => {
    const { formStore } = useStore();
    const { metricsValues, updateMetricsValues } = formStore;

    const handleMetricChange = (e: React.ChangeEvent<HTMLInputElement>) =>
      updateMetricsValues(reportID, metric.key, e.target.value);

    return (
      <TextInput
        label={metric.label}
        error={metricsValues[reportID]?.[metric.key]?.error}
        type="text"
        name={metric.key}
        valueLabel={metric.unit}
        onChange={handleMetricChange}
        value={
          metricsValues[reportID]?.[metric.key]?.value !== undefined
            ? formatNumberInput(metricsValues[reportID][metric.key].value)
            : formatNumberInput(metric.value?.toString()) || ""
        }
        persistLabel
        placeholder="Enter value"
        required
        autoFocus={autoFocus}
        onFocus={updateFieldDescription}
        onBlur={clearFieldDescription}
      />
    );
  }
);

interface DisaggregationDimensionTextInputProps extends MetricTextInputProps {
  disaggregation: MetricDisaggregations;
  disaggregationIndex: number;
  dimension: MetricDisaggregationDimensions;
  dimensionIndex: number;
}

export const DisaggregationDimensionTextInput = observer(
  ({
    reportID,
    metric,
    dimension,
    disaggregation,
    disaggregationIndex,
    dimensionIndex,
    updateFieldDescription,
    clearFieldDescription,
  }: DisaggregationDimensionTextInputProps) => {
    const { formStore } = useStore();
    const { disaggregations, updateDisaggregationDimensionValue } = formStore;

    const handleDisaggregationDimensionChange = (
      e: React.ChangeEvent<HTMLInputElement>
    ) =>
      updateDisaggregationDimensionValue(
        reportID,
        metric.key,
        disaggregation.key,
        dimension.key,
        e.target.value,
        disaggregation.required
      );

    return (
      <TextInput
        key={dimension.key}
        label={dimension.label}
        error={
          disaggregations?.[reportID]?.[metric.key]?.[disaggregation.key]?.[
            dimension.key
          ]?.error
        }
        type="text"
        name={dimension.key}
        id={dimension.key}
        valueLabel={metric.unit}
        onChange={handleDisaggregationDimensionChange}
        value={
          disaggregations?.[reportID]?.[metric.key]?.[disaggregation.key]?.[
            dimension.key
          ]?.value !== undefined
            ? formatNumberInput(
                disaggregations[reportID][metric.key][disaggregation.key][
                  dimension.key
                ].value
              )
            : formatNumberInput(
                metric.disaggregations?.[disaggregationIndex]?.dimensions?.[
                  dimensionIndex
                ].value?.toString()
              ) || ""
        }
        persistLabel
        placeholder="Enter value"
        required={disaggregation.required}
        onFocus={updateFieldDescription}
        onBlur={clearFieldDescription}
      />
    );
  }
);

interface AdditionalContextInputsProps extends MetricTextInputProps {
  context: MetricContext;
  contextIndex: number;
}

interface BinaryContextProps extends AdditionalContextInputsProps {
  options: string[];
}

export const BinaryRadioButtonInputs = observer(
  ({
    reportID,
    metric,
    context,
    contextIndex,
    options,
  }: BinaryContextProps) => {
    const { formStore } = useStore();
    const { contexts, updateContextValue } = formStore;

    const handleContextChange = (e: React.ChangeEvent<HTMLInputElement>) =>
      updateContextValue(
        reportID,
        metric.key,
        context.key,
        e.target.value,
        context.required,
        context.type
      );

    return (
      <>
        {options.map((option: string) => (
          <BinaryRadioButton
            type="radio"
            key={option}
            id={`${context.key}-${option}`}
            name={context.key}
            label={option}
            value={option}
            onChange={handleContextChange}
            checked={
              contexts?.[reportID]?.[metric.key]?.[context.key]
                ? contexts[reportID][metric.key][context.key].value === option
                : metric.contexts[contextIndex].value === option
            }
          />
        ))}
      </>
    );
  }
);

export const AdditionalContextInput = observer(
  ({
    reportID,
    metric,
    context,
    contextIndex,
    updateFieldDescription,
    clearFieldDescription,
  }: AdditionalContextInputsProps) => {
    const { formStore } = useStore();
    const { contexts, updateContextValue } = formStore;
    const getContextValue = () => {
      if (
        contexts?.[reportID]?.[metric.key]?.[context.key]?.value !== undefined
      ) {
        return context.type === "NUMBER"
          ? formatNumberInput(
              contexts[reportID]?.[metric.key][context.key].value
            )
          : contexts[reportID]?.[metric.key][context.key].value;
      }

      return metric.contexts[contextIndex].value?.toString() || "";
    };
    const contextValue = getContextValue();

    const handleContextChange = (
      e: React.ChangeEvent<HTMLTextAreaElement | HTMLInputElement>
    ) =>
      updateContextValue(
        reportID,
        metric.key,
        context.key,
        e.target.value,
        context.required,
        context.type
      );

    return (
      <TextInput
        type="text"
        name={context.key}
        id={context.key}
        label="Type here..."
        onChange={handleContextChange}
        value={contextValue}
        multiline={context.type === "TEXT"}
        error={contexts?.[reportID]?.[metric.key]?.[context.key]?.error}
        required={context.required}
        onFocus={updateFieldDescription}
        onBlur={clearFieldDescription}
      />
    );
  }
);
