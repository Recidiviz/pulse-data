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
import { BinaryRadioButton, TextInput } from "../Forms";

interface MetricTextInputProps {
  metric: Metric;
}

export const MetricTextInput = observer(({ metric }: MetricTextInputProps) => {
  const { formStore } = useStore();
  const { metricsValues, updateMetricsValues } = formStore;

  return (
    <TextInput
      label={metric.label}
      error=""
      type="text"
      name={metric.key}
      valueLabel={metric.unit}
      context={metric.reporting_note}
      onChange={(e) => updateMetricsValues(metric.key, e)}
      value={
        metricsValues[metric.key] !== undefined
          ? metricsValues[metric.key]
          : (metric.value as string)
      }
      required
    />
  );
});

interface DisaggregationDimensionTextInputProps extends MetricTextInputProps {
  dimension: MetricDisaggregationDimensions;
  disaggregation: MetricDisaggregations;
  disaggregationIndex: number;
  dimensionIndex: number;
}
export const DisaggregationDimensionTextInput = observer(
  ({
    metric,
    dimension,
    disaggregation,
    disaggregationIndex,
    dimensionIndex,
  }: DisaggregationDimensionTextInputProps) => {
    const { formStore } = useStore();
    const { disaggregations, updateDisaggregationDimensionValue } = formStore;

    return (
      <TextInput
        key={dimension.key}
        label={dimension.label}
        error=""
        type="text"
        name={dimension.key}
        valueLabel={metric.unit}
        context={dimension.reporting_note}
        onChange={(e) =>
          updateDisaggregationDimensionValue(metric.key, disaggregation.key, e)
        }
        value={
          disaggregations?.[metric.key]?.[disaggregation.key]?.[
            dimension.key
          ] !== undefined
            ? disaggregations[metric.key][disaggregation.key][dimension.key]
            : (metric.disaggregations?.[disaggregationIndex]?.dimensions?.[
                dimensionIndex
              ].value as string) || ""
        }
        required={disaggregation.required}
      />
    );
  }
);

interface AdditionalContextInputsProps extends MetricTextInputProps {
  context: MetricContext;
  contextIndex: number;
}

export const BinaryRadioButtonInputs = observer(
  ({ metric, context }: AdditionalContextInputsProps) => {
    const { formStore } = useStore();
    const { contexts, updateContextValue } = formStore;

    return (
      <>
        <BinaryRadioButton
          type="radio"
          id={`${context.key}-yes`}
          name={context.key}
          label="Yes"
          value="Yes"
          onChange={(e) => updateContextValue(metric.key, e)}
          checked={contexts?.[metric.key]?.[context.key] === "Yes"}
        />
        <BinaryRadioButton
          type="radio"
          id={`${context.key}-no`}
          name={context.key}
          label="No"
          value="No"
          onChange={(e) => updateContextValue(metric.key, e)}
          checked={contexts?.[metric.key]?.[context.key] === "No"}
        />
      </>
    );
  }
);

export const AdditionalContextInput = observer(
  ({ metric, context, contextIndex }: AdditionalContextInputsProps) => {
    const { formStore } = useStore();
    const { contexts, updateContextValue } = formStore;

    return (
      <TextInput
        error=""
        type="text"
        name={context.key}
        label="Type here..."
        context={context.reporting_note || ""}
        onChange={(e) => updateContextValue(metric.key, e)}
        value={
          contexts?.[metric.key]?.[context.key] !== undefined
            ? contexts[metric.key][context.key]
            : (metric.contexts[contextIndex].value as string) || ""
        }
        additionalContext
      />
    );
  }
);
