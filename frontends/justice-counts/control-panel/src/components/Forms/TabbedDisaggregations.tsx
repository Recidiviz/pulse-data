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

import React, { useEffect, useState } from "react";

import { Metric as MetricType } from "../../shared/types";
import { useStore } from "../../stores";
import successIcon from "../assets/status-check-icon.png";
import errorIcon from "../assets/status-error-icon.png";
import { DisaggregationDimensionTextInput } from "../Reports/DataEntryFormComponents";
import {
  DisaggregationHasInputIndicator,
  DisaggregationInputWrapper,
  DisaggregationTabsContainer,
  TabDisplay,
  TabItem,
  TabsRow,
} from ".";

export const TabbedDisaggregations: React.FC<{
  metric: MetricType;
  reportMetrics: MetricType[];
  reportID: number;
  currentIndex: number;
  updateFieldDescription: (title?: string, description?: string) => void;
}> = ({
  metric,
  reportMetrics,
  reportID,
  currentIndex,
  updateFieldDescription,
}) => {
  const [activeDisaggregation, setActiveDisaggregation] = useState<{
    [metricKey: string]: {
      disaggregationKey: string;
      disaggregationIndex: number;
      hasValue?: boolean;
    };
  }>({});
  const [disaggregationHasInput, setDisaggregationHasInput] = useState<{
    [disaggregationKey: string]: {
      hasInput?: boolean;
    };
  }>({});
  const { formStore } = useStore();

  useEffect(
    () => {
      metric.disaggregations.forEach((disaggregation, index) => {
        searchDimensionsForInput(disaggregation.key, index);
      });
    },
    // eslint-disable-next-line react-hooks/exhaustive-deps
    []
  );

  const checkMetricForErrorsInUpdatedValues = (
    metricKey: string,
    disaggregationKey: string
  ) => {
    let foundErrors = false;

    if (
      formStore.disaggregations?.[reportID]?.[metricKey]?.[disaggregationKey]
    ) {
      Object.values(
        formStore.disaggregations?.[reportID]?.[metricKey]?.[disaggregationKey]
      ).forEach((dimension) => {
        if (dimension.error) {
          foundErrors = true;
        }
      });
    }

    return foundErrors;
  };

  const searchDimensionsForInput = (
    disaggregationKey: string,
    disaggregationIndex: number
  ) => {
    let inputFoundInUpdate = false;
    let inputFoundFromLastSave = false;

    reportMetrics[currentIndex]?.disaggregations[
      disaggregationIndex
    ]?.dimensions?.forEach((dimension) => {
      const updatedDimensionValue =
        formStore.disaggregations[reportID]?.[metric.key]?.[
          disaggregationKey
        ]?.[dimension.key]?.value;

      if (
        dimension.value &&
        !inputFoundFromLastSave &&
        updatedDimensionValue !== ""
      )
        inputFoundFromLastSave = true;
    });

    if (
      formStore.disaggregations[reportID]?.[metric.key]?.[disaggregationKey]
    ) {
      Object.values(
        formStore.disaggregations[reportID]?.[metric.key]?.[disaggregationKey]
      ).forEach((dimension) => {
        if (dimension.value && !inputFoundInUpdate) inputFoundInUpdate = true;
      });
    }

    if (inputFoundInUpdate || inputFoundFromLastSave) {
      setDisaggregationHasInput((prev) => {
        return {
          ...prev,
          [disaggregationKey]: { hasInput: true },
        };
      });
    } else {
      setDisaggregationHasInput((prev) => {
        return {
          ...prev,
          [disaggregationKey]: { hasInput: false },
        };
      });
    }
  };

  const updateActiveDisaggregationTab = (
    metricKey: string,
    disaggregationKey: string,
    disaggregationIndex: number
  ) => {
    searchDimensionsForInput(disaggregationKey, disaggregationIndex);
    setActiveDisaggregation((prev) => ({
      ...prev,
      [metricKey]: {
        disaggregationKey,
        disaggregationIndex,
      },
    }));
  };

  return (
    <DisaggregationTabsContainer>
      <TabsRow>
        {metric.disaggregations.map((disaggregation, disaggregationIndex) => {
          const foundError = checkMetricForErrorsInUpdatedValues(
            metric.key,
            disaggregation.key
          );
          return (
            <TabItem
              key={disaggregation.key}
              active={
                (!activeDisaggregation[metric.key]?.disaggregationKey &&
                  disaggregationIndex === 0) ||
                activeDisaggregation[metric.key]?.disaggregationKey ===
                  disaggregation.key
              }
              onClick={() =>
                updateActiveDisaggregationTab(
                  metric.key,
                  disaggregation.key,
                  disaggregationIndex
                )
              }
            >
              {disaggregation.display_name}
              <DisaggregationHasInputIndicator
                active={
                  (!activeDisaggregation[metric.key]?.disaggregationKey &&
                    disaggregationIndex === 0) ||
                  activeDisaggregation[metric.key]?.disaggregationKey ===
                    disaggregation.key
                }
                error={foundError}
                hasInput={disaggregationHasInput[disaggregation.key]?.hasInput}
              >
                {/* Error State Icon */}
                {foundError && (
                  <img src={errorIcon} alt="" width="16px" height="16px" />
                )}

                {/* Success State Icon */}
                {disaggregationHasInput[disaggregation.key]?.hasInput &&
                  !foundError && (
                    <img src={successIcon} alt="" width="16px" height="16px" />
                  )}
              </DisaggregationHasInputIndicator>
            </TabItem>
          );
        })}
      </TabsRow>

      <TabDisplay>
        {reportMetrics[currentIndex].disaggregations[
          activeDisaggregation[metric.key]?.disaggregationIndex || 0
        ]?.dimensions.map((dimension, dimensionIndex) => {
          return (
            <DisaggregationInputWrapper
              key={dimension.key}
              onChange={() =>
                searchDimensionsForInput(
                  metric.disaggregations[
                    activeDisaggregation[metric.key]?.disaggregationIndex || 0
                  ].key,
                  activeDisaggregation[metric.key]?.disaggregationIndex || 0
                )
              }
            >
              <DisaggregationDimensionTextInput
                reportID={reportID}
                key={dimension.key + dimension.reporting_note}
                metric={metric}
                dimension={dimension}
                disaggregation={
                  metric.disaggregations[
                    activeDisaggregation[metric.key]?.disaggregationIndex || 0
                  ]
                }
                disaggregationIndex={
                  activeDisaggregation[metric.key]?.disaggregationIndex || 0
                }
                dimensionIndex={dimensionIndex}
                updateFieldDescription={() =>
                  updateFieldDescription(
                    dimension.label,
                    dimension.reporting_note
                  )
                }
                clearFieldDescription={() => updateFieldDescription(undefined)}
              />
            </DisaggregationInputWrapper>
          );
        })}
      </TabDisplay>
    </DisaggregationTabsContainer>
  );
};
