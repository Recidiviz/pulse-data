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
import React, { Fragment, useEffect, useState } from "react";

import { Metric as MetricType } from "../../shared/types";
import { useStore } from "../../stores";
import { printReportTitle } from "../../utils";
import {
  AdditionalContextLabel,
  BinaryRadioGroupClearButton,
  BinaryRadioGroupContainer,
  BinaryRadioGroupQuestion,
  BinaryRadioGroupWrapper,
  DisaggregationHasInputIndicator,
  DisaggregationInputWrapper,
  DisaggregationTabsContainer,
  Form,
  Metric,
  MetricSectionSubTitle,
  MetricSectionTitle,
  OpacityGradient,
  PreTitle,
  TabDisplay,
  TabItem,
  TabsRow,
  Title,
} from "../Forms";
import {
  AdditionalContextInput,
  BinaryRadioButtonInputs,
  DisaggregationDimensionTextInput,
  MetricTextInput,
} from "./DataEntryFormComponents";

/** This definitely needs a lot of clean up, refactor and a ton of optimization - but got the functionality of it working so far */
const DisaggregationDisplay: React.FC<{
  metric: MetricType;
  reportMetrics: MetricType[];
  reportID: number;
  currentIndex: number;
  updateFieldDescription: (title: string, description: string) => void;
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
                inputHasValue={
                  disaggregationHasInput[disaggregation.key]?.hasInput
                }
              />
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
                clearFieldDescription={() => updateFieldDescription("", "")}
              />
            </DisaggregationInputWrapper>
          );
        })}
      </TabDisplay>
    </DisaggregationTabsContainer>
  );
};

const DataEntryForm: React.FC<{
  reportID: number;
  updateFieldDescription: (title: string, description: string) => void;
}> = ({ reportID, updateFieldDescription }) => {
  const [scrolled, setScrolled] = useState(false);
  const { formStore, reportStore } = useStore();

  useEffect(() => {
    /** Will need to debounce */
    const updateScrolled = () =>
      window.scrollY > 64 ? setScrolled(true) : setScrolled(false);

    document.addEventListener("scroll", updateScrolled);

    return () => {
      document.removeEventListener("scroll", updateScrolled);
    };
  }, [scrolled]);

  const reportOverview = reportStore.reportOverviews[reportID];
  const reportMetrics = reportStore.reportMetrics[reportID];

  if (!reportOverview || !reportMetrics) {
    return null;
  }

  return (
    <Form>
      {/* Form Title */}
      <PreTitle>Enter Data</PreTitle>
      <Title scrolled={scrolled} sticky>
        {reportOverview &&
          printReportTitle(
            reportOverview.month,
            reportOverview.year,
            reportOverview.frequency
          )}
      </Title>

      {/* Metrics */}
      {reportMetrics.map((metric, index) => {
        return (
          <Metric key={metric.key}>
            <MetricSectionTitle>{metric.display_name}</MetricSectionTitle>
            <MetricSectionSubTitle>{metric.description}</MetricSectionSubTitle>

            {/* Metric Value */}
            <MetricTextInput
              reportID={reportID}
              metric={metric}
              autoFocus={index === 0}
              updateFieldDescription={() =>
                updateFieldDescription(metric.display_name, metric.description)
              }
              clearFieldDescription={() => updateFieldDescription("", "")}
            />

            {/* Disaggregations */}
            {metric.disaggregations.length > 0 && (
              <DisaggregationDisplay
                reportID={reportID}
                currentIndex={index}
                reportMetrics={reportMetrics}
                metric={metric}
                updateFieldDescription={updateFieldDescription}
              />
            )}

            {/* Contexts */}
            {metric.contexts.length > 0 &&
              metric.contexts.map((context, contextIndex) => {
                if (context.type === "BOOLEAN") {
                  return (
                    <BinaryRadioGroupContainer
                      key={context.key}
                      onMouseEnter={() =>
                        updateFieldDescription(
                          context.display_name as string,
                          context.reporting_note as string
                        )
                      }
                      onMouseLeave={() => updateFieldDescription("", "")}
                    >
                      <BinaryRadioGroupQuestion>
                        {context.display_name}
                      </BinaryRadioGroupQuestion>

                      <BinaryRadioGroupWrapper>
                        <BinaryRadioButtonInputs
                          reportID={reportID}
                          metric={metric}
                          context={context}
                          contextIndex={contextIndex}
                        />
                      </BinaryRadioGroupWrapper>
                      <BinaryRadioGroupClearButton
                        onClick={() =>
                          formStore.resetBinaryInput(
                            reportID,
                            metric.key,
                            context.key,
                            context.required
                          )
                        }
                      >
                        Clear Input
                      </BinaryRadioGroupClearButton>
                    </BinaryRadioGroupContainer>
                  );
                }
                return (
                  <Fragment key={context.key}>
                    <AdditionalContextLabel>
                      {context.display_name}
                    </AdditionalContextLabel>
                    <AdditionalContextInput
                      reportID={reportID}
                      metric={metric}
                      context={context}
                      contextIndex={contextIndex}
                      updateFieldDescription={() =>
                        updateFieldDescription(
                          context.display_name as string,
                          context.reporting_note as string
                        )
                      }
                      clearFieldDescription={() =>
                        updateFieldDescription("", "")
                      }
                    />
                  </Fragment>
                );
              })}
          </Metric>
        );
      })}
      <OpacityGradient />
    </Form>
  );
};

export default observer(DataEntryForm);
