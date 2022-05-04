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
import React, { Fragment } from "react";

import { useStore } from "../../stores";
import { printReportTitle } from "../../utils";
import {
  AdditionalContextLabel,
  BinaryRadioGroupClearButton,
  BinaryRadioGroupContainer,
  BinaryRadioGroupQuestion,
  BinaryRadioGroupWrapper,
  DisaggregationContentHelperText,
  DisaggregationToggle,
  Form,
  MetricSectionSubTitle,
  MetricSectionTitle,
  PreTitle,
  Title,
  TitleWrapper,
} from "../Forms";
import {
  AdditionalContextInput,
  BinaryRadioButtonInputs,
  DisaggregationDimensionTextInput,
  MetricTextInput,
} from "./DataEntryFormComponents";

const DataEntryForm: React.FC<{ reportID: number; activeMetric: string }> = ({
  reportID,
  activeMetric,
}) => {
  const { formStore, reportStore } = useStore();

  const reportOverview = reportStore.reportOverviews[reportID];
  const reportMetrics = reportStore.reportMetrics[reportID];

  if (!reportOverview || !reportMetrics) {
    return null;
  }

  return (
    <Form>
      {/* Form Title */}
      <PreTitle>Enter Data</PreTitle>
      <Title>
        {reportOverview &&
          printReportTitle(
            reportOverview.month,
            reportOverview.year,
            reportOverview.frequency
          )}
      </Title>

      {/* Metrics */}
      {reportMetrics
        ?.filter((metric) => metric.key === activeMetric)
        .map((metric) => {
          return (
            <Fragment key={metric.key}>
              <TitleWrapper underlined>
                <MetricSectionTitle>{metric.display_name}</MetricSectionTitle>
                <MetricSectionSubTitle>
                  {metric.description}
                </MetricSectionSubTitle>
              </TitleWrapper>

              {/* Metric Value */}
              <MetricTextInput metric={metric} />

              {/* Disaggregations */}
              {metric.disaggregations.length > 0 &&
                metric.disaggregations.map(
                  (disaggregation, disaggregationIndex) => {
                    return (
                      <DisaggregationToggle
                        key={disaggregation.key}
                        description={disaggregation.display_name}
                      >
                        {disaggregation.helper_text && (
                          <DisaggregationContentHelperText>
                            {disaggregation.helper_text}
                          </DisaggregationContentHelperText>
                        )}

                        {/* Dimensions */}
                        {disaggregation.dimensions.length > 0 &&
                          disaggregation.dimensions.map(
                            (dimension, dimensionIndex) => {
                              return (
                                <DisaggregationDimensionTextInput
                                  key={dimension.key}
                                  metric={metric}
                                  dimension={dimension}
                                  disaggregation={disaggregation}
                                  disaggregationIndex={disaggregationIndex}
                                  dimensionIndex={dimensionIndex}
                                />
                              );
                            }
                          )}
                      </DisaggregationToggle>
                    );
                  }
                )}

              {/* Contexts */}
              {metric.contexts.length > 0 &&
                metric.contexts.map((context, contextIndex) => {
                  if (context.type === "BOOLEAN") {
                    return (
                      <BinaryRadioGroupContainer key={context.key}>
                        <BinaryRadioGroupQuestion>
                          {context.display_name}
                        </BinaryRadioGroupQuestion>

                        <BinaryRadioGroupWrapper>
                          <BinaryRadioButtonInputs
                            metric={metric}
                            context={context}
                            contextIndex={contextIndex}
                          />
                        </BinaryRadioGroupWrapper>
                        <BinaryRadioGroupClearButton
                          data-name={context.key}
                          onClick={(e) =>
                            formStore.resetBinaryInput(metric.key, e)
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
                        metric={metric}
                        context={context}
                        contextIndex={contextIndex}
                      />
                    </Fragment>
                  );
                })}
            </Fragment>
          );
        })}
    </Form>
  );
};

export default observer(DataEntryForm);
