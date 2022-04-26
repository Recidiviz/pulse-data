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
import React, { Fragment } from "react";

import { Metric, ReportOverview } from "../../shared/types";
import { printReportTitle } from "../../utils";
import {
  AdditionalContextLabel,
  BinaryRadioButton,
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
  TextInput,
  Title,
  TitleWrapper,
} from "../Forms";
import {
  FormContexts,
  FormDimensions,
  FormDisaggregations,
  FormReport,
  useForm,
} from "../Forms/useForm";

interface DataEntryFormProps {
  reportOverview: ReportOverview;
  reportMetrics: Metric[];
}

const DataEntryForm: React.FC<DataEntryFormProps> = ({
  reportOverview,
  reportMetrics,
}) => {
  /** Maps fetched reportMetrics to a flattened structure */
  const createInitialFormReport = () => {
    const initialValues: FormReport = {};

    reportMetrics.forEach((metric: Metric, metricIndex: number) => {
      /** Metric */
      initialValues[metric.key] = {
        value: metric.value as string | number,
        contexts: {} as FormContexts,
        disaggregations: {} as FormDisaggregations,
      };

      /** Contexts */
      reportMetrics[metricIndex].contexts.forEach((context) => {
        initialValues[metric.key].contexts[context.key] = context.value as
          | string
          | number;
      });

      /** Disaggregations > Dimensions */
      reportMetrics[metricIndex].disaggregations.forEach((disaggregation) => {
        const dimensions = {} as FormDimensions;

        disaggregation.dimensions.forEach((dimension) => {
          dimensions[dimension.key] = dimension.value as string | number;
        });

        initialValues[metric.key].disaggregations[disaggregation.key] =
          dimensions;
      });
    });

    return initialValues;
  };

  /** Map Form metric object to report metrics */
  const mapFormReportToMetricReport = (
    metrics: Metric[],
    formReport: FormReport
  ) => {
    const updatedFormData = metrics.map((metric) => {
      if (formReport[metric.key]) {
        return {
          ...metric,
          value: formReport[metric.key].value,
          contexts: metric.contexts.map((context) => {
            return {
              ...context,
              value: formReport[metric.key].contexts[context.key],
            };
          }),
          disaggregations: metric.disaggregations.map((disaggregation) => {
            return {
              ...disaggregation,
              dimensions: disaggregation.dimensions.map((dimension) => {
                return {
                  ...dimension,
                  value:
                    formReport[metric.key].disaggregations[disaggregation.key][
                      dimension.key
                    ],
                };
              }),
            };
          }),
        };
      }
      return metric;
    });

    return updatedFormData;
  };

  const {
    formData,
    handleMetricChange,
    handleContextChange,
    handleDisaggregationDimensionChange,
    handleSubmit,
    resetBinaryInputs,
  } = useForm({ initialValues: createInitialFormReport() });

  return (
    <Form onSubmit={handleSubmit}>
      {/* Form Title */}
      <PreTitle>Enter Data</PreTitle>
      <Title>
        {printReportTitle(
          reportOverview.month,
          reportOverview.year,
          reportOverview.frequency as "MONTHLY" | "ANNUAL"
        )}
      </Title>

      {/* Metrics */}
      {reportMetrics.map((metric) => {
        return (
          <Fragment key={metric.key}>
            <TitleWrapper underlined>
              <MetricSectionTitle>{metric.display_name}</MetricSectionTitle>
              <MetricSectionSubTitle>
                {metric.description}
              </MetricSectionSubTitle>
            </TitleWrapper>

            {/* Metric Value */}
            <TextInput
              label={metric.label}
              error=""
              type="text"
              name={metric.key}
              valueLabel={metric.unit}
              context={metric.reporting_note}
              onChange={(e) => handleMetricChange(metric.key, e)}
              value={formData[metric.key].value || ""}
              required
            />

            {/* Disaggregations */}
            {metric.disaggregations.length > 0 &&
              metric.disaggregations.map((disaggregation) => {
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
                      disaggregation.dimensions.map((dimension) => {
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
                              handleDisaggregationDimensionChange(
                                metric.key,
                                disaggregation.key,
                                e
                              )
                            }
                            value={
                              formData[metric.key].disaggregations[
                                disaggregation.key
                              ][dimension.key] || ""
                            }
                            required={disaggregation.required}
                          />
                        );
                      })}
                  </DisaggregationToggle>
                );
              })}

            {/* Context Questions */}
            {metric.contexts.length > 0 &&
              metric.contexts.map((context) => {
                if (context.type === "BOOLEAN") {
                  return (
                    <BinaryRadioGroupContainer key={context.key}>
                      <BinaryRadioGroupQuestion>
                        {context.display_name}
                      </BinaryRadioGroupQuestion>

                      <BinaryRadioGroupWrapper>
                        <BinaryRadioButton
                          type="radio"
                          id={`${context.key}-yes`}
                          name={context.key}
                          label="Yes"
                          value="yes"
                          onChange={(e) => handleContextChange(metric.key, e)}
                          checked={
                            formData[metric.key].contexts[context.key] === "yes"
                          }
                        />
                        <BinaryRadioButton
                          type="radio"
                          id={`${context.key}-no`}
                          name={context.key}
                          label="No"
                          value="no"
                          onChange={(e) => handleContextChange(metric.key, e)}
                          checked={
                            formData[metric.key].contexts[context.key] === "no"
                          }
                        />
                      </BinaryRadioGroupWrapper>
                      <BinaryRadioGroupClearButton
                        data-name={context.key}
                        onClick={(e) => resetBinaryInputs(metric.key, e)}
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
                    <TextInput
                      error=""
                      type="text"
                      name={context.key}
                      label="Type here..."
                      context={context.reporting_note || ""}
                      onChange={(e) => handleContextChange(metric.key, e)}
                      value={formData[metric.key].contexts[context.key] || ""}
                      additionalContext
                    />
                  </Fragment>
                );
              })}
          </Fragment>
        );
      })}

      {/* For testing to help us see the form object update based on the input */}
      <pre
        style={{
          width: 320,
          height: 500,
          position: "fixed",
          zIndex: 2,
          bottom: 20,
          left: 20,
          background: "white",
          overflow: "scroll",
          fontSize: 10,
          lineHeight: 2,
          border: "1px dashed black",
          padding: 10,
        }}
      >
        {JSON.stringify(formData, null, 2)}
      </pre>

      <pre
        style={{
          width: 320,
          height: 500,
          position: "fixed",
          zIndex: 2,
          bottom: 20,
          right: 20,
          background: "white",
          overflow: "scroll",
          fontSize: 10,
          lineHeight: 2,
          border: "1px dashed black",
          padding: 10,
        }}
      >
        {JSON.stringify(
          mapFormReportToMetricReport(reportMetrics, formData),
          null,
          2
        )}
      </pre>
    </Form>
  );
};

export default DataEntryForm;
