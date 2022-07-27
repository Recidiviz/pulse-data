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

import React, { useState } from "react";

import { ReportFrequency } from "../../shared/types";
import notReportedIcon from "../assets/not-reported-icon.png";
import {
  BinaryRadioButton,
  BinaryRadioGroupClearButton,
  BinaryRadioGroupContainer,
  BinaryRadioGroupQuestion,
  TextInput,
} from "../Forms";
import { PageTitle, TabbedBar, TabbedItem, TabbedOptions } from "../Reports";
import {
  Dimension,
  DimensionTitle,
  DimensionTitleWrapper,
  Disaggregation,
  DisaggregationHeader,
  DisaggregationName,
  Header,
  Label,
  MetricBoxContainer,
  MetricConfigurationContainer,
  MetricContextContainer,
  MetricContextItem,
  MetricDescription,
  MetricDetailsDisplay,
  MetricDisaggregations,
  MetricName,
  MetricNameBadgeToggleWrapper,
  MetricNameBadgeWrapper,
  MetricOnOffWrapper,
  MetricsViewBadge,
  MetricsViewContainer,
  MetricsViewControlPanel,
  MultipleChoiceWrapper,
  NotReportedIcon,
  PanelContainerLeft,
  PanelContainerRight,
  RadioButtonGroupWrapper,
  Slider,
  Subheader,
  ToggleSwitch,
  ToggleSwitchInput,
  ToggleSwitchLabel,
  ToggleSwitchWrapper,
} from ".";
import { metricsViewMockResponse } from "./MetricsViewMocks";

type MetricBoxProps = {
  metricIndex: number;
  displayName: string;
  frequency: ReportFrequency;
  description: string;
  enabled?: boolean;
  activeMetricIndex: number;
  setActiveMetricIndex: React.Dispatch<React.SetStateAction<number>>;
};

const MetricBox: React.FC<MetricBoxProps> = ({
  metricIndex,
  displayName,
  frequency,
  description,
  enabled,
  activeMetricIndex,
  setActiveMetricIndex,
}): JSX.Element => {
  return (
    <MetricBoxContainer
      onClick={() => setActiveMetricIndex(metricIndex)}
      enabled={enabled}
      selected={metricIndex === activeMetricIndex}
    >
      <MetricNameBadgeToggleWrapper>
        <MetricNameBadgeWrapper>
          <MetricName>{displayName}</MetricName>
          <MetricsViewBadge frequency={frequency} enabled={enabled}>
            {frequency}
          </MetricsViewBadge>
        </MetricNameBadgeWrapper>

        {!enabled && <NotReportedIcon src={notReportedIcon} alt="" />}
      </MetricNameBadgeToggleWrapper>

      <MetricDescription>{description}</MetricDescription>
    </MetricBoxContainer>
  );
};

type MetricConfigurationProps = {
  metricKey: string;
  metricDisplayName: string;
  metricEnabled: boolean;
  disaggregations: {
    key: string;
    display_name: string;
    enabled: boolean;
    dimensions: {
      key: string;
      label: string;
      reporting_note: string;
      enabled: boolean;
    }[];
  }[];
  placeholderMetricSettingsToUpdate:
    | { [key: string]: string | boolean }
    | undefined;
  updatePlaceholderMetricSettings: (updatedSetting: {
    [key: string]: string | boolean;
  }) => void;
};

const MetricConfiguration: React.FC<MetricConfigurationProps> = ({
  metricKey,
  metricDisplayName,
  metricEnabled,
  disaggregations,
  placeholderMetricSettingsToUpdate,
  updatePlaceholderMetricSettings,
}): JSX.Element => {
  const isUpdatedMetricEnabled = placeholderMetricSettingsToUpdate?.[metricKey];

  return (
    <MetricConfigurationContainer>
      <MetricOnOffWrapper>
        <Header>
          Are you currently able to report any part of this metric?
        </Header>
        <Subheader>
          Answering “No” means that {metricDisplayName} will not appear on
          automatically generated reports from here on out. You can change this
          later.
        </Subheader>
        <RadioButtonGroupWrapper>
          <BinaryRadioButton
            type="radio"
            id="yes"
            name="metric-config"
            label="Yes"
            value="yes"
            checked={
              isUpdatedMetricEnabled === true ||
              (isUpdatedMetricEnabled === undefined && metricEnabled)
            }
            onChange={() =>
              updatePlaceholderMetricSettings({
                [metricKey]: true,
              })
            }
          />
          <BinaryRadioButton
            type="radio"
            id="no"
            name="metric-config"
            label="No"
            value="no"
            checked={
              isUpdatedMetricEnabled === false ||
              (isUpdatedMetricEnabled === undefined && !metricEnabled)
            }
            onChange={() => {
              updatePlaceholderMetricSettings({
                [metricKey]: false,
              });
            }}
          />
        </RadioButtonGroupWrapper>
      </MetricOnOffWrapper>

      <MetricDisaggregations
        enabled={
          isUpdatedMetricEnabled === true ||
          (isUpdatedMetricEnabled === undefined && metricEnabled)
        }
      >
        <Header>Breakdowns</Header>
        <Subheader>
          Turning any of these breakdowns “Off” means that they will not appear
          on automatically generated reports from here on out. You can change
          this later.
        </Subheader>

        {disaggregations.map((disaggregation) => {
          const isUpdatedDisaggregationEnabled =
            placeholderMetricSettingsToUpdate?.[disaggregation.key] as boolean;

          return (
            <Disaggregation key={disaggregation.key}>
              <DisaggregationHeader>
                <DisaggregationName>
                  {disaggregation.display_name}
                </DisaggregationName>

                <ToggleSwitchWrapper>
                  <ToggleSwitchLabel
                    switchedOn={
                      isUpdatedDisaggregationEnabled === true ||
                      (isUpdatedDisaggregationEnabled === undefined &&
                        disaggregation.enabled)
                    }
                  />
                  <ToggleSwitch>
                    <ToggleSwitchInput
                      type="checkbox"
                      checked={
                        (placeholderMetricSettingsToUpdate?.[
                          disaggregation.key
                        ] === undefined &&
                          disaggregation.enabled) ||
                        isUpdatedDisaggregationEnabled === true
                      }
                      onChange={() => {
                        const updatedValue =
                          isUpdatedDisaggregationEnabled === undefined
                            ? !disaggregation.enabled
                            : !isUpdatedDisaggregationEnabled;

                        updatePlaceholderMetricSettings({
                          [disaggregation.key]: updatedValue,
                        });
                      }}
                    />
                    <Slider />
                  </ToggleSwitch>
                </ToggleSwitchWrapper>
              </DisaggregationHeader>

              {disaggregation.dimensions.map((dimension) => {
                const disaggregationEnabled =
                  isUpdatedDisaggregationEnabled === undefined
                    ? disaggregation.enabled
                    : isUpdatedDisaggregationEnabled;

                return (
                  <Dimension
                    key={dimension.key}
                    enabled={
                      isUpdatedMetricEnabled === false || disaggregationEnabled
                    }
                  >
                    <DimensionTitleWrapper>
                      <DimensionTitle
                        enabled={
                          disaggregationEnabled &&
                          placeholderMetricSettingsToUpdate?.[dimension.key] ===
                            true
                        }
                      >
                        {dimension.label}
                      </DimensionTitle>
                    </DimensionTitleWrapper>

                    <ToggleSwitchWrapper>
                      <ToggleSwitchLabel
                        switchedOn={
                          (placeholderMetricSettingsToUpdate?.[
                            dimension.key
                          ] === true ||
                            (placeholderMetricSettingsToUpdate?.[
                              dimension.key
                            ] === undefined &&
                              !dimension.enabled)) &&
                          disaggregationEnabled
                        }
                      />
                      <ToggleSwitch>
                        <ToggleSwitchInput
                          type="checkbox"
                          checked={
                            ((placeholderMetricSettingsToUpdate?.[
                              dimension.key
                            ] === undefined &&
                              !dimension.enabled) ||
                              placeholderMetricSettingsToUpdate?.[
                                dimension.key
                              ] === true) &&
                            disaggregationEnabled
                          }
                          onChange={() => {
                            if (disaggregationEnabled) {
                              updatePlaceholderMetricSettings({
                                [dimension.key]:
                                  (placeholderMetricSettingsToUpdate?.[
                                    dimension.key
                                  ] === undefined &&
                                    !dimension.enabled) ||
                                  !placeholderMetricSettingsToUpdate?.[
                                    dimension.key
                                  ],
                              });
                            }
                          }}
                        />
                        <Slider />
                      </ToggleSwitch>
                    </ToggleSwitchWrapper>
                  </Dimension>
                );
              })}
            </Disaggregation>
          );
        })}
      </MetricDisaggregations>
    </MetricConfigurationContainer>
  );
};

type MetricContextConfigurationProps = {
  contexts:
    | {
        key: string;
        display_name: string;
        reporting_note: string;
        required: boolean;
        type: string;
        value: string;
        multiple_choice_options?: string[];
      }[]
    | {
        key: string;
        display_name: string;
        reporting_note: string;
        required: boolean;
        type: string;
        value: null;
        multiple_choice_options?: string[];
      }[];
};

const MetricContextConfiguration: React.FC<MetricContextConfigurationProps> = ({
  contexts,
}) => {
  return (
    <MetricContextContainer>
      <Subheader>
        Anything entered here will appear as the default value for all reports.
        If you are entering data for a particular month, you can still replace
        this as necessary.
      </Subheader>

      {contexts.map((context) => (
        <MetricContextItem key={context.key}>
          {context.type === "BOOLEAN" && (
            <>
              <Label noBottomMargin>{context.display_name}</Label>
              <RadioButtonGroupWrapper>
                <BinaryRadioButton
                  type="radio"
                  id={`${context.key}-yes`}
                  name={context.key}
                  label="Yes"
                  value="yes"
                  defaultChecked
                />
                <BinaryRadioButton
                  type="radio"
                  id={`${context.key}-no`}
                  name={context.key}
                  label="No"
                  value="no"
                />
              </RadioButtonGroupWrapper>
              <BinaryRadioGroupClearButton>
                Clear Input
              </BinaryRadioGroupClearButton>
            </>
          )}

          {context.type === "TEXT" && (
            <>
              <Label>{context.display_name}</Label>
              <TextInput type="text" label="" />
            </>
          )}

          {context.type === "MULTIPLE_CHOICE" && (
            <BinaryRadioGroupContainer key={context.key}>
              <BinaryRadioGroupQuestion>
                {context.display_name}
              </BinaryRadioGroupQuestion>

              <MultipleChoiceWrapper>
                {context.multiple_choice_options?.map((option) => (
                  <BinaryRadioButton
                    type="radio"
                    key={option}
                    id={`${context.key}-${option}`}
                    name={`${context.key}`}
                    label={option}
                    value={option}
                  />
                ))}
              </MultipleChoiceWrapper>
              <BinaryRadioGroupClearButton>
                Clear Input
              </BinaryRadioGroupClearButton>
            </BinaryRadioGroupContainer>
          )}
        </MetricContextItem>
      ))}
    </MetricContextContainer>
  );
};

export const MetricsView: React.FC = () => {
  const metricFilterOptions = ["All Metrics"] as const;
  type MetricFilterOptions = typeof metricFilterOptions[number];

  const configSections = ["Configuration", "Context", "Data"] as const;
  type ConfigSections = typeof configSections[number];

  const [activeMetricFilter, setActiveMetricFilter] =
    useState<MetricFilterOptions>("All Metrics");

  const [activeConfigSection, setActiveConfigSection] =
    useState<ConfigSections>("Configuration");

  const [activeMetricIndex, setActiveMetricIndex] = useState<number>(0);

  const [
    placeholderMetricSettingsToUpdate,
    setPlaceholderMetricSettingsToUpdate,
  ] = useState<{ [key: string]: string | boolean }>();

  const updatePlaceholderMetricSettings = (updatedSetting: {
    [key: string]: string | boolean;
  }) =>
    setPlaceholderMetricSettingsToUpdate((prev) => ({
      ...prev,
      ...updatedSetting,
    }));

  return (
    <>
      <MetricsViewContainer>
        <PageTitle>Metrics</PageTitle>

        <TabbedBar>
          <TabbedOptions>
            {metricFilterOptions.map((filterOption) => (
              <TabbedItem
                key={filterOption}
                selected={activeMetricFilter === filterOption}
                onClick={() => setActiveMetricFilter(filterOption)}
              >
                {filterOption}
              </TabbedItem>
            ))}
          </TabbedOptions>
        </TabbedBar>

        <MetricsViewControlPanel>
          {/* List Of Metrics */}
          <PanelContainerLeft>
            {metricsViewMockResponse.map((metric, index) => (
              <MetricBox
                key={metric.key}
                metricIndex={index}
                displayName={metric.display_name}
                frequency={metric.frequency as ReportFrequency}
                description={metric.description}
                enabled={
                  placeholderMetricSettingsToUpdate?.[
                    metricsViewMockResponse[index].key
                  ] === true ||
                  (placeholderMetricSettingsToUpdate?.[
                    metricsViewMockResponse[index].key
                  ] === undefined &&
                    metricsViewMockResponse[index].enabled)
                }
                activeMetricIndex={activeMetricIndex}
                setActiveMetricIndex={setActiveMetricIndex}
              />
            ))}
          </PanelContainerLeft>

          {/* Configuration | Context | Data */}
          <PanelContainerRight>
            <MetricNameBadgeWrapper>
              <MetricName isTitle>
                {metricsViewMockResponse[activeMetricIndex].display_name}
              </MetricName>
              <MetricsViewBadge
                frequency={
                  metricsViewMockResponse[activeMetricIndex]
                    .frequency as ReportFrequency
                }
              >
                {metricsViewMockResponse[activeMetricIndex].frequency}
              </MetricsViewBadge>
            </MetricNameBadgeWrapper>

            <TabbedBar noPadding>
              <TabbedOptions>
                {configSections.map((section) => (
                  <TabbedItem
                    key={section}
                    selected={activeConfigSection === section}
                    onClick={() => setActiveConfigSection(section)}
                  >
                    {section}
                  </TabbedItem>
                ))}
              </TabbedOptions>
            </TabbedBar>

            <MetricDetailsDisplay>
              {/* Configuration */}
              {activeConfigSection === "Configuration" && (
                <MetricConfiguration
                  metricKey={metricsViewMockResponse[activeMetricIndex].key}
                  metricDisplayName={
                    metricsViewMockResponse[activeMetricIndex].display_name
                  }
                  metricEnabled={
                    metricsViewMockResponse[activeMetricIndex].enabled
                  }
                  disaggregations={
                    metricsViewMockResponse[activeMetricIndex].disaggregations
                  }
                  placeholderMetricSettingsToUpdate={
                    placeholderMetricSettingsToUpdate
                  }
                  updatePlaceholderMetricSettings={
                    updatePlaceholderMetricSettings
                  }
                />
              )}

              {/* Context */}
              {activeConfigSection === "Context" && (
                <MetricContextConfiguration
                  contexts={metricsViewMockResponse[activeMetricIndex].contexts}
                />
              )}

              {/* Data */}
              {activeConfigSection === "Data" && (
                <div>Placeholder for Data Component</div>
              )}
            </MetricDetailsDisplay>
          </PanelContainerRight>
        </MetricsViewControlPanel>
      </MetricsViewContainer>
    </>
  );
};
