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
import React, { useEffect, useRef, useState } from "react";

import { FormError, ReportFrequency } from "../../shared/types";
import { useStore } from "../../stores";
import { removeCommaSpaceAndTrim } from "../../utils";
import notReportedIcon from "../assets/not-reported-icon.png";
import {
  BinaryRadioButton,
  BinaryRadioGroupClearButton,
  BinaryRadioGroupContainer,
  BinaryRadioGroupQuestion,
  TextInput,
} from "../Forms";
import { PageTitle, TabbedBar, TabbedItem, TabbedOptions } from "../Reports";
import { showToast } from "../Toast";
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
  MetricSettingsDisplayError,
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

type MetricsViewMetric = {
  key: string;
  display_name: string;
  description: string;
  frequency: string;
  enabled: boolean;
  contexts: {
    key: string;
    display_name: string;
    reporting_note: string;
    required: boolean;
    type: string;
    value: string | null;
    multiple_choice_options?: string[];
  }[];
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
};

type MetricBoxProps = {
  metricKey: string;
  displayName: string;
  frequency: ReportFrequency;
  description: string;
  enabled?: boolean;
  activeMetricKey: string;
  setActiveMetricKey: React.Dispatch<React.SetStateAction<string>>;
};

const MetricBox: React.FC<MetricBoxProps> = ({
  metricKey,
  displayName,
  frequency,
  description,
  enabled,
  activeMetricKey,
  setActiveMetricKey,
}): JSX.Element => {
  return (
    <MetricBoxContainer
      onClick={() => setActiveMetricKey(metricKey)}
      enabled={enabled}
      selected={metricKey === activeMetricKey}
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
  activeMetricKey: string;
  metricSettings: { [key: string]: MetricsViewMetric };
  saveAndUpdateMetricSettings: (
    typeOfUpdate: "METRIC" | "DISAGGREGATION" | "DIMENSION" | "CONTEXT",
    updatedSetting: MetricSettings
  ) => void;
};

const MetricConfiguration: React.FC<MetricConfigurationProps> = ({
  activeMetricKey,
  metricSettings,
  saveAndUpdateMetricSettings,
}): JSX.Element => {
  const metricDisplayName = metricSettings[activeMetricKey]?.display_name;
  const metricEnabled = Boolean(metricSettings[activeMetricKey]?.enabled);

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
            checked={metricEnabled}
            onChange={() =>
              saveAndUpdateMetricSettings("METRIC", {
                key: activeMetricKey,
                enabled: true,
              })
            }
          />
          <BinaryRadioButton
            type="radio"
            id="no"
            name="metric-config"
            label="No"
            value="no"
            checked={!metricEnabled}
            onChange={() =>
              saveAndUpdateMetricSettings("METRIC", {
                key: activeMetricKey,
                enabled: false,
              })
            }
          />
        </RadioButtonGroupWrapper>
      </MetricOnOffWrapper>

      <MetricDisaggregations enabled={metricEnabled}>
        <Header>Breakdowns</Header>
        <Subheader>
          Turning any of these breakdowns “Off” means that they will not appear
          on automatically generated reports from here on out. You can change
          this later.
        </Subheader>

        {metricSettings[activeMetricKey]?.disaggregations?.map(
          (disaggregation) => {
            return (
              <Disaggregation key={disaggregation.key}>
                <DisaggregationHeader>
                  <DisaggregationName enabled={disaggregation.enabled}>
                    {disaggregation.display_name}
                  </DisaggregationName>

                  <ToggleSwitchWrapper>
                    <ToggleSwitchLabel switchedOn={disaggregation.enabled} />
                    <ToggleSwitch>
                      <ToggleSwitchInput
                        type="checkbox"
                        checked={disaggregation.enabled}
                        onChange={() =>
                          saveAndUpdateMetricSettings("DISAGGREGATION", {
                            key: activeMetricKey,
                            disaggregations: [
                              {
                                key: disaggregation.key,
                                enabled: !disaggregation.enabled,
                              },
                            ],
                          })
                        }
                      />
                      <Slider />
                    </ToggleSwitch>
                  </ToggleSwitchWrapper>
                </DisaggregationHeader>

                {disaggregation?.dimensions.map((dimension) => {
                  return (
                    <Dimension
                      key={dimension.key}
                      enabled={!metricEnabled || disaggregation.enabled}
                    >
                      <DimensionTitleWrapper>
                        <DimensionTitle
                          enabled={disaggregation.enabled && dimension.enabled}
                        >
                          {dimension.label}
                        </DimensionTitle>
                      </DimensionTitleWrapper>

                      <ToggleSwitchWrapper>
                        <ToggleSwitchLabel
                          switchedOn={
                            disaggregation.enabled && dimension.enabled
                          }
                        />
                        <ToggleSwitch>
                          <ToggleSwitchInput
                            type="checkbox"
                            checked={
                              disaggregation.enabled && dimension.enabled
                            }
                            onChange={() => {
                              if (disaggregation.enabled) {
                                saveAndUpdateMetricSettings("DIMENSION", {
                                  key: activeMetricKey,
                                  disaggregations: [
                                    {
                                      key: disaggregation.key,
                                      dimensions: [
                                        {
                                          key: dimension.key,
                                          enabled: !dimension.enabled,
                                        },
                                      ],
                                    },
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
          }
        )}
      </MetricDisaggregations>
    </MetricConfigurationContainer>
  );
};

type MetricSettingsUpdateOptions =
  | "METRIC"
  | "DISAGGREGATION"
  | "DIMENSION"
  | "CONTEXT";

type MetricContextConfigurationProps = {
  metricKey: string;
  contexts: {
    key: string;
    display_name: string;
    reporting_note: string;
    required: boolean;
    type: string;
    value: string | null;
    multiple_choice_options?: string[];
  }[];
  updateMetricSettings: (
    typeOfUpdate: MetricSettingsUpdateOptions,
    updatedSetting: MetricSettings
  ) => void;
  saveAndUpdateMetricSettings: (
    typeOfUpdate: MetricSettingsUpdateOptions,
    updatedSetting: MetricSettings
  ) => void;
};

const MetricContextConfiguration: React.FC<MetricContextConfigurationProps> = ({
  metricKey,
  contexts,
  updateMetricSettings,
  saveAndUpdateMetricSettings,
}) => {
  const [contextErrors, setContextErrors] =
    useState<{ [key: string]: FormError }>();

  const contextNumberValidation = (key: string, value: string) => {
    const cleanValue = removeCommaSpaceAndTrim(value);
    const isPositiveNumber =
      (cleanValue !== "" && Number(cleanValue) === 0) || Number(cleanValue) > 0;

    if (!isPositiveNumber && cleanValue !== "") {
      setContextErrors({
        [key]: {
          message:
            "The value entered cannot be saved. Please enter a valid number.",
        },
      });
      return false;
    }

    setContextErrors((prev) => {
      const otherContextErrors = { ...prev };
      delete otherContextErrors[key];

      return otherContextErrors;
    });
    return true;
  };

  useEffect(() => {
    contexts.forEach((context) => {
      if (context.type === "NUMBER") {
        contextNumberValidation(context.key, context.value || "");
      }
    });
  }, [contexts]);

  return (
    <MetricContextContainer>
      <Subheader>
        Anything entered here will appear as the default value for all reports.
        If you are entering data for a particular month, you can still replace
        this as necessary.
      </Subheader>

      {contexts?.map((context) => (
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
                  checked={context.value === "yes"}
                  onChange={() =>
                    saveAndUpdateMetricSettings("CONTEXT", {
                      key: metricKey,
                      contexts: [{ key: context.key, value: "yes" }],
                    })
                  }
                />
                <BinaryRadioButton
                  type="radio"
                  id={`${context.key}-no`}
                  name={context.key}
                  label="No"
                  value="no"
                  checked={context.value === "no"}
                  onChange={() =>
                    saveAndUpdateMetricSettings("CONTEXT", {
                      key: metricKey,
                      contexts: [{ key: context.key, value: "no" }],
                    })
                  }
                />
              </RadioButtonGroupWrapper>
              <BinaryRadioGroupClearButton
                onClick={() =>
                  saveAndUpdateMetricSettings("CONTEXT", {
                    key: metricKey,
                    contexts: [{ key: context.key, value: "" }],
                  })
                }
              >
                Clear Input
              </BinaryRadioGroupClearButton>
            </>
          )}

          {(context.type === "TEXT" || context.type === "NUMBER") && (
            <>
              <Label>{context.display_name}</Label>
              <TextInput
                type="text"
                name={context.key}
                id={context.key}
                label=""
                value={context.value || ""}
                multiline={context.type === "TEXT"}
                error={contextErrors?.[context.key]}
                onChange={(e) => {
                  if (context.type === "NUMBER") {
                    const isContextValid = contextNumberValidation(
                      context.key,
                      e.currentTarget.value
                    );

                    if (!isContextValid) {
                      return updateMetricSettings("CONTEXT", {
                        key: metricKey,
                        contexts: [
                          { key: context.key, value: e.currentTarget.value },
                        ],
                      });
                    }
                  }

                  saveAndUpdateMetricSettings("CONTEXT", {
                    key: metricKey,
                    contexts: [
                      { key: context.key, value: e.currentTarget.value },
                    ],
                  });
                }}
              />
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
                    checked={context.value === option}
                    onChange={() =>
                      saveAndUpdateMetricSettings("CONTEXT", {
                        key: metricKey,
                        contexts: [{ key: context.key, value: option }],
                      })
                    }
                  />
                ))}
              </MultipleChoiceWrapper>
              <BinaryRadioGroupClearButton
                onClick={() =>
                  saveAndUpdateMetricSettings("CONTEXT", {
                    key: metricKey,
                    contexts: [{ key: context.key, value: "" }],
                  })
                }
              >
                Clear Input
              </BinaryRadioGroupClearButton>
            </BinaryRadioGroupContainer>
          )}
        </MetricContextItem>
      ))}
    </MetricContextContainer>
  );
};

export type MetricSettings = {
  key: string;
  enabled?: boolean;
  contexts?: {
    key: string;
    value: string;
  }[];
  disaggregations?: {
    key: string;
    enabled?: boolean;
    dimensions?: {
      key: string;
      enabled: boolean;
    }[];
  }[];
};

export const MetricsView: React.FC = observer(() => {
  const { reportStore } = useStore();
  const configPanelRef = useRef<HTMLDivElement>(null);

  const metricFilterOptions = ["All Metrics", "Active"] as const;
  type MetricFilterOptions = typeof metricFilterOptions[number];

  const configSections = ["Configuration", "Context", "Data"] as const;
  type ConfigSections = typeof configSections[number];

  const [activeMetricFilter, setActiveMetricFilter] =
    useState<MetricFilterOptions>("All Metrics");

  const [activeConfigSection, setActiveConfigSection] =
    useState<ConfigSections>("Configuration");

  const [activeMetricKey, setActiveMetricKey] = useState<string>("");

  const [metricSettings, setMetricSettings] = useState<{
    [key: string]: MetricsViewMetric;
  }>({});

  const [filteredMetricSettings, setFilteredMetricSettings] = useState<{
    [key: string]: MetricsViewMetric;
  }>({});

  const updateMetricSettings = (
    typeOfUpdate: MetricSettingsUpdateOptions,
    updatedSetting: MetricSettings
  ) => {
    setMetricSettings((prev) => {
      const metricKey = updatedSetting.key;

      if (typeOfUpdate === "METRIC") {
        return {
          ...prev,
          [updatedSetting.key]: {
            ...prev[metricKey],
            enabled: Boolean(updatedSetting.enabled),
          },
        };
      }

      if (typeOfUpdate === "DISAGGREGATION") {
        const updatedDisaggregations = prev[metricKey].disaggregations.map(
          (disaggregation) => {
            /** Quick Note: for now, all updates happen one at a time thus leaving
             * one item in the disaggregations/dimensions/contexts arrays that will
             * be updated at any one time. We can update this in the future to handle
             * updating multiple settings at one time if necessary.
             */
            if (
              disaggregation.key === updatedSetting.disaggregations?.[0].key
            ) {
              return {
                ...disaggregation,
                enabled: Boolean(updatedSetting.disaggregations?.[0].enabled),
              };
            }
            return disaggregation;
          }
        );

        return {
          ...prev,
          [updatedSetting.key]: {
            ...prev[metricKey],
            disaggregations: updatedDisaggregations,
          },
        };
      }

      if (typeOfUpdate === "DIMENSION") {
        const updatedDisaggregations = prev[metricKey].disaggregations.map(
          (disaggregation) => {
            if (
              disaggregation.key === updatedSetting.disaggregations?.[0].key
            ) {
              return {
                ...disaggregation,
                dimensions: disaggregation.dimensions.map((dimension) => {
                  if (
                    dimension.key ===
                    updatedSetting.disaggregations?.[0].dimensions?.[0].key
                  ) {
                    return {
                      ...dimension,
                      enabled: Boolean(
                        updatedSetting.disaggregations?.[0].dimensions?.[0]
                          .enabled
                      ),
                    };
                  }
                  return dimension;
                }),
              };
            }
            return disaggregation;
          }
        );

        return {
          ...prev,
          [updatedSetting.key]: {
            ...prev[metricKey],
            disaggregations: updatedDisaggregations,
          },
        };
      }

      if (typeOfUpdate === "CONTEXT") {
        const updatedContext = prev[metricKey].contexts.map((context) => {
          if (context.key === updatedSetting.contexts?.[0].key) {
            return {
              ...context,
              value: updatedSetting.contexts?.[0].value,
            };
          }
          return context;
        });

        return {
          ...prev,
          [updatedSetting.key]: {
            ...prev[metricKey],
            contexts: updatedContext,
          },
        };
      }

      return prev;
    });
  };

  const saveAndUpdateMetricSettings = async (
    typeOfUpdate: MetricSettingsUpdateOptions,
    updatedSetting: MetricSettings
  ) => {
    const response = (await reportStore.updateReportSettings([
      updatedSetting,
    ])) as Response;

    if (response.status === 200) {
      updateMetricSettings(typeOfUpdate, updatedSetting);
      showToast(`Settings saved.`, true, "grey", 4000);
    } else {
      showToast(
        `Something went wrong with saving the settings. Please try again.`,
        true,
        "red",
        4000
      );
    }
  };

  useEffect(() => {
    const fetchReportSettings = async () => {
      const response = (await reportStore.getReportSettings()) as Response;
      const reportSettings = (await response.json()) as MetricsViewMetric[];
      const metricKeyToMetricMap: { [key: string]: MetricsViewMetric } = {};

      reportSettings?.forEach((metric) => {
        metricKeyToMetricMap[metric.key] = metric;
      });

      setMetricSettings(metricKeyToMetricMap);
      setActiveMetricKey(Object.keys(metricKeyToMetricMap)[0]);
    };

    fetchReportSettings();
  }, [reportStore]);

  useEffect(() => {
    if (activeMetricFilter === "All Metrics") {
      return setFilteredMetricSettings(metricSettings);
    }

    if (activeMetricFilter === "Active") {
      const filteredMetricKeyToMetricMap: { [key: string]: MetricsViewMetric } =
        {};

      Object.values(metricSettings)
        .filter((metric) => metric.enabled)
        ?.forEach((metric) => {
          filteredMetricKeyToMetricMap[metric.key] = metric;
        });

      return setFilteredMetricSettings(filteredMetricKeyToMetricMap);
    }
  }, [metricSettings, activeMetricFilter]);

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

        {metricSettings[activeMetricKey] ? (
          <MetricsViewControlPanel>
            {/* List Of Metrics */}
            <PanelContainerLeft
              onClick={() => {
                if (configPanelRef.current) {
                  configPanelRef.current.scrollTo({
                    top: 0,
                    behavior: "smooth",
                  });
                }
                setActiveConfigSection(configSections[0]);
              }}
            >
              {filteredMetricSettings &&
                Object.values(filteredMetricSettings).map((metric) => (
                  <MetricBox
                    key={metric.key}
                    metricKey={metric.key}
                    displayName={metric.display_name}
                    frequency={metric.frequency as ReportFrequency}
                    description={metric.description}
                    enabled={metric.enabled}
                    activeMetricKey={activeMetricKey}
                    setActiveMetricKey={setActiveMetricKey}
                  />
                ))}
            </PanelContainerLeft>

            {/* Configuration | Context | Data */}
            <PanelContainerRight ref={configPanelRef}>
              <MetricNameBadgeWrapper>
                <MetricName isTitle>
                  {metricSettings[activeMetricKey]?.display_name}
                </MetricName>
                <MetricsViewBadge
                  frequency={
                    metricSettings[activeMetricKey]
                      ?.frequency as ReportFrequency
                  }
                >
                  {metricSettings[activeMetricKey]?.frequency}
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
                    activeMetricKey={activeMetricKey}
                    metricSettings={metricSettings}
                    saveAndUpdateMetricSettings={saveAndUpdateMetricSettings}
                  />
                )}

                {/* Context */}
                {activeConfigSection === "Context" && (
                  <MetricContextConfiguration
                    metricKey={activeMetricKey}
                    contexts={metricSettings[activeMetricKey]?.contexts}
                    updateMetricSettings={updateMetricSettings}
                    saveAndUpdateMetricSettings={saveAndUpdateMetricSettings}
                  />
                )}

                {/* Data */}
                {activeConfigSection === "Data" && (
                  <div>Placeholder for Data Component</div>
                )}
              </MetricDetailsDisplay>
            </PanelContainerRight>
          </MetricsViewControlPanel>
        ) : (
          <MetricSettingsDisplayError>
            There was an issue retrieving the report metric settings. Please
            refresh and try again.
          </MetricSettingsDisplayError>
        )}
      </MetricsViewContainer>
    </>
  );
});
