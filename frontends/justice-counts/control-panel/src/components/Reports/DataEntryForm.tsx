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

import { runInAction } from "mobx";
import { observer } from "mobx-react-lite";
import React, { Fragment, useEffect, useRef, useState } from "react";
import { useNavigate } from "react-router-dom";
import styled from "styled-components/macro";

import {
  trackAutosaveFailed,
  trackAutosaveTriggered,
  trackReportNotStartedToDraft,
} from "../../analytics";
import { useStore } from "../../stores";
import { memoizeDebounce, printReportTitle } from "../../utils";
import {
  BinaryRadioGroupClearButton,
  BinaryRadioGroupContainer,
  BinaryRadioGroupQuestion,
  BinaryRadioGroupWrapper,
  ErrorWithTooltip,
  Form,
  FormWrapper,
  GoBackToReportsOverviewLink,
  Metric,
  MetricSectionSubTitle,
  MetricSectionTitle,
  MetricSectionTitleWrapper,
  NotReportedIcon,
  OnePanelBackLinkContainer,
  OpacityGradient,
  PreTitle,
  RequiredChip,
  TabbedDisaggregations,
  Title,
} from "../Forms";
import { palette, typography } from "../GlobalStyles";
import { Onboarding, OnboardingDataEntrySummary } from "../Onboarding";
import { showToast } from "../Toast";
import {
  AdditionalContextInput,
  BinaryRadioButtonInputs,
  MetricTextInput,
} from "./DataEntryFormComponents";
import {
  DATA_ENTRY_WIDTH,
  ONE_PANEL_MAX_WIDTH,
  PublishButton,
  SIDE_PANEL_HORIZONTAL_PADDING,
} from "./ReportDataEntry.styles";

const DataEntryFormPublishButtonContainer = styled.div`
  position: fixed;
  display: flex;
  bottom: 0;
  left: 0;
  right: 0;
  padding: 0 ${SIDE_PANEL_HORIZONTAL_PADDING}px;
  justify-content: center;
  padding: 0 24px 8px;
  background: ${palette.solid.white};
`;

const DataEntryFormPublishButton = styled(PublishButton)`
  display: none;
  @media only screen and (max-width: ${ONE_PANEL_MAX_WIDTH}px) {
    display: block;
    flex: 0 1 ${DATA_ENTRY_WIDTH}px;
  }
`;

const DataEntryFormRequiredChip = styled(RequiredChip)`
  margin-right: 16px;
`;

const DataEntryFormErrorWithTooltipContainer = styled.div`
  position: absolute;
  width: 100%;
  top: 70px;
`;

const AdditionalContextLabel = styled.div`
  ${typography.sizeCSS.medium}
  margin-top: 40px;
  margin-bottom: 16px;
`;

const DataEntryForm: React.FC<{
  reportID: number;
  updateFieldDescription: (title?: string, description?: string) => void;
  updateActiveMetric: (metricKey: string) => void;
  toggleConfirmationDialogue: () => void;
}> = ({
  reportID,
  updateFieldDescription,
  updateActiveMetric,
  toggleConfirmationDialogue,
}) => {
  const [showOnboarding, setShowOnboarding] = useState(true);
  const [hasVersionConflict, setHasVersionConflict] = useState(false);
  const [scrolled, setScrolled] = useState(false);
  const metricsRef = useRef<HTMLDivElement[]>([]);
  const { formStore, reportStore, userStore } = useStore();
  const navigate = useNavigate();

  const isPublished =
    reportStore.reportOverviews[reportID].status === "PUBLISHED";

  useEffect(() => {
    /** Will need to debounce */
    const updateScrolled = () =>
      window.scrollY > 64 ? setScrolled(true) : setScrolled(false);

    document.addEventListener("scroll", updateScrolled);

    return () => {
      document.removeEventListener("scroll", updateScrolled);
    };
  }, [scrolled]);

  useEffect(
    () => {
      const metricRefsToCleanUp = metricsRef.current;

      const observerOptions: IntersectionObserverInit = {
        root: null,
        rootMargin: "0px",
        threshold: 0.7,
      };

      const observerCallback = (entries: IntersectionObserverEntry[]) => {
        entries.forEach((entry: IntersectionObserverEntry) => {
          if (entry.isIntersecting) {
            updateActiveMetric(entry.target.id);
          }
        });
      };

      const intersectionObserver = new IntersectionObserver(
        observerCallback,
        observerOptions
      );

      metricsRef.current.forEach(
        (metricElement) =>
          metricElement && intersectionObserver.observe(metricElement)
      );

      return () =>
        metricRefsToCleanUp.forEach(
          (metricElement) =>
            metricElement && intersectionObserver.unobserve(metricElement)
        );
    },
    // eslint-disable-next-line react-hooks/exhaustive-deps
    []
  );

  useEffect(() => {
    /** Runs validation of previously saved inputs on load */
    formStore.validatePreviouslySavedInputs(reportID);
  }, [formStore, reportID]);

  const saveUpdatedMetrics = async (metricKey?: string | undefined) => {
    const updatedMetrics = formStore.reportUpdatedValuesForBackend(
      reportID,
      metricKey
    );
    const oldStatus = reportStore.reportOverviews[reportID].status;
    const status =
      reportStore.reportOverviews[reportID].status === "PUBLISHED"
        ? "PUBLISHED"
        : "DRAFT";

    showToast("Saving...", false, "grey", -1, true);
    trackAutosaveTriggered(reportID);
    const response = (await reportStore.updateReport(
      reportID,
      updatedMetrics,
      status
    )) as Response;

    if (response.status === 200) {
      showToast("Saved", false, "grey");
      if (oldStatus === "NOT_STARTED" && status === "DRAFT") {
        const agencyID = reportStore.reportOverviews[reportID]?.agency_id;
        const agency = userStore.userAgencies?.find((a) => a.id === agencyID);
        trackReportNotStartedToDraft(reportID, agency);
      }
    } else {
      const body = await response.json();
      if (body.code === "version_conflict") {
        showToast(
          "Someone else has edited the report since you last opened it. Please refresh the page to view the latest changes and continue editing.",
          false,
          "red",
          -1
        );
        runInAction(() => {
          setHasVersionConflict(true);
        });
      } else {
        showToast("Failed to save", false, "red");
      }
      trackAutosaveFailed(reportID);
    }
  };

  const debouncedSave = useRef(
    memoizeDebounce(saveUpdatedMetrics, 1500)
  ).current;

  /** Saves metrics before tab/window close or page refreshes */
  useEffect(
    () => {
      const saveBeforeExiting = (e: BeforeUnloadEvent) => {
        e.preventDefault();
        saveUpdatedMetrics();
      };

      window.addEventListener("beforeunload", saveBeforeExiting);
      return () =>
        window.removeEventListener("beforeunload", saveBeforeExiting);
    },
    // eslint-disable-next-line react-hooks/exhaustive-deps
    []
  );

  const reportOverview = reportStore.reportOverviews[reportID];
  const reportMetrics = reportStore.reportMetrics[reportID];

  if (!reportOverview || !reportMetrics) {
    return null;
  }

  return (
    <FormWrapper>
      <Form
        onChange={(e) => {
          // When the form has changed, check the changed element for a `data-metric-key`
          // data attribute. If present, pass to the `debouncedSave` function, which will
          // then only save that metric. If not present, `metricKey` will be undefined,
          // in which case `debouncedSave` will save all metrics.
          const target = e.target as HTMLFormElement;
          const metricKey = target.getAttribute("data-metric-key") ?? undefined;
          debouncedSave(metricKey);
        }}
      >
        {/* Form Title */}
        <OnePanelBackLinkContainer>
          <GoBackToReportsOverviewLink onClick={() => navigate("/")} />
        </OnePanelBackLinkContainer>
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
            <Metric
              key={metric.key}
              id={metric.key}
              ref={(e: HTMLDivElement) =>
                metric.enabled && metricsRef.current?.push(e)
              }
              notReporting={!metric.enabled}
            >
              <MetricSectionTitleWrapper>
                <MetricSectionTitle notReporting={!metric.enabled}>
                  {metric.display_name}
                </MetricSectionTitle>
                {!metric.enabled && <NotReportedIcon />}
              </MetricSectionTitleWrapper>
              <MetricSectionSubTitle>
                {metric.description}
              </MetricSectionSubTitle>

              {metric.enabled && (
                <>
                  {/* Metric Value */}
                  <MetricTextInput
                    reportID={reportID}
                    metric={metric}
                    autoFocus={index === 0}
                    disabled={isPublished || hasVersionConflict}
                  />

                  {/* Disaggregations */}
                  {metric.disaggregations.length > 0 && (
                    <TabbedDisaggregations
                      reportID={reportID}
                      currentIndex={index}
                      reportMetrics={reportMetrics}
                      metric={metric}
                      updateFieldDescription={updateFieldDescription}
                      disabled={isPublished || hasVersionConflict}
                    />
                  )}

                  {/* Contexts */}
                  {/* TODO(#13314): display multiple choice options as drop down if there are >2 options */}
                  {metric.contexts.length > 0 &&
                    metric.contexts.map((context, contextIndex) => {
                      if (context.type === "MULTIPLE_CHOICE") {
                        const contextError =
                          formStore.contexts?.[reportID]?.[metric.key]?.[
                            context.key
                          ]?.error;
                        return (
                          <BinaryRadioGroupContainer key={context.key}>
                            <BinaryRadioGroupQuestion>
                              {context.required && (
                                <DataEntryFormRequiredChip />
                              )}
                              {context.display_name}
                            </BinaryRadioGroupQuestion>

                            <BinaryRadioGroupWrapper>
                              <BinaryRadioButtonInputs
                                reportID={reportID}
                                metric={metric}
                                context={context}
                                contextIndex={contextIndex}
                                options={context.multiple_choice_options}
                                disabled={isPublished || hasVersionConflict}
                              />
                            </BinaryRadioGroupWrapper>
                            <BinaryRadioGroupClearButton
                              onClick={() => {
                                if (
                                  !isPublished &&
                                  !hasVersionConflict &&
                                  (formStore.contexts?.[reportID]?.[
                                    metric.key
                                  ]?.[context.key]?.value ||
                                    context.value)
                                ) {
                                  formStore.resetBinaryInput(
                                    reportID,
                                    metric.key,
                                    context.key,
                                    context.required
                                  );
                                  showToast(
                                    "Saving...",
                                    false,
                                    "grey",
                                    undefined,
                                    true
                                  );
                                  debouncedSave(metric.key);
                                }
                              }}
                              disabled={isPublished || hasVersionConflict}
                            >
                              Clear Input
                            </BinaryRadioGroupClearButton>

                            {/* Error */}
                            {contextError && (
                              <DataEntryFormErrorWithTooltipContainer>
                                <ErrorWithTooltip error={contextError} />
                              </DataEntryFormErrorWithTooltipContainer>
                            )}
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
                              updateFieldDescription(undefined)
                            }
                            disabled={isPublished || hasVersionConflict}
                          />
                        </Fragment>
                      );
                    })}
                </>
              )}
            </Metric>
          );
        })}
        <DataEntryFormPublishButtonContainer>
          <DataEntryFormPublishButton
            onClick={(e) => {
              /** Should trigger a confirmation dialogue before submitting */
              e.preventDefault();
              toggleConfirmationDialogue();
            }}
            isPublished={isPublished || hasVersionConflict}
          />
        </DataEntryFormPublishButtonContainer>
        <OpacityGradient />
      </Form>

      {/* Onboarding */}

      {userStore.onboardingTopicsCompleted?.dataentryview === false &&
        showOnboarding && (
          <Onboarding
            setShowOnboarding={setShowOnboarding}
            topic="dataentryview"
          />
        )}

      <OnboardingDataEntrySummary />
    </FormWrapper>
  );
};

export default observer(DataEntryForm);
