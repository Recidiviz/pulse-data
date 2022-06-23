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
import React, { Fragment, useEffect, useRef, useState } from "react";
import { useNavigate } from "react-router-dom";
import styled from "styled-components/macro";

import { useStore } from "../../stores";
import { memoizeDebounce, printReportTitle } from "../../utils";
import {
  BinaryRadioGroupClearButton,
  BinaryRadioGroupContainer,
  BinaryRadioGroupQuestion,
  BinaryRadioGroupWrapper,
  ErrorLabel,
  Form,
  GoBackToReportsOverviewLink,
  Metric,
  MetricSectionSubTitle,
  MetricSectionTitle,
  OnePanelBackLinkContainer,
  OpacityGradient,
  PreTitle,
  RequiredChip,
  TabbedDisaggregations,
  Title,
} from "../Forms";
import { palette, typography } from "../GlobalStyles";
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

const DataEntryFormErrorLabel = styled(ErrorLabel)`
  top: 140px;
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
  const [scrolled, setScrolled] = useState(false);
  const metricsRef = useRef<HTMLDivElement[]>([]);
  const { formStore, reportStore } = useStore();
  const navigate = useNavigate();

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
    const status =
      reportStore.reportOverviews[reportID].status === "PUBLISHED"
        ? "PUBLISHED"
        : "DRAFT";

    showToast("Saving...", false, "grey", -1, true);
    const response = (await reportStore.updateReport(
      reportID,
      updatedMetrics,
      status
    )) as Response;

    if (response.status === 200) {
      showToast("Saved", false, "grey");
    } else {
      showToast("Failed to save", false, "red");
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
            ref={(e: HTMLDivElement) => metricsRef.current?.push(e)}
          >
            <MetricSectionTitle>{metric.display_name}</MetricSectionTitle>
            <MetricSectionSubTitle>{metric.description}</MetricSectionSubTitle>

            {/* Metric Value */}
            <MetricTextInput
              reportID={reportID}
              metric={metric}
              autoFocus={index === 0}
            />

            {/* Disaggregations */}
            {metric.disaggregations.length > 0 && (
              <TabbedDisaggregations
                reportID={reportID}
                currentIndex={index}
                reportMetrics={reportMetrics}
                metric={metric}
                updateFieldDescription={updateFieldDescription}
              />
            )}

            {/* Contexts */}
            {/* TODO(#13314): display multiple choice options as drop down if there are >2 options */}
            {metric.contexts.length > 0 &&
              metric.contexts.map((context, contextIndex) => {
                if (context.type === "MULTIPLE_CHOICE") {
                  return (
                    <BinaryRadioGroupContainer key={context.key}>
                      <BinaryRadioGroupQuestion>
                        {context.required && <DataEntryFormRequiredChip />}
                        {context.display_name}
                      </BinaryRadioGroupQuestion>

                      <BinaryRadioGroupWrapper>
                        <BinaryRadioButtonInputs
                          reportID={reportID}
                          metric={metric}
                          context={context}
                          contextIndex={contextIndex}
                          options={context.multiple_choice_options}
                        />
                      </BinaryRadioGroupWrapper>
                      <BinaryRadioGroupClearButton
                        onClick={() => {
                          if (
                            formStore.contexts?.[reportID]?.[metric.key]?.[
                              context.key
                            ]?.value ||
                            context.value
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
                      >
                        Clear Input
                      </BinaryRadioGroupClearButton>

                      {/* Error */}
                      {formStore.contexts?.[reportID]?.[metric.key]?.[
                        context.key
                      ]?.error && (
                        <DataEntryFormErrorLabel
                          error={
                            formStore.contexts?.[reportID]?.[metric.key]?.[
                              context.key
                            ]?.error
                          }
                        >
                          {
                            formStore.contexts?.[reportID]?.[metric.key]?.[
                              context.key
                            ]?.error
                          }
                        </DataEntryFormErrorLabel>
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
                    />
                  </Fragment>
                );
              })}
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
        />
      </DataEntryFormPublishButtonContainer>
      <OpacityGradient />
    </Form>
  );
};

export default observer(DataEntryForm);
