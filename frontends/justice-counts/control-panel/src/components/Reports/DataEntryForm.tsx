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

import debounce from "lodash.debounce";
import { observer } from "mobx-react-lite";
import React, { Fragment, useEffect, useRef, useState } from "react";

import { useStore } from "../../stores";
import { printReportTitle } from "../../utils";
import {
  AdditionalContextLabel,
  BinaryRadioGroupClearButton,
  BinaryRadioGroupContainer,
  BinaryRadioGroupQuestion,
  BinaryRadioGroupWrapper,
  ErrorLabel,
  Form,
  Metric,
  MetricSectionSubTitle,
  MetricSectionTitle,
  OpacityGradient,
  PreTitle,
  TabbedDisaggregations,
  Title,
} from "../Forms";
import { showToast } from "../Toast";
import {
  AdditionalContextInput,
  BinaryRadioButtonInputs,
  MetricTextInput,
} from "./DataEntryFormComponents";

const DataEntryForm: React.FC<{
  reportID: number;
  updateFieldDescription: (title: string, description: string) => void;
  updateActiveMetric: (metricKey: string) => void;
}> = ({ reportID, updateFieldDescription, updateActiveMetric }) => {
  const [scrolled, setScrolled] = useState(false);
  const metricsRef = useRef<HTMLDivElement[]>([]);
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

  useEffect(
    () => {
      const metricRefsToCleanUp = metricsRef.current;

      const observerOptions: IntersectionObserverInit = {
        root: null,
        rootMargin: "0px",
        threshold: 0.5,
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

  const saveUpdatedMetrics = async () => {
    const updatedMetrics = formStore.reportUpdatedValuesForBackend(reportID);
    const status =
      reportStore.reportOverviews[reportID].status === "PUBLISHED"
        ? "PUBLISHED"
        : "DRAFT";

    const response = (await reportStore.updateReport(
      reportID,
      updatedMetrics,
      status
    )) as Response;

    if (response.status === 200) {
      showToast("Saved", true);
    } else {
      showToast("Failed to save", false, true);
    }
  };

  const debouncedSave = useRef(debounce(saveUpdatedMetrics, 1500)).current;

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
      onChange={() => {
        showToast("Saving...", false, false, undefined, true);
        debouncedSave();
      }}
    >
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
              updateFieldDescription={() =>
                updateFieldDescription(metric.display_name, metric.description)
              }
              clearFieldDescription={() => updateFieldDescription("", "")}
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
                              false,
                              undefined,
                              true
                            );
                            debouncedSave();
                          }
                        }}
                      >
                        Clear Input
                      </BinaryRadioGroupClearButton>

                      {/* Error */}
                      {formStore.contexts?.[reportID]?.[metric.key]?.[
                        context.key
                      ]?.error && (
                        <ErrorLabel
                          error={
                            formStore.contexts?.[reportID]?.[metric.key]?.[
                              context.key
                            ]?.error
                          }
                          binaryContext
                        >
                          {
                            formStore.contexts?.[reportID]?.[metric.key]?.[
                              context.key
                            ]?.error
                          }
                        </ErrorLabel>
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
