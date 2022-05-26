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
import { useNavigate } from "react-router-dom";
import styled from "styled-components/macro";

import { Metric } from "../../shared/types";
import { useStore } from "../../stores";
import {
  printCommaSeparatedList,
  printDateRangeFromMonthYear,
  printElapsedDaysSinceDate,
  rem,
} from "../../utils";
import checkmark from "../assets/status-check-icon.png";
import xmark from "../assets/status-error-icon.png";
import { GoBackLink, PreTitle, Title } from "../Forms";
import { palette, typography } from "../GlobalStyles";

export const ReportSummaryWrapper = styled.div`
  width: 355px;
  height: 100%;
  position: fixed;
  top: 0;
  left: 0;
  z-index: 1;
  padding: 96px 24px 0 24px;
  background: ${palette.solid.white};
`;

type ReportSummaryProgressIndicatorProps = {
  sectionStatus?: "completed" | "error";
};

export const ReportSummaryProgressIndicatorWrapper = styled.div`
  margin-top: 28px;
`;

export const ReportSummaryProgressIndicator = styled.div<ReportSummaryProgressIndicatorProps>`
  background: ${({ sectionStatus }) => {
    if (sectionStatus === "error") {
      return palette.highlight.red;
    }
    return sectionStatus === "completed"
      ? palette.highlight.lightblue1
      : undefined;
  }};

  height: 40px;
  display: flex;
  align-items: center;
  position: relative;
  padding-left: 33px;
  border-radius: 2px;
  font-size: ${rem("15px")};
  line-height: 24px;

  &:hover {
    cursor: pointer;
  }

  &:before {
    content: ${({ sectionStatus }) => {
      if (sectionStatus === "error") {
        return `"x"`;
      }
      return sectionStatus === "completed" ? `"✓"` : `"•"`;
    }};
    background: ${({ sectionStatus }) => {
      if (sectionStatus === "error") {
        return palette.solid.red;
      }
      return sectionStatus === "completed"
        ? palette.solid.blue
        : palette.highlight.grey8;
    }};

    width: 16px;
    height: 16px;
    display: flex;
    justify-content: center;
    align-items: center;
    position: absolute;
    top: 12px;
    left: 10px;
    border-radius: 100%;
    font-size: 8px;
    font-weight: 600;
    font-family: sans-serif;
    color: white;
  }
`;

const ReportSummarySection = styled.div<{
  activeSection?: boolean;
  metricHasError?: boolean;
}>`
  height: 40px;
  display: flex;
  align-items: center;
  position: relative;
  padding-left: 33px;
  border-radius: 2px;
  font-size: ${rem("15px")};
  line-height: 24px;
  background: ${({ activeSection, metricHasError }) => {
    if (activeSection && metricHasError) {
      return palette.highlight.red;
    }
    if (activeSection) {
      return palette.highlight.lightblue1;
    }
  }};

  &:hover {
    cursor: pointer;
    background: ${palette.highlight.grey1};
  }
`;

const ReportStatusIcon = styled.div<{
  activeSection?: boolean;
  metricHasError?: boolean;
  metricHasEntries?: boolean;
}>`
  width: 16px;
  height: 16px;
  display: flex;
  justify-content: center;
  align-items: center;
  position: absolute;
  top: 12px;
  left: 10px;
  border-radius: 100%;
  font-size: 8px;
  font-weight: 600;
  font-family: sans-serif;
  background: ${({ activeSection, metricHasError, metricHasEntries }) => {
    if (metricHasError) {
      return palette.solid.red;
    }
    if (!metricHasError && metricHasEntries) {
      return palette.solid.blue;
    }

    if (!metricHasEntries && activeSection) {
      return palette.highlight.lightblue2;
    }

    return palette.highlight.grey8;
  }};
  color: white;
`;

export const EditDetails = styled.div`
  width: 307px;
  position: fixed;
  bottom: 61px;
`;

export const EditDetailsTitle = styled.div`
  ${typography.sizeCSS.small}
  padding-top: 8px;
  border-top: 1px solid ${palette.solid.darkgrey};
`;

export const EditDetailsContent = styled.div`
  ${typography.sizeCSS.normal}
  color: ${palette.highlight.grey9};
  margin-bottom: 18px;
`;

const ReportStatusIconComponent: React.FC<{
  metricHasError: boolean;
  metricHasEntries: boolean;
  activeMetric: string;
  metric: Metric;
  updateActiveMetric: (metricKey: string) => void;
}> = ({
  metricHasEntries,
  metricHasError,
  metric,
  activeMetric,
  updateActiveMetric,
}) => {
  return (
    <ReportSummarySection
      onClick={() => updateActiveMetric(metric.key)}
      activeSection={metric.key === activeMetric}
      metricHasError={metricHasError}
    >
      <ReportStatusIcon
        metricHasError={metricHasError}
        activeSection={metric.key === activeMetric}
        metricHasEntries={metricHasEntries}
      >
        {/* Error State */}
        {metricHasError && <img src={xmark} alt="" width="6px" height="6px" />}

        {/* Validated State */}
        {!metricHasError && metricHasEntries && (
          <img src={checkmark} alt="" width="6px" height="5px" />
        )}

        {/* Default State (no entries) */}
        {!metricHasError && !metricHasEntries && "•"}
      </ReportStatusIcon>
      {metric.display_name}
    </ReportSummarySection>
  );
};

const ReportSummaryPanel: React.FC<{
  reportID: number;
  activeMetric: string;
  updateActiveMetric: (metricKey: string) => void;
}> = ({ reportID, activeMetric, updateActiveMetric }) => {
  const navigate = useNavigate();
  const { formStore, reportStore, userStore } = useStore();
  const {
    editors,
    last_modified_at: lastModifiedAt,
    month,
    year,
  } = reportStore.reportOverviews[reportID];

  return (
    <ReportSummaryWrapper>
      <PreTitle>
        <GoBackLink onClick={() => navigate("/")}>
          Back to Reports Overview
        </GoBackLink>
      </PreTitle>
      <Title>Report Summary</Title>

      <ReportSummaryProgressIndicatorWrapper>
        {reportStore.reportMetrics[reportID].map((metric, metricIndex) => {
          return (
            <ReportStatusIconComponent
              key={metric.key}
              updateActiveMetric={updateActiveMetric}
              activeMetric={activeMetric}
              metricHasError={Boolean(
                formStore.metricsValues[reportID]?.[metric.key]?.value
              )}
              /**
               * Need to figure out what this component should subscribe to in order to give
               * the default/no data entry styling state.
               *
               * We can't subscribe to the formStore metric/disaggregation/context objects as
               * those are newly created when values are entered during the session.
               *
               * Do we map through the reportStore metric to see if no data has been entered?
               *
               * Right now, it's subscribed to the top-level metric value (either in the temp
               * formStore object [that is populated from the current session]
               * or reportStore metric from the DB).
               */
              metricHasEntries={
                Boolean(
                  reportStore.reportMetrics[reportID][metricIndex].value
                ) || Boolean(formStore.metricsValues[reportID]?.[metric.key])
              }
              metric={metric}
            />
          );
        })}
      </ReportSummaryProgressIndicatorWrapper>

      <EditDetails>
        <EditDetailsTitle>Date Range</EditDetailsTitle>
        <EditDetailsContent>
          {printDateRangeFromMonthYear(month, year)}
        </EditDetailsContent>

        <EditDetailsTitle>Editors</EditDetailsTitle>
        <EditDetailsContent>
          {editors.length ? printCommaSeparatedList(editors) : userStore.name}
        </EditDetailsContent>

        <EditDetailsTitle>Details</EditDetailsTitle>
        <EditDetailsContent>
          {editors.length === 1 &&
            !lastModifiedAt &&
            `Created today by ${editors[0]}`}

          {editors.length >= 1 &&
            lastModifiedAt &&
            `Last modified ${printElapsedDaysSinceDate(lastModifiedAt)} by ${
              editors[editors.length - 1]
            }`}

          {!editors.length && ``}
        </EditDetailsContent>
      </EditDetails>
    </ReportSummaryWrapper>
  );
};

export default observer(ReportSummaryPanel);
