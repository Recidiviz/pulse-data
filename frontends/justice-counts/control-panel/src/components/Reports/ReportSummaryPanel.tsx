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
  printElapsedDaysMonthsYearsSinceDate,
} from "../../utils";
// import successIcon from "../assets/status-check-icon.png";
import errorIcon from "../assets/status-error-icon.png";
import { GoBackToReportsOverviewLink, PreTitle, Title } from "../Forms";
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

export const ReportSummaryProgressIndicatorWrapper = styled.div`
  margin-top: 28px;
`;

const ReportSummarySection = styled.a`
  ${typography.sizeCSS.normal}
  height: 24px;
  display: flex;
  align-items: center;
  justify-content: flex-start;
  position: relative;
  text-decoration: none;
  margin-bottom: 2px;
  border-radius: 2px;
  color: ${palette.highlight.grey8};
  transition: 0.2s ease;

  &:hover {
    cursor: pointer;
    color: ${palette.solid.darkgrey};
  }
`;

const MetricDisplayName = styled.div<{
  activeSection?: boolean;
}>`
  ${({ activeSection }) =>
    activeSection && `color: ${palette.solid.darkgrey};`};
  border-bottom: 2px solid
    ${({ activeSection }) =>
      activeSection ? palette.solid.blue : `transparent`};
  max-width: 238px;
  white-space: nowrap;
  overflow: hidden;
  text-overflow: ellipsis;
`;

const ReportStatusIcon = styled.div<{
  metricHasError?: boolean;
  metricHasEntries?: boolean;
}>`
  width: 16px;
  height: 16px;
  display: flex;
  justify-content: center;
  align-items: center;
  border-radius: 100%;
  margin-right: 8px;

  background: ${({ metricHasError }) => {
    if (metricHasError) {
      return palette.solid.red;
    }

    return `transparent`;
  }};
  color: white;
  border: 1px solid ${palette.highlight.grey4};
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
  activeMetric: string;
  metric: Metric;
}> = ({ metricHasError, metric, activeMetric }) => {
  return (
    <ReportSummarySection
      onClick={() => {
        document
          .getElementById(metric.key)
          ?.scrollIntoView({ behavior: "smooth" });
      }}
    >
      <ReportStatusIcon metricHasError={metricHasError}>
        {/* Error State */}
        {metricHasError && (
          <img src={errorIcon} alt="" width="16px" height="16px" />
        )}

        {/* Validated State [Placeholder] */}
        {/* {!metricHasError && (
          <img src={successIcon} alt="" width="16px" height="16px" />
        )} */}
      </ReportStatusIcon>
      <MetricDisplayName activeSection={metric.key === activeMetric}>
        {metric.display_name}
      </MetricDisplayName>
    </ReportSummarySection>
  );
};

const ReportSummaryPanel: React.FC<{
  reportID: number;
  activeMetric: string;
}> = ({ reportID, activeMetric }) => {
  const navigate = useNavigate();
  const { formStore, reportStore, userStore } = useStore();
  const {
    editors,
    last_modified_at: lastModifiedAt,
    month,
    year,
    frequency,
  } = reportStore.reportOverviews[reportID];

  const checkMetricForErrorsInUpdatedValues = (metricKey: string) => {
    let foundErrors = false;

    if (formStore.metricsValues[reportID]?.[metricKey]?.error) {
      foundErrors = true;
    }

    if (formStore.disaggregations[reportID]?.[metricKey]) {
      Object.values(formStore.disaggregations[reportID][metricKey]).forEach(
        (disaggregation) => {
          Object.values(disaggregation).forEach((dimension) => {
            if (dimension.error) {
              foundErrors = true;
            }
          });
        }
      );
    }

    if (formStore.contexts[reportID]?.[metricKey]) {
      Object.values(formStore.contexts[reportID][metricKey]).forEach(
        (context) => {
          if (context.error) {
            foundErrors = true;
          }
        }
      );
    }

    return foundErrors;
  };

  return (
    <ReportSummaryWrapper>
      <PreTitle>
        <GoBackToReportsOverviewLink onClick={() => navigate("/")} />
      </PreTitle>
      <Title>Report Summary</Title>

      <ReportSummaryProgressIndicatorWrapper>
        {reportStore.reportMetrics[reportID].map((metric) => {
          const foundErrors = checkMetricForErrorsInUpdatedValues(metric.key);

          return (
            <ReportStatusIconComponent
              key={metric.key}
              activeMetric={activeMetric}
              metricHasError={foundErrors}
              metric={metric}
            />
          );
        })}
      </ReportSummaryProgressIndicatorWrapper>

      <EditDetails>
        <EditDetailsTitle>Date Range</EditDetailsTitle>
        <EditDetailsContent>
          {printDateRangeFromMonthYear(month, year, frequency)}
        </EditDetailsContent>

        <EditDetailsTitle>Editors</EditDetailsTitle>
        <EditDetailsContent>
          {editors.length
            ? printCommaSeparatedList(editors)
            : userStore.nameOrEmail}
        </EditDetailsContent>

        <EditDetailsTitle>Details</EditDetailsTitle>
        <EditDetailsContent>
          {editors.length === 1 &&
            !lastModifiedAt &&
            `Created today by ${editors[0]}`}

          {editors.length >= 1 &&
            lastModifiedAt &&
            `Last modified ${printElapsedDaysMonthsYearsSinceDate(
              lastModifiedAt
            )} by ${editors[editors.length - 1]}`}

          {!editors.length && ``}
        </EditDetailsContent>
      </EditDetails>
    </ReportSummaryWrapper>
  );
};

export default observer(ReportSummaryPanel);
