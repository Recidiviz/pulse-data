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
import { rem } from "../../utils";
import checkmark from "../assets/checkmark.png";
import xmark from "../assets/xmark.png";
import {
  GoBackLink,
  PreTitle,
  ReportSummaryProgressIndicatorWrapper,
  ReportSummaryWrapper,
  Title,
} from "../Forms";
import { palette } from "../GlobalStyles";

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
  const { formStore, reportStore } = useStore();

  return (
    <ReportSummaryWrapper>
      <PreTitle>
        <GoBackLink onClick={() => navigate("/")}>← Go to list view</GoBackLink>
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
    </ReportSummaryWrapper>
  );
};

export default observer(ReportSummaryPanel);
