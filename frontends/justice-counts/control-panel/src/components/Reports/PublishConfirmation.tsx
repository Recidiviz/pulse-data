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
import React, { Fragment, useEffect, useState } from "react";
import { useNavigate } from "react-router-dom";
import styled from "styled-components/macro";

import { MetricWithErrors } from "../../shared/types";
import { useStore } from "../../stores";
import { printReportTitle, rem } from "../../utils";
import { Button } from "../Forms";
import { palette } from "../GlobalStyles";
import { showToast } from "../Toast";
import { PublishButton } from "./PublishDataPanel";

const ConfirmationDialogueWrapper = styled.div`
  width: 100vw;
  height: 100vh;
  background: ${palette.solid.white};
  position: fixed;
  top: 0;
  left: 0;
  z-index: 2;
  padding: 80px 0;
  overflow: scroll;
`;

const ConfirmationDialogue = styled.div`
  width: 865px;
  display: flex;
  flex-direction: column;
  text-align: left;
  margin: 0 auto;
`;

const ConfirmationTitle = styled.div`
  font-size: ${rem("64px")};
  font-weight: 500;
  line-height: 64px;
  letter-spacing: -0.02em;
`;

const ConfirmationSubTitle = styled.div`
  width: 314px;
  height: 48px;

  font-size: ${rem("15px")};
  line-height: 24px;
  font-weight: 500;
  margin: 24px 0;
`;

const MetricsRow = styled.div`
  display: flex;
  justify-content: space-between;
  border-top: 2px solid ${palette.solid.darkgrey};
  margin-bottom: 40px;
`;

const Metric = styled.div`
  display: flex;
  flex: 1 1 auto;
  justify-content: space-between;
  padding-top: 16px;
`;

const MetricSectionWrapper = styled.div`
  flex: 0 1 424px;
  display: flex;
  flex-direction: column;
  justify-content: center;

  padding-top: 16px;
`;

const MetricValueLabel = styled.div`
  display: flex;
  justify-content: space-between;
  padding-bottom: 8px;
  font-size: ${rem("15px")};
  line-height: 24px;
  font-weight: 700;
`;

const MetricValue = styled.div<{ missingValue?: boolean }>`
  height: 64px;
  font-size: ${rem("64px")};
  line-height: 64px;
  letter-spacing: -0.01em;
  margin-bottom: 8px;
  color: ${({ missingValue }) => missingValue && palette.solid.red};
`;

const Breakdown = styled.div`
  display: flex;
  justify-content: space-between;
  align-items: center;
  border-bottom: 1px dashed ${palette.solid.darkgrey};
  padding: 8px 0 4px 0;
  font-size: ${rem("15px")};
  line-height: 24px;
  font-weight: 500;
`;

const BreakdownLabel = styled.div`
  display: flex;
  flex: 1;
`;

const BreakdownValue = styled.div`
  display: flex;
  flex: 1;
  justify-content: flex-end;
`;

const ButtonWrapper = styled.div`
  padding-top: 16px;
  display: flex;
  flex: 1 1 auto;
  align-items: center;
  justify-content: space-between;
`;

const EmptyValue = styled.div`
  width: 24px;
  height: 24px;
  background: ${palette.solid.orange};
`;

const IncompleteChip = styled.div`
  background: ${palette.solid.orange};
  padding: 4px 8px;
  font-size: ${rem("12px")};
  line-height: 16px;
  font-weight: 600;
  color: ${palette.solid.white};
`;

const MetricsDisplay: React.FC<{
  metric: MetricWithErrors;
}> = ({ metric }) => {
  const [metricHasError, setMetricHasError] = useState(false);
  const flagMetricHasError = () => setMetricHasError(true);

  if (metric.error && !metricHasError) {
    flagMetricHasError();
  }

  return (
    <Metric>
      <MetricSectionWrapper>
        {/* Overall Metric Value */}
        <MetricValue>{metric.value?.toLocaleString("en-US")}</MetricValue>
        <MetricValueLabel>
          {metric.label}
          {metricHasError && <IncompleteChip>Incomplete</IncompleteChip>}
        </MetricValueLabel>
      </MetricSectionWrapper>

      <MetricSectionWrapper>
        {/* Disaggregations > Dimensions */}
        {metric.disaggregations.length > 0 &&
          metric.disaggregations.map((disaggregation) => {
            return (
              disaggregation.dimensions.length > 0 &&
              disaggregation.dimensions.map((dimension) => {
                if (
                  disaggregation.required &&
                  !dimension.value &&
                  !metricHasError
                )
                  flagMetricHasError();

                return (
                  <Fragment key={dimension.key}>
                    <Breakdown>
                      <BreakdownLabel>{dimension.label}</BreakdownLabel>
                      <BreakdownValue>
                        {dimension.value?.toLocaleString("en-US") || (
                          <EmptyValue />
                        )}
                      </BreakdownValue>
                    </Breakdown>
                  </Fragment>
                );
              })
            );
          })}

        {/* Contexts */}
        {metric.contexts.length > 0 &&
          metric.contexts.map((context) => {
            if (context.required && !context.value && !metricHasError)
              flagMetricHasError();
            return (
              <Fragment key={context.key}>
                <Breakdown>
                  <BreakdownLabel>{context.display_name}</BreakdownLabel>
                  <BreakdownValue>
                    {context.value?.toLocaleString("en-US") || <EmptyValue />}
                  </BreakdownValue>
                </Breakdown>
              </Fragment>
            );
          })}
      </MetricSectionWrapper>
    </Metric>
  );
};

const PublishConfirmation: React.FC<{
  toggleConfirmationDialogue: () => void;
  reportID: number;
}> = ({ toggleConfirmationDialogue, reportID }) => {
  const [isPublishable, setIsPublishable] = useState(false);
  const [metricsPreview, setMetricsPreview] = useState<MetricWithErrors[]>();
  const { formStore, reportStore } = useStore();
  const navigate = useNavigate();

  const publishReport = async () => {
    if (isPublishable) {
      setIsPublishable(false);

      const finalMetricsToPublish =
        formStore.reportUpdatedValuesForBackend(reportID);

      const response = (await reportStore.updateReport(
        reportID,
        finalMetricsToPublish,
        "PUBLISHED"
      )) as Response;

      if (response.status === 200) {
        navigate("/");
        showToast(
          `Congratulations! You published the ${printReportTitle(
            reportStore.reportOverviews[reportID].month,
            reportStore.reportOverviews[reportID].year,
            reportStore.reportOverviews[reportID].frequency
          )} report!`,
          true
        );
      } else {
        showToast(
          `Something went wrong publishing the ${printReportTitle(
            reportStore.reportOverviews[reportID].month,
            reportStore.reportOverviews[reportID].year,
            reportStore.reportOverviews[reportID].frequency
          )} report!`,
          false
        );
        setIsPublishable(true);
      }
    }
  };

  useEffect(() => {
    const { metrics, isPublishable: publishable } =
      formStore.validateAndGetAllMetricFormValues(reportID);
    setMetricsPreview(metrics);
    setIsPublishable(publishable);
  }, [formStore, reportID]);

  /** Prevent body from scrolling when this dialog is open */
  useEffect(() => {
    document.body.style.overflow = "hidden";
    return () => {
      document.body.style.overflow = "unset";
    };
  }, []);

  return (
    <ConfirmationDialogueWrapper>
      <ConfirmationDialogue>
        <ConfirmationTitle>Overview</ConfirmationTitle>
        <ButtonWrapper>
          <ConfirmationSubTitle>
            Take a moment to review the numbers that will be published.
          </ConfirmationSubTitle>

          <PublishButton onClick={publishReport} disabled={!isPublishable}>
            Publish Data
          </PublishButton>
        </ButtonWrapper>

        {metricsPreview &&
          metricsPreview.map((metric) => {
            return (
              <MetricsRow key={metric.key}>
                <MetricsDisplay metric={metric} />
              </MetricsRow>
            );
          })}

        <Button onClick={toggleConfirmationDialogue}>Cancel</Button>
      </ConfirmationDialogue>
    </ConfirmationDialogueWrapper>
  );
};

export default observer(PublishConfirmation);
