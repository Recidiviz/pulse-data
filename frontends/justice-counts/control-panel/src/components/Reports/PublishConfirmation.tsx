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

import { trackReportPublished } from "../../analytics";
import {
  MetricContextWithErrors,
  MetricDisaggregationDimensionsWithErrors,
  MetricDisaggregationsWithErrors,
  MetricWithErrors,
} from "../../shared/types";
import { useStore } from "../../stores";
import { printReportTitle, rem } from "../../utils";
import errorIcon from "../assets/status-error-icon.png";
import { Button } from "../Forms";
import { palette } from "../GlobalStyles";
import { showToast } from "../Toast";
import { PublishButton } from "./ReportDataEntry.styles";

const CONTAINER_WIDTH = 912;
const CONTAINER_HORIZONTAL_PADDING = 24;

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
  display: flex;
  flex-direction: column;
  align-items: center;
`;

const ConfirmHeaderContainer = styled.div`
  background: white;
  width: 100%;
  position: fixed;
  background: white;
  top: 0px;
  z-index: 1;
  padding-top: 88px;
  display: flex;
  flex-direction: column;
  align-items: center;
`;

const ConfirmHeader = styled.div`
  max-width: ${CONTAINER_WIDTH}px;
  padding: 0 ${CONTAINER_HORIZONTAL_PADDING}px;
`;

const ConfirmationDialogue = styled.div`
  max-width: ${CONTAINER_WIDTH}px;
  width: 100%;
  display: flex;
  flex-direction: column;
  text-align: left;
  padding: 0 ${CONTAINER_HORIZONTAL_PADDING}px;
  padding-top: 176px;
`;

const ConfirmationTitle = styled.div`
  font-size: ${rem("64px")};
  font-weight: 500;
  line-height: 64px;
  letter-spacing: -0.02em;
`;

const ConfirmationSubTitle = styled.div`
  flex: 1;
  margin-right: 16px;
  font-size: ${rem("15px")};
  line-height: 24px;
  font-weight: 500;
`;

const Metric = styled.div`
  display: flex;
  flex: 1 1 auto;
  justify-content: space-between;
  border-top: 2px solid ${palette.solid.darkgrey};
  margin-bottom: 40px;

  @media only screen and (max-width: ${CONTAINER_WIDTH +
    CONTAINER_HORIZONTAL_PADDING * 2}px) {
    flex-direction: column;
    border-top: 0;
  }
`;

const MetricOverviewWrapper = styled.div`
  flex: 0 1 330px;
  display: flex;
  flex-direction: column;
  justify-content: flex-start;

  @media only screen and (max-width: ${CONTAINER_WIDTH +
    CONTAINER_HORIZONTAL_PADDING * 2}px) {
    flex: 0 1 auto;
  }
`;

const MetricDetailWrapper = styled.div`
  flex: 0 1 534px;
  display: flex;
  flex-direction: column;
  justify-content: stretch;

  @media only screen and (max-width: ${CONTAINER_WIDTH +
    CONTAINER_HORIZONTAL_PADDING * 2}px) {
    flex: 0 1 auto;
  }
`;

const MetricValueLabel = styled.div`
  display: flex;
  justify-content: flex-start;
  padding-bottom: 16px;
  align-items: center;
  font-size: ${rem("18px")};
  line-height: 24px;
  font-weight: 700;

  @media only screen and (max-width: ${CONTAINER_WIDTH +
    CONTAINER_HORIZONTAL_PADDING * 2}px) {
    border-bottom: 1px solid ${palette.solid.darkgrey};
  }
`;

const MetricValue = styled.div<{ missing?: boolean; error?: boolean }>`
  margin-top: 8px;
  font-size: ${rem("32px")};
  line-height: 1.5;
  letter-spacing: -0.01em;
  color: ${({ missing, error }) =>
    error ? palette.solid.red : missing && palette.highlight.grey8};
`;

const Breakdown = styled.div<{ missing?: boolean }>`
  display: flex;
  justify-content: space-between;
  align-items: stretch;
  border-bottom: 1px dashed ${palette.solid.darkgrey};
  padding: 4px 0;
  font-size: ${rem("15px")};
  line-height: 24px;
  font-weight: 500;
`;

const DisaggregationBreakdown = styled(Breakdown)`
  &:first-child {
    padding-top: 0;
  }

  &:last-child {
    margin-bottom: 24px;
  }
`;

const BreakdownLabel = styled.div`
  display: flex;
  flex: 1;
`;

const BreakdownValue = styled.div<{ missing?: boolean; error?: boolean }>`
  display: flex;
  flex: 1;
  justify-content: flex-end;
  font-style: ${({ missing }) => missing && "italic"};
  color: ${({ missing, error }) =>
    error ? palette.solid.red : missing && palette.highlight.grey8};
`;

const ContextContainer = styled(Breakdown)<{ verticalOnly?: boolean }>`
  border-bottom: 1px solid ${palette.solid.darkgrey};
  padding: 8px 0;

  flex-direction: ${({ verticalOnly }) => (verticalOnly ? "column" : "row")};

  @media only screen and (max-width: ${CONTAINER_WIDTH +
    CONTAINER_HORIZONTAL_PADDING * 2}px) {
    flex-direction: column;
  }
`;

const ContextTitle = styled(BreakdownLabel)`
  font-weight: 700;
  display: flex;
  flex-direction: row;
  justify-content: space-between;
  align-items: flex-start;
`;

const ContextValue = styled(BreakdownValue)`
  justify-content: flex-start;
`;

const ButtonContainer = styled.div`
  padding: 16px 0;
  display: flex;
  flex: 1 1 auto;
  align-items: center;
  justify-content: space-between;
  border-bottom: 2px solid ${palette.solid.darkgrey};

  @media only screen and (max-width: ${CONTAINER_WIDTH +
    CONTAINER_HORIZONTAL_PADDING * 2}px) {
    border-bottom: 1px solid ${palette.solid.darkgrey};
  }
`;

const PublishConfirmButton = styled(Button)`
  margin-right: 8px;
  flex: 1;
`;

const TopPublishConfirmButton = styled(PublishConfirmButton)`
  flex: 0 0 207px;

  @media only screen and (max-width: ${CONTAINER_WIDTH +
    CONTAINER_HORIZONTAL_PADDING * 2}px) {
    display: none;
  }
`;

const PublishConfirmPublishButton = styled(PublishButton)`
  flex: 1;

  &::after {
    content: "Publish Data";
  }
`;

const TopPublishConfirmPublishButton = styled(PublishConfirmPublishButton)`
  flex: 0 0 207px;

  @media only screen and (max-width: ${CONTAINER_WIDTH +
    CONTAINER_HORIZONTAL_PADDING * 2}px) {
    display: none;
  }
`;

const DisaggregationContainer = styled.div`
  font-weight: 700;
  font-size: ${rem("15px")};
  margin-top: 16px;
  border-bottom: 1px solid ${palette.solid.darkgrey};
`;

const DisaggregationTitleContainer = styled.div`
  display: flex;
  flex-direction: row;
  justify-content: space-between;
  align-items: flex-start;
  margin-bottom: 16px;
`;

const LabelLeft = styled.div`
  display: inline;
`;

const BreakdownContainer = styled.div`
  padding-left: 20px;
  margin-top: 8px;
  border-left: 1px solid ${palette.solid.darkgrey};
`;

const DropdownArrowContainer = styled.div<{ collapsed?: boolean }>`
  width: 16px;
  height: 16px;
  border-radius: 8px;
  background: ${({ collapsed }) =>
    collapsed ? palette.highlight.grey2 : palette.highlight.lightblue2};
  display: flex;
  align-items: center;
  justify-content: center;
  margin-left: 4px;
  flex-shrink: 0;

  &:hover {
    cursor: pointer;
  }
`;

const DropdownArrow = styled.div<{ collapsed?: boolean }>`
  width: 6px;
  height: 6px;
  border: none;
  border-bottom: 2px solid
    ${({ collapsed }) => (collapsed ? palette.solid.grey : palette.solid.blue)};
  border-right: 2px solid
    ${({ collapsed }) => (collapsed ? palette.solid.grey : palette.solid.blue)};
  transform: rotate(45deg) translate(-1px, -1px);
  transform: rotate(${({ collapsed }) => (collapsed ? 225 : 45)}deg)
    translate(-1px, -1px);
`;

const ContextDropdownArrowContainer = styled(DropdownArrowContainer)`
  margin-top: 3px;
`;

const ErrorImg = styled.img`
  margin-left: 4px;
  width: 16px;
  height: 16px;
`;

const InnerErrorImg = styled(ErrorImg)`
  transform: translate(0, 2px);
`;

const MobileButtonContainer = styled.div`
  background: ${palette.solid.white};
  position: fixed;
  bottom: 0;
  display: none;
  justify-content: space-between;
  width: 100%;
  padding: 0 ${CONTAINER_HORIZONTAL_PADDING}px 8px;

  @media only screen and (max-width: ${CONTAINER_WIDTH +
    CONTAINER_HORIZONTAL_PADDING * 2}px) {
    display: flex;
  }
`;

const Disaggregation: React.FC<{
  disaggregation: MetricDisaggregationsWithErrors;
}> = ({ disaggregation }) => {
  const [collapsed, setCollapsed] = React.useState<boolean>(false);
  const { display_name: displayName, dimensions } = disaggregation;
  const hasError = !!dimensions.find(
    (dim: MetricDisaggregationDimensionsWithErrors) => dim.error
  );
  return (
    <DisaggregationContainer>
      <DisaggregationTitleContainer>
        <LabelLeft>
          {displayName}
          {hasError && <InnerErrorImg src={errorIcon} alt="" />}
        </LabelLeft>
        <DropdownArrowContainer
          collapsed={collapsed}
          onClick={() => setCollapsed(!collapsed)}
        >
          <DropdownArrow collapsed={collapsed} />
        </DropdownArrowContainer>
      </DisaggregationTitleContainer>
      {dimensions.length > 0 && !collapsed && (
        <BreakdownContainer>
          {dimensions.map(
            (dimension: MetricDisaggregationDimensionsWithErrors) => {
              return (
                <Fragment key={dimension.key}>
                  <DisaggregationBreakdown missing={!dimension.value}>
                    <BreakdownLabel>{dimension.label}</BreakdownLabel>
                    <BreakdownValue
                      missing={!dimension.value}
                      error={!!dimension.error}
                    >
                      {dimension.value?.toLocaleString("en-US") ||
                        "Not Reported"}
                    </BreakdownValue>
                  </DisaggregationBreakdown>
                </Fragment>
              );
            }
          )}
        </BreakdownContainer>
      )}
    </DisaggregationContainer>
  );
};

const Context: React.FC<{ context: MetricContextWithErrors }> = ({
  context,
}) => {
  const [collapsed, setCollapsed] = React.useState<boolean>(false);
  const hasError = !!context.error;
  return (
    <ContextContainer verticalOnly>
      <ContextTitle>
        <LabelLeft>
          {context.display_name}
          {hasError && <InnerErrorImg src={errorIcon} alt="" />}
        </LabelLeft>
        <ContextDropdownArrowContainer
          collapsed={collapsed}
          onClick={() => setCollapsed(!collapsed)}
        >
          <DropdownArrow collapsed={collapsed} />
        </ContextDropdownArrowContainer>
      </ContextTitle>
      {!collapsed && (
        <ContextValue missing={!context.value} error={!!context.error}>
          {context.value?.toLocaleString("en-US") || "Not Reported"}
        </ContextValue>
      )}
    </ContextContainer>
  );
};

const MetricsDisplay: React.FC<{
  metric: MetricWithErrors;
}> = ({ metric }) => {
  const hasError = !!metric.error;
  return (
    <Metric>
      <MetricOverviewWrapper>
        {/* Overall Metric Value */}
        <MetricValue missing={!metric.value} error={hasError}>
          {metric.value?.toLocaleString("en-US") || "Not Reported"}
        </MetricValue>
        <MetricValueLabel>
          {metric.label}
          {hasError && <ErrorImg src={errorIcon} alt="" />}
        </MetricValueLabel>
      </MetricOverviewWrapper>

      <MetricDetailWrapper>
        {/* Disaggregations > Dimensions */}
        {metric.disaggregations.length > 0 &&
          metric.disaggregations.map((disaggregation) => {
            return (
              <Disaggregation
                key={disaggregation.key}
                disaggregation={disaggregation}
              />
            );
          })}

        {/* Contexts */}
        {metric.contexts.length > 0 &&
          metric.contexts.map((context) => {
            return <Context key={context.key} context={context} />;
          })}
      </MetricDetailWrapper>
    </Metric>
  );
};

const PublishConfirmation: React.FC<{
  toggleConfirmationDialogue: () => void;
  reportID: number;
}> = ({ toggleConfirmationDialogue, reportID }) => {
  const [isPublishable, setIsPublishable] = useState(false);
  const [metricsPreview, setMetricsPreview] = useState<MetricWithErrors[]>();
  const { formStore, reportStore, userStore } = useStore();
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
        const agencyID = reportStore.reportOverviews[reportID]?.agency_id;
        const agency = userStore.userAgencies?.find((a) => a.id === agencyID);
        trackReportPublished(reportID, finalMetricsToPublish, agency);
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
      <ConfirmHeaderContainer>
        <ConfirmHeader>
          <ConfirmationTitle>Review</ConfirmationTitle>
          <ButtonContainer>
            <ConfirmationSubTitle>
              Take a moment to review the numbers that will be published. Click
              the arrows to show the data for any disaggregations.
            </ConfirmationSubTitle>
            <TopPublishConfirmButton onClick={toggleConfirmationDialogue}>
              Back to Data Entry
            </TopPublishConfirmButton>
            <TopPublishConfirmPublishButton
              onClick={publishReport}
              disabled={!isPublishable}
            />
          </ButtonContainer>
        </ConfirmHeader>
      </ConfirmHeaderContainer>
      <ConfirmationDialogue>
        {metricsPreview &&
          metricsPreview.map((metric) => {
            return <MetricsDisplay key={metric.key} metric={metric} />;
          })}
      </ConfirmationDialogue>
      <MobileButtonContainer>
        <PublishConfirmButton onClick={toggleConfirmationDialogue}>
          Back to Data Entry
        </PublishConfirmButton>
        <PublishConfirmPublishButton
          onClick={publishReport}
          disabled={!isPublishable}
        />
      </MobileButtonContainer>
    </ConfirmationDialogueWrapper>
  );
};

export default observer(PublishConfirmation);
