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
import styled from "styled-components/macro";

import { Report } from "../../shared/types";
import { rem } from "../../utils";
import { Button, PublishButton } from "../Forms";
import { palette } from "../GlobalStyles";

const ConfirmationDialogue = styled.div`
  width: 100vw;
  height: 100vh;
  display: flex;
  justify-content: center;
  align-items: center;
  background: ${palette.solid.white};
  position: fixed;
  top: 0;
  left: 0;
  z-index: 2;
`;

const ConfirmationDialogueWrapper = styled.div`
  width: 676px;
  max-height: 881px;
  display: flex;
  flex-direction: column;
  padding: 10px 15px;
  text-align: left;
`;

const ConfirmationTitle = styled.div`
  font-size: ${rem("32px")};
  font-weight: 600;
  line-height: 48px;
`;

const ConfirmationSubTitle = styled.div`
  max-width: 524px;
  font-size: ${rem("20px")};
  font-weight: 500;
  line-height: 30px;
  margin-top: 16px;
  margin-bottom: 72px;
`;

const ConfirmationSection = styled.div`
  height: 400px;
  border-top: 1px solid ${palette.solid.darkgrey};
  border-bottom: 1px solid ${palette.solid.darkgrey};
`;

const ReportReview = styled.div`
  height: 495px;
  display: flex;
  flex-direction: column;
  overflow: scroll;
`;

const ReportItem = styled.div`
  display: flex;
  justify-content: space-between;
  padding: 16px 0;
  border-bottom: 1px dashed ${palette.solid.darkgrey};
`;

const ReportItemLabel = styled.div`
  transition: 0.2s;

  &:hover {
    cursor: pointer;
    color: ${palette.solid.blue};
  }
`;

const ReportItemValue = styled.div<{ missingValue?: boolean }>`
  color: ${({ missingValue }) => missingValue && palette.solid.red};
`;

const ButtonWrapper = styled.div`
  padding-top: 16px;
  display: flex;
  flex: 1 1 auto;
  justify-content: space-between;
`;

const PublishConfirmation: React.FC<{
  toggleConfirmationDialogue: () => void;
  submitReport: (reportID: number) => void;
  tempFinalObject: Report;
}> = ({ toggleConfirmationDialogue, submitReport, tempFinalObject }) => {
  return (
    <ConfirmationDialogue>
      <ConfirmationDialogueWrapper>
        <ConfirmationTitle>Review before Publishing</ConfirmationTitle>
        <ConfirmationSubTitle>
          Take a moment to review the answers for each of the fields before
          submitting the report.
        </ConfirmationSubTitle>

        <ConfirmationSection>
          <ReportReview>
            {tempFinalObject.metrics.map((metric) => {
              return (
                <ReportItem key={metric.key}>
                  <ReportItemLabel>{metric.label}</ReportItemLabel>
                  <ReportItemValue>{metric.value}</ReportItemValue>
                </ReportItem>
              );
            })}
          </ReportReview>
        </ConfirmationSection>

        <ButtonWrapper>
          <Button onClick={toggleConfirmationDialogue}>Cancel</Button>
          <PublishButton onClick={() => submitReport(0)}>
            Publish Data
          </PublishButton>
        </ButtonWrapper>
      </ConfirmationDialogueWrapper>
    </ConfirmationDialogue>
  );
};

export default observer(PublishConfirmation);
