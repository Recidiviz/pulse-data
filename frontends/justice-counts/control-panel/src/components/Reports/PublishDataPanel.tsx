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
import React, { useState } from "react";
import styled from "styled-components/macro";

import PreviewDataObject from "../../mocks/PreviewDataObject";
import { useStore } from "../../stores";
import { Title } from "../Forms";
import { palette, typography } from "../GlobalStyles";
import PublishConfirmation from "./PublishConfirmation";

export const PublishDataWrapper = styled.div`
  width: 360px;
  position: fixed;
  top: 0;
  right: 0;
  z-index: 1;
  padding: 112px 24px 0 24px;
  height: 100%;
  background: ${palette.solid.white};
`;

export const PublishButton = styled.button<{ disabled?: boolean }>`
  ${typography.sizeCSS.medium}
  width: 315px;
  height: 56px;
  display: flex;
  justify-content: center;
  align-items: center;
  background: ${({ disabled }) => (disabled ? "none" : palette.solid.blue)};
  color: ${({ disabled }) =>
    disabled ? palette.highlight.grey8 : palette.solid.white};
  border: 1px solid
    ${({ disabled }) =>
      disabled ? palette.highlight.grey3 : palette.highlight.grey3};
  border-radius: 2px;
  transition: 0.2s ease;

  &:hover {
    cursor: pointer;
    background: ${({ disabled }) =>
      disabled ? "none" : palette.solid.darkblue};
  }
`;

const TempSaveButton = styled.button`
  position: absolute;
  top: 195px;
  right: 26px;
  background: none;
  border: none;
  color: ${palette.solid.blue};
  font-size: 1rem;
  transition: 0.2s ease;
  border-bottom: 1px solid transparent;

  &:hover {
    cursor: pointer;
    border-bottom: 1px solid ${palette.solid.blue};
  }

  &:active {
    transform: scale(0.97);
  }
`;

const FieldDescriptionTitle = styled.div`
  margin-bottom: 10px;
  color: ${palette.solid.darkgrey};
`;

const FieldDescriptionContainer = styled.div`
  ${typography.sizeCSS.normal}
  padding-top: 16px;
  color: ${palette.highlight.grey9};
`;

const PublishDataPanel: React.FC<{
  reportID: number;
  fieldDescription: { title: string; description: string };
}> = ({ reportID, fieldDescription }) => {
  const [showConfirmation, setShowConfirmation] = useState(false);
  const { formStore, reportStore } = useStore();

  const toggleConfirmationDialogue = () =>
    setShowConfirmation(!showConfirmation);

  const saveUpdatedMetrics = () => {
    const updatedMetrics = formStore.reportUpdatedValuesForBackend(reportID);
    reportStore.updateReport(reportID, updatedMetrics, "DRAFT");
  };

  return (
    <>
      <PublishDataWrapper>
        {/* Replace with autosave */}
        <TempSaveButton onClick={saveUpdatedMetrics}>Save</TempSaveButton>
        <Title>
          <PublishButton
            onClick={() => {
              /** Should trigger a confirmation dialogue before submitting */
              toggleConfirmationDialogue();
            }}
          >
            Publish Data (Review)
          </PublishButton>
        </Title>

        {/* Displays the description of the field currently hovered */}
        {fieldDescription.description && (
          <FieldDescriptionContainer>
            <FieldDescriptionTitle>
              {fieldDescription.title}
            </FieldDescriptionTitle>
            {fieldDescription.description}
          </FieldDescriptionContainer>
        )}
      </PublishDataWrapper>

      {showConfirmation && (
        <PublishConfirmation
          toggleConfirmationDialogue={toggleConfirmationDialogue}
          reportID={reportID}
        />
      )}

      <PreviewDataObject
        description="This is the request body that will be sent to the backend:"
        objectToDisplay={{
          status: "DRAFT",
          metrics: formStore.reportUpdatedValuesForBackend(reportID),
        }}
      />
    </>
  );
};

export default observer(PublishDataPanel);
