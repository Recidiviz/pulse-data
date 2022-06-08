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

import PreviewDataObject from "../../mocks/PreviewDataObject";
import { useStore } from "../../stores";
import { Title } from "../Forms";
import { palette } from "../GlobalStyles";
import {
  FieldDescription,
  FieldDescriptionProps,
  PublishButton,
  PublishDataWrapper,
} from "./ReportDataEntry.styles";

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

const PublishDataPanel: React.FC<{
  reportID: number;
  fieldDescription?: FieldDescriptionProps;
  toggleConfirmationDialogue: () => void;
}> = ({ reportID, fieldDescription, toggleConfirmationDialogue }) => {
  const { formStore, reportStore } = useStore();

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
        {fieldDescription && (
          <FieldDescription fieldDescription={fieldDescription} />
        )}
      </PublishDataWrapper>

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
