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
import { Report } from "../../shared/types";
import { useStore } from "../../stores";
import {
  printCommaSeparatedList,
  printElapsedDaysSinceDate,
} from "../../utils";
import {
  EditDetails,
  EditDetailsContent,
  EditDetailsTitle,
  PublishButton,
  PublishDataWrapper,
  Title,
} from "../Forms";
import { palette } from "../GlobalStyles";
import PublishConfirmation from "./PublishConfirmation";

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

const PublishDataPanel: React.FC<{ reportID: number }> = ({ reportID }) => {
  const [showConfirmation, setShowConfirmation] = useState(false);
  const [tempFinalObject, setTempFinalObject] = useState({}); // Temporarily Displaying Final Object For Testing Purposes
  const { formStore, reportStore, userStore } = useStore();
  const { editors, last_modified_at: lastModifiedAt } =
    reportStore.reportOverviews[reportID];

  const toggleConfirmationDialogue = () =>
    setShowConfirmation(!showConfirmation);

  const saveUpdatedMetrics = () => {
    const updatedMetrics = formStore.reportUpdatedValuesForBackend(reportID);
    reportStore.updateReport(reportID, updatedMetrics, "DRAFT");
  };

  return (
    <>
      <PublishDataWrapper>
        <TempSaveButton onClick={saveUpdatedMetrics}>Save</TempSaveButton>
        <Title>
          <PublishButton
            onClick={() => {
              /** Should trigger a confirmation dialogue before submitting */
              toggleConfirmationDialogue();
              setTempFinalObject(formStore.submitReport(reportID)); // Temporarily Displaying Final Object For Testing Purposes
            }}
          >
            Publish Data
          </PublishButton>
        </Title>

        <EditDetails>
          <EditDetailsTitle>Editors</EditDetailsTitle>
          <EditDetailsContent>
            {editors.length ? printCommaSeparatedList(editors) : userStore.name}
          </EditDetailsContent>

          <EditDetailsTitle>Details</EditDetailsTitle>

          <EditDetailsContent>
            {editors.length
              ? `Last modified ${
                  lastModifiedAt && printElapsedDaysSinceDate(lastModifiedAt)
                } by ${editors[editors.length - 1]}`
              : `Created ${
                  (lastModifiedAt &&
                    printElapsedDaysSinceDate(lastModifiedAt)) ||
                  "today"
                } by ${userStore.name}`}
          </EditDetailsContent>
        </EditDetails>
      </PublishDataWrapper>
      {showConfirmation && (
        <PublishConfirmation
          toggleConfirmationDialogue={toggleConfirmationDialogue}
          tempFinalObject={tempFinalObject as Report}
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
