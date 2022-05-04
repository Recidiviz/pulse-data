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

import { Report } from "../../shared/types";
import { useStore } from "../../stores";
import { printCommaSeparatedList } from "../../utils";
import {
  EditDetails,
  EditDetailsContent,
  EditDetailsTitle,
  PublishButton,
  PublishDataWrapper,
  Title,
} from "../Forms";
import PublishConfirmation from "./PublishConfirmation";

const PublishDataPanel: React.FC<{ reportID: number }> = ({ reportID }) => {
  const [showConfirmation, setShowConfirmation] = useState(false);
  const [tempFinalObject, setTempFinalObject] = useState({}); // Temporarily Displaying Final Object For Testing Purposes
  const { formStore, reportStore } = useStore();

  const toggleConfirmationDialogue = () =>
    setShowConfirmation(!showConfirmation);

  return (
    <>
      <PublishDataWrapper>
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
            {printCommaSeparatedList(
              reportStore.reportOverviews[0]?.editors || [""]
            )}
          </EditDetailsContent>

          <EditDetailsTitle>Details</EditDetailsTitle>
          <EditDetailsContent>
            Created today by{" "}
            {reportStore.reportOverviews[0]?.editors?.[0] || ""}
          </EditDetailsContent>
        </EditDetails>

        {/* Temporarily Displaying Final Object For Testing Purposes */}
        <pre
          style={{
            width: 320,
            height: 500,
            position: "fixed",
            zIndex: 2,
            bottom: 20,
            right: 20,
            background: "white",
            overflow: "scroll",
            fontSize: 10,
            lineHeight: 2,
            border: "1px dashed black",
            padding: 10,
          }}
        >
          {JSON.stringify(tempFinalObject, null, 2)}
        </pre>
      </PublishDataWrapper>
      {showConfirmation && (
        <PublishConfirmation
          toggleConfirmationDialogue={toggleConfirmationDialogue}
          tempFinalObject={tempFinalObject as Report}
          submitReport={formStore.submitReport}
        />
      )}
    </>
  );
};

export default observer(PublishDataPanel);
