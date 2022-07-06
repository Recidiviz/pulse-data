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

import { Title } from "../Forms";
import HelperText from "./HelperText";
import {
  FieldDescription,
  FieldDescriptionProps,
  PublishButton,
  PublishDataWrapper,
} from "./ReportDataEntry.styles";

const PublishDataPanel: React.FC<{
  reportID: number;
  activeMetric: string;
  fieldDescription?: FieldDescriptionProps;
  toggleConfirmationDialogue: () => void;
  isPublished?: boolean;
}> = ({
  reportID,
  activeMetric,
  fieldDescription,
  toggleConfirmationDialogue,
  isPublished,
}) => {
  return (
    <>
      <PublishDataWrapper>
        <Title>
          <PublishButton
            isPublished={isPublished}
            onClick={() => toggleConfirmationDialogue()}
          />
        </Title>

        {/* Metric Description, Definitions and Reporting Notes */}
        <HelperText reportID={reportID} activeMetric={activeMetric} />

        {/* Displays the description of the field currently focused */}
        {fieldDescription && (
          <FieldDescription fieldDescription={fieldDescription} />
        )}
      </PublishDataWrapper>
    </>
  );
};

export default observer(PublishDataPanel);
