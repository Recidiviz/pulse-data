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

import React from "react";

import { ReportOverview } from "../../shared/types";
import { printCommaSeparatedList } from "../../utils";
import {
  EditDetails,
  EditDetailsContent,
  EditDetailsTitle,
  PublishButton,
  PublishDataWrapper,
  Title,
} from "../Forms";

const PublishDataPanel: React.FC<{
  reportOverview: ReportOverview;
}> = ({ reportOverview }) => {
  return (
    <PublishDataWrapper>
      <Title>
        <PublishButton>Publish Data</PublishButton>
      </Title>

      <EditDetails>
        <EditDetailsTitle>Editors</EditDetailsTitle>
        <EditDetailsContent>
          {printCommaSeparatedList(reportOverview.editors)}
        </EditDetailsContent>

        <EditDetailsTitle>Details</EditDetailsTitle>
        <EditDetailsContent>
          Created today by {reportOverview.editors[0]}
        </EditDetailsContent>
      </EditDetails>
    </PublishDataWrapper>
  );
};

export default PublishDataPanel;
