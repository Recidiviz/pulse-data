// Recidiviz - a data platform for criminal justice reform
// Copyright (C) 2021 Recidiviz, Inc.
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
import { useEffect, useState } from "react";
import * as React from "react";
import { Icon, IconSVG, palette } from "@recidiviz/case-triage-components";
import { ClientMarkedInProgress } from "../../stores/ClientsStore/ClientsStore";
import {
  InProgressOverlay,
  InProgressConfirmation,
  InProgressConfirmationHeading,
} from "./ClientMarkedInProgressOverlay.styles";

interface Props {
  clientMarkedInProgress: ClientMarkedInProgress;
}

const ClientMarkedInProgressOverlay = ({
  clientMarkedInProgress: { client, wasPositiveAction },
}: Props) => {
  return (
    <InProgressOverlay>
      <InProgressConfirmation>
        <InProgressConfirmationHeading>
          {wasPositiveAction ? "Great work!" : "Thanks for your feedback!"}
        </InProgressConfirmationHeading>
        We&lsquo;ve marked {client.name}&lsquo;
        {client.name[client.name.length - 1] === "s" ? "" : "s"} case as in
        progress.
      </InProgressConfirmation>
      <Icon kind={IconSVG.Success} fill={palette.white.main} size={32} />
    </InProgressOverlay>
  );
};

export default ClientMarkedInProgressOverlay;
