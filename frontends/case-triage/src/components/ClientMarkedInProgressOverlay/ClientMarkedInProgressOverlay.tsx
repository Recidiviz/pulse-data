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
import * as React from "react";
import { Icon, IconSVG, palette } from "@recidiviz/design-system";
import { ClientMarkedInProgress } from "../../stores/ClientsStore/ClientsStore";
import {
  InProgressOverlay,
  InProgressConfirmation,
  InProgressConfirmationHeading,
  Undo,
} from "./ClientMarkedInProgressOverlay.styles";
import { useRootStore } from "../../stores";

interface Props {
  clientMarkedInProgress: ClientMarkedInProgress;
}

const ClientMarkedInProgressOverlay = ({
  clientMarkedInProgress: { client, wasPositiveAction },
}: Props): JSX.Element => {
  const { caseUpdatesStore } = useRootStore();

  return (
    <InProgressOverlay>
      <InProgressConfirmation>
        <InProgressConfirmationHeading>
          {wasPositiveAction ? "Great work!" : "Thanks for your feedback!"}
        </InProgressConfirmationHeading>
        We&lsquo;ve marked <span className="fs-exclude">{client.name}</span>
        &lsquo;s case as in progress.{" "}
        <Undo onClick={() => caseUpdatesStore.undo(client)}>Undo?</Undo>
      </InProgressConfirmation>
      <Icon kind={IconSVG.Success} fill={palette.white} size={32} />
    </InProgressOverlay>
  );
};

export default ClientMarkedInProgressOverlay;
