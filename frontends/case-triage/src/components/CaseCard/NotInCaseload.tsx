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
import moment from "moment";
import { Icon, IconSVG } from "@recidiviz/design-system";

import { DecoratedClient } from "../../stores/ClientsStore";
import {
  ButtonContainer,
  Caption,
  CaseCardBody,
  CaseCardIconContainer,
  CaseCardInfo,
} from "./CaseCard.styles";
import { UncheckedButton } from "./CaseCardButtons";
import { CaseUpdateActionType } from "../../stores/CaseUpdatesStore";

interface NotInCaseloadProps {
  className: string;
  client: DecoratedClient;

  onUndo: () => void;
}

const NotInCaseload = ({
  client,
  className,
  onUndo,
}: NotInCaseloadProps): JSX.Element => {
  const submissionTime = moment(
    client.caseUpdates[CaseUpdateActionType.NOT_ON_CASELOAD].actionTs
  ).format("MMMM Do, Y");

  return (
    <CaseCardBody className={className}>
      <CaseCardIconContainer>
        <Icon kind={IconSVG.UserDelete} size={24} />
      </CaseCardIconContainer>
      <CaseCardInfo>
        <strong>Client not in caseload</strong>
        <br />
        <Caption>
          <div>Reported on {submissionTime}.</div>
          <div>Once confirmed, this client will be removed from your list.</div>
        </Caption>
        <ButtonContainer>
          <UncheckedButton onClick={onUndo}>Undo</UncheckedButton>
        </ButtonContainer>
      </CaseCardInfo>
    </CaseCardBody>
  );
};

export default NotInCaseload;
