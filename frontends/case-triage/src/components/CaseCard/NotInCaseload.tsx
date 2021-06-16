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
import { Icon, IconSVG } from "@recidiviz/design-system";
import moment from "moment";
import * as React from "react";
import {
  CaseUpdateActionType,
  NotInCaseloadActionType,
} from "../../stores/CaseUpdatesStore";
import { Client } from "../../stores/ClientsStore";
import {
  ButtonContainer,
  Caption,
  CaseCardBody,
  CaseCardIconContainer,
  CaseCardInfo,
} from "./CaseCard.styles";
import { UncheckedButton } from "./CaseCardButtons";

interface NotInCaseloadProps {
  action: NotInCaseloadActionType;

  className: string;
  client: Client;

  onUndo: (updateId: string) => void;
}

const NotInCaseloadActionTitles = {
  [CaseUpdateActionType.NOT_ON_CASELOAD]: "Not on Caseload",
  [CaseUpdateActionType.CURRENTLY_IN_CUSTODY]: "In Custody",
};

const NotInCaseload = ({
  action,
  client,
  className,
  onUndo,
}: NotInCaseloadProps): JSX.Element | null => {
  const caseUpdate = client.caseUpdates[action];
  const submissionTime = moment(caseUpdate.actionTs).format("MMMM Do, Y");

  return (
    <CaseCardBody className={className}>
      <CaseCardIconContainer>
        <Icon kind={IconSVG.ItemDelete} size={24} />
      </CaseCardIconContainer>
      <CaseCardInfo>
        <strong>{NotInCaseloadActionTitles[action]}</strong>
        <br />
        <Caption>
          <div>Reported on {submissionTime}</div>
        </Caption>
        <ButtonContainer>
          <UncheckedButton
            onClick={() => caseUpdate.updateId && onUndo(caseUpdate.updateId)}
          >
            Return to Caseload
          </UncheckedButton>
        </ButtonContainer>
      </CaseCardInfo>
    </CaseCardBody>
  );
};

export default NotInCaseload;
