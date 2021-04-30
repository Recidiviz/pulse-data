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
import { useState } from "react";
import { H3, Icon, IconSVG } from "@recidiviz/design-system";
import { autorun } from "mobx";
import {
  Caption,
  CaseCard as CaseCardComponent,
  CaseCardHeading,
  ClientNameRow,
  CloseButton,
  EllipsisDropdown,
} from "./CaseCard.styles";
import NeedsEmployment from "./NeedsEmployment";
import NeedsFaceToFaceContact from "./NeedsFaceToFaceContact";
import NeedsRiskAssessment from "./NeedsRiskAssessment";
import { DecoratedClient } from "../../stores/ClientsStore";
import { titleCase } from "../../utils";
import { useRootStore } from "../../stores";
import NotInCaseload from "./NotInCaseload";
import {
  NotInCaseloadActions,
  NotInCaseloadActionType,
} from "../../stores/CaseUpdatesStore";

export interface CaseCardProps {
  client: DecoratedClient;
}

const CaseCard: React.FC<CaseCardProps> = ({ client }: CaseCardProps) => {
  const { caseUpdatesStore, clientsStore } = useRootStore();

  const [marginTop, setMarginTop] = useState(clientsStore.activeClientOffset);

  React.useEffect(() =>
    autorun(() => {
      setMarginTop(clientsStore.activeClientOffset);
    })
  );
  const currentNotOnCaseloadAction = NotInCaseloadActions.find(
    (value: string) => client.caseUpdates[value as NotInCaseloadActionType]
  );

  return (
    <CaseCardComponent stacked style={{ marginTop }}>
      <CaseCardHeading className="fs-exclude">
        <ClientNameRow>
          <H3 as="div">{client.name}</H3>

          <EllipsisDropdown
            actions={NotInCaseloadActions}
            client={client}
            icon={IconSVG.TripleDot}
            borderless
          />
          <CloseButton onClick={() => clientsStore.view()}>
            <Icon kind={IconSVG.Close} />
          </CloseButton>
        </ClientNameRow>
        <Caption>
          {titleCase(client.supervisionType)},{" "}
          {titleCase(client.supervisionLevelText)},{" "}
          {titleCase(client.personExternalId)}
        </Caption>
      </CaseCardHeading>
      {currentNotOnCaseloadAction ? (
        <NotInCaseload
          action={currentNotOnCaseloadAction as NotInCaseloadActionType}
          client={client}
          onUndo={(updateId: string) => {
            caseUpdatesStore.removeAction(
              client,
              updateId,
              currentNotOnCaseloadAction
            );
          }}
          className="fs-exclude"
        />
      ) : null}
      <NeedsEmployment client={client} className="fs-exclude" />
      <NeedsRiskAssessment client={client} className="fs-exclude" />
      <NeedsFaceToFaceContact client={client} className="fs-exclude" />
    </CaseCardComponent>
  );
};

export default CaseCard;
