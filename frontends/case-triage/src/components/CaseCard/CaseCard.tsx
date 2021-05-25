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
import { useEffect, useState } from "react";
import { Dropdown, H3, Icon, IconSVG } from "@recidiviz/design-system";
import { observer } from "mobx-react-lite";
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
import { titleCase } from "../../utils";
import { useRootStore } from "../../stores";
import NotInCaseload from "./NotInCaseload";
import {
  CaseUpdateActionType,
  NotInCaseloadActions,
  NotInCaseloadActionType,
} from "../../stores/CaseUpdatesStore";
import OpportunitySupervisionLevelReview from "../CaseOpportunities/OpportunitySupervisionLevelReview";
import TEST_IDS from "../TestIDs";
import { CaseCardProps } from "./CaseCard.types";

const CaseCard: React.FC<CaseCardProps> = ({ client }) => {
  const { caseUpdatesStore, clientsStore, opportunityStore } = useRootStore();

  const [transitionDuration, setTransitionDuration] = useState("0");
  const [translateY, setTranslateY] = useState(clientsStore.activeClientOffset);

  useEffect(() => {
    return autorun(() => {
      if (clientsStore.activeClientOffset !== translateY) {
        if (clientsStore.clientPendingAnimation) {
          clientsStore.setClientPendingAnimation(false);
          setTransitionDuration("1s");
        } else {
          setTransitionDuration("0s");
        }

        setTranslateY(clientsStore.activeClientOffset);
      }
    });
  });

  const currentNotOnCaseloadAction = NotInCaseloadActions.find(
    (value: string) => client.caseUpdates[value as NotInCaseloadActionType]
  );
  let ellipsisDropdownActions: CaseUpdateActionType[] = [];
  let ellipsisDropdown;
  if (!currentNotOnCaseloadAction) {
    ellipsisDropdownActions =
      ellipsisDropdownActions.concat(NotInCaseloadActions);
  } else if (client.hasInProgressUpdate(CaseUpdateActionType.NOT_ON_CASELOAD)) {
    ellipsisDropdownActions.push(CaseUpdateActionType.NOT_ON_CASELOAD);
  }

  if (ellipsisDropdownActions.length > 0) {
    ellipsisDropdown = (
      <EllipsisDropdown
        actions={ellipsisDropdownActions}
        client={client}
        alignment="right"
        borderless
      >
        <Dropdown.ToggleIcon kind={IconSVG.TripleDot} size={18} />
      </EllipsisDropdown>
    );
  }

  const opportunity = opportunityStore.getTopOpportunityForClient(
    client.personExternalId
  );

  return (
    <CaseCardComponent
      data-testid={TEST_IDS.CASE_CARD}
      stacked
      style={{
        transitionDuration,
        transitionProperty: "transform",
        transform: `translateY(${translateY}px)`,
      }}
    >
      <CaseCardHeading className="fs-exclude">
        <ClientNameRow>
          <H3 as="div">{client.name}</H3>

          {ellipsisDropdown}
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

      {opportunity ? (
        <OpportunitySupervisionLevelReview
          className="fs-exclude"
          client={client}
          opportunity={opportunity}
        />
      ) : null}

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

export default observer(CaseCard);
