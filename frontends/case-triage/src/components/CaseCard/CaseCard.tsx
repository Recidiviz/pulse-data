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
import { Card, Icon, IconSVG } from "@recidiviz/design-system";
import { observer } from "mobx-react-lite";
import {
  Caption,
  CaseCardHeading,
  ClientNameRow,
  CloseButton,
} from "./CaseCard.styles";
import NeedsEmployment from "./NeedsEmployment";
import NeedsFaceToFaceContact from "./NeedsFaceToFaceContact";
import NeedsRiskAssessment from "./NeedsRiskAssessment";
import { remScaledPixels, titleCase } from "../../utils";
import { useRootStore } from "../../stores";
import NotInCaseload from "./NotInCaseload";
import {
  NotInCaseloadActions,
  NotInCaseloadActionType,
} from "../../stores/CaseUpdatesStore";
import OpportunitySupervisionLevelReview from "../CaseOpportunities/OpportunitySupervisionLevelReview";
import TEST_IDS from "../TestIDs";
import { CaseCardProps } from "./CaseCard.types";
import { NotInCaseloadDropdown } from "./NotInCaseloadDropdown";
import { ClientName } from "./ClientProfileCard.styles";

const CaseCard: React.FC<CaseCardProps> = ({ client }) => {
  const { caseUpdatesStore, clientsStore, opportunityStore } = useRootStore();

  const opportunity = opportunityStore.getTopOpportunityForClient(
    client.personExternalId
  );

  const notInCaseloadUpdate = client.findInProgressUpdate(NotInCaseloadActions);

  return (
    <Card stacked data-testid={TEST_IDS.CASE_CARD}>
      <CaseCardHeading className="fs-exclude">
        <ClientNameRow>
          <ClientName>{client.name}</ClientName>
          <NotInCaseloadDropdown client={client} />
          <CloseButton onClick={() => clientsStore.setShowClientCard(false)}>
            <Icon kind={IconSVG.Close} size={remScaledPixels(14)} />
          </CloseButton>
        </ClientNameRow>
        <Caption>
          {titleCase(client.supervisionType)},{" "}
          {titleCase(client.supervisionLevelText)},{" "}
          {titleCase(client.personExternalId)}
        </Caption>
      </CaseCardHeading>

      <>
        {opportunity ? (
          <OpportunitySupervisionLevelReview
            className="fs-exclude"
            client={client}
            opportunity={opportunity}
          />
        ) : null}

        {notInCaseloadUpdate ? (
          <NotInCaseload
            action={notInCaseloadUpdate.actionType as NotInCaseloadActionType}
            client={client}
            onUndo={(updateId: string) => {
              caseUpdatesStore.removeAction(
                client,
                updateId,
                notInCaseloadUpdate?.actionType
              );
            }}
            className="fs-exclude"
          />
        ) : null}
        <NeedsEmployment client={client} className="fs-exclude" />
        <NeedsRiskAssessment client={client} className="fs-exclude" />
        <NeedsFaceToFaceContact client={client} className="fs-exclude" />
      </>
    </Card>
  );
};

export default observer(CaseCard);
