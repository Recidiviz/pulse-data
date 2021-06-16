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
import { IconSVG, NeedState } from "@recidiviz/design-system";
import { observer } from "mobx-react-lite";
import { useRootStore } from "../../stores";
import {
  ClientNameSupervisionLevel,
  ClientNeed,
  FirstCardSection,
  MainText,
  MobileClientIcons,
  NeedsIconsCardSection,
  NextActionCardSection,
  PendingText,
  SecondaryText,
} from "./ClientList.styles";
import { titleCase } from "../../utils";
import DueDate from "../DueDate";
import Tooltip from "../Tooltip";
import { CaseUpdateActionType } from "../../stores/CaseUpdatesStore";
import { TodayDueDate } from "../DueDate/DueDate.styles";
import { OPPORTUNITY_TITLES } from "../../stores/OpportunityStore/Opportunity";
import { ClientProps } from "./ClientList.types";
import { Client } from "../../stores/ClientsStore";

const getNeedsMetState = (
  needsMet: Client["needsMet"],
  need: keyof Client["needsMet"]
): NeedState => {
  return needsMet[need] ? NeedState.MET : NeedState.NOT_MET;
};

interface TertiaryTextProps {
  client: Client;
}

const TertiaryText = observer(({ client }: TertiaryTextProps): JSX.Element => {
  const { opportunityStore } = useRootStore();

  const notOnCaseloadAction =
    client.caseUpdates[CaseUpdateActionType.NOT_ON_CASELOAD];

  const currentlyInCustodyAction =
    client.caseUpdates[CaseUpdateActionType.CURRENTLY_IN_CUSTODY];

  const opportunity = opportunityStore.getTopOpportunityForClient(
    client.personExternalId
  );

  if (opportunity && !opportunity.deferredUntil) {
    return (
      <TodayDueDate>
        {OPPORTUNITY_TITLES[opportunity.opportunityType]}
      </TodayDueDate>
    );
  }

  if (notOnCaseloadAction) {
    return (
      <PendingText>
        Reported on{" "}
        {moment(notOnCaseloadAction.actionTs).format("MMMM Do, YYYY")}
      </PendingText>
    );
  }
  if (currentlyInCustodyAction) {
    return <PendingText>In Custody</PendingText>;
  }

  return <DueDate date={client.nextContactDate} />;
});

const ClientCardIcons: React.FC<ClientProps> = observer(
  ({ children, client }) => {
    const { opportunityStore } = useRootStore();

    const topOpp = opportunityStore.getTopOpportunityForClient(
      client.personExternalId
    );

    return (
      <>
        {topOpp ? (
          <Tooltip
            key={topOpp.opportunityType}
            title={OPPORTUNITY_TITLES[topOpp.opportunityType]}
          >
            <ClientNeed kind={IconSVG.Star} state={NeedState.NOT_MET} />
          </Tooltip>
        ) : null}
        <Tooltip
          title={client.needsMet.employment ? "Employed" : "Employment Missing"}
        >
          <ClientNeed
            kind={IconSVG.NeedsEmployment}
            state={getNeedsMetState(client.needsMet, "employment")}
          />
        </Tooltip>
        <Tooltip
          title={
            client.needsMet.assessment
              ? "Risk Assessment Up to Date"
              : "Risk Assessment Needed"
          }
        >
          <ClientNeed
            kind={IconSVG.NeedsRiskAssessment}
            state={getNeedsMetState(client.needsMet, "assessment")}
          />
        </Tooltip>
        <Tooltip
          title={
            client.needsMet.faceToFaceContact
              ? "Face to Face Contact Up to Date"
              : "Face to Face Contact Needed"
          }
        >
          <ClientNeed
            kind={IconSVG.NeedsContact}
            state={getNeedsMetState(client.needsMet, "faceToFaceContact")}
          />
        </Tooltip>
        {children}
      </>
    );
  }
);

const CardIconsLayout: React.FC<ClientProps> = ({ client }: ClientProps) => {
  return (
    <>
      <FirstCardSection className="fs-exclude">
        <ClientNameSupervisionLevel>
          <MainText>{client.name}</MainText>
          <SecondaryText>
            {titleCase(client.supervisionType)},{" "}
            {titleCase(client.supervisionLevelText)}
          </SecondaryText>
        </ClientNameSupervisionLevel>
        <MobileClientIcons>
          <ClientCardIcons client={client} />
        </MobileClientIcons>
      </FirstCardSection>
      <NextActionCardSection>
        <TertiaryText client={client} />
      </NextActionCardSection>

      <NeedsIconsCardSection>
        <ClientCardIcons client={client} />
      </NeedsIconsCardSection>
    </>
  );
};
export default CardIconsLayout;
