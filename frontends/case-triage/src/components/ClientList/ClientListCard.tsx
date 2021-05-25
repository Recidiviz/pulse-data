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
import { autorun } from "mobx";
import { DecoratedClient } from "../../stores/ClientsStore";
import { useRootStore } from "../../stores";
import {
  ClientListCardElement,
  ClientNeed,
  FirstCardSection,
  InProgressIndicator,
  MainText,
  NeedsIconsCardSection,
  NextActionCardSection,
  PendingText,
  SecondaryText,
} from "./ClientList.styles";
import { titleCase } from "../../utils";
import DueDate from "../DueDate";
import Tooltip from "../Tooltip";
import { CaseUpdateActionType } from "../../stores/CaseUpdatesStore";
import { getNextContactDate } from "../../stores/ClientsStore/Client";
import { TodayDueDate } from "../DueDate/DueDate.styles";
import { OPPORTUNITY_TITLES } from "../../stores/OpportunityStore/Opportunity";
import { trackPersonSelected } from "../../analytics";

interface ClientProps {
  client: DecoratedClient;
}

const getNeedsMetState = (
  needsMet: DecoratedClient["needsMet"],
  need: keyof DecoratedClient["needsMet"]
): NeedState => {
  return needsMet[need] ? NeedState.MET : NeedState.NOT_MET;
};

interface TertiaryTextProps {
  client: DecoratedClient;
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

  return <DueDate date={getNextContactDate(client)} />;
});

const ClientComponent: React.FC<ClientProps> = observer(
  ({ client }: ClientProps) => {
    const { clientsStore, opportunityStore, policyStore } = useRootStore();
    const cardRef = React.useRef<HTMLDivElement>(null);

    const topOpp = opportunityStore.getTopOpportunityForClient(
      client.personExternalId
    );

    const viewClient = React.useCallback(() => {
      clientsStore.view(
        client,
        cardRef !== null && cardRef.current !== null
          ? cardRef.current.offsetTop
          : 0
      );
    }, [client, clientsStore, cardRef]);

    React.useLayoutEffect(() => {
      // When undoing a Case Update, we need to re-open the client card
      // Check if this card's client is pending a `view`, if so, re-open the Case Card
      return autorun(() => {
        if (clientsStore.isActive(client)) {
          viewClient();
        }
      });
    }, [client, clientsStore, viewClient]);

    // Counts in-progress (non-deprecated) actions
    const numInProgressActions = Object.values(CaseUpdateActionType).reduce(
      (accumulator, actionType) =>
        accumulator +
        (client.hasInProgressUpdate(actionType as CaseUpdateActionType)
          ? 1
          : 0),
      0
    );

    const omsName = policyStore.policies?.omsName || "OMS";

    return (
      <ClientListCardElement
        className={
          clientsStore.activeClient?.personExternalId ===
          client.personExternalId
            ? "client-card--active"
            : ""
        }
        ref={cardRef}
        onClick={() => {
          trackPersonSelected(client);
          viewClient();
        }}
      >
        <FirstCardSection className="fs-exclude">
          <MainText>{client.name}</MainText>
          <SecondaryText>
            {titleCase(client.supervisionType)},{" "}
            {titleCase(client.supervisionLevelText)}
          </SecondaryText>
        </FirstCardSection>
        <NeedsIconsCardSection>
          <TertiaryText client={client} />
        </NeedsIconsCardSection>
        <NextActionCardSection>
          {topOpp ? (
            <Tooltip
              key={topOpp.opportunityType}
              title={OPPORTUNITY_TITLES[topOpp.opportunityType]}
            >
              <ClientNeed kind={IconSVG.Star} state={NeedState.NOT_MET} />
            </Tooltip>
          ) : null}
          <Tooltip
            title={
              client.needsMet.employment ? "Employed" : "Employment Missing"
            }
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
              state={
                client.needsMet.faceToFaceContact
                  ? NeedState.MET
                  : NeedState.NOT_MET
              }
            />
          </Tooltip>
        </NextActionCardSection>
        {numInProgressActions > 0 ? (
          <Tooltip
            title={
              <>
                <strong>{numInProgressActions}</strong> action
                {numInProgressActions !== 1 ? "s" : ""} being confirmed with{" "}
                {omsName}
              </>
            }
          >
            <InProgressIndicator />
          </Tooltip>
        ) : null}
      </ClientListCardElement>
    );
  }
);

export default ClientComponent;
