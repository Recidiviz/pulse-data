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
import { DecoratedClient } from "../../stores/ClientsStore";
import { useRootStore } from "../../stores";
import {
  ClientCard,
  ClientNeed,
  DEFAULT_IN_PROGRESS_INDICATOR_OFFSET,
  FirstCardSection,
  IN_PROGRESS_INDICATOR_SIZE,
  InProgressIndicator,
  MainText,
  PendingText,
  SecondaryText,
  SecondCardSection,
  ThirdCardSection,
} from "./ClientList.styles";
import { titleCase } from "../../utils";
import DueDate from "../DueDate";
import Tooltip from "../Tooltip";
import { CaseUpdateActionType } from "../../stores/CaseUpdatesStore";
import { getNextContactDate } from "../../stores/ClientsStore/Client";

interface ClientProps {
  client: DecoratedClient;
}

const getNeedsMetState = (
  needsMet: DecoratedClient["needsMet"],
  need: keyof DecoratedClient["needsMet"]
): NeedState => {
  return needsMet[need] ? NeedState.MET : NeedState.NOT_MET;
};

const ClientComponent: React.FC<ClientProps> = ({ client }: ClientProps) => {
  const { clientsStore, policyStore } = useRootStore();
  const cardRef = React.useRef<HTMLDivElement>(null);

  const viewClient = React.useCallback(() => {
    clientsStore.view(
      client,
      cardRef !== null && cardRef.current !== null
        ? cardRef.current.offsetTop
        : 0
    );
  }, [client, clientsStore, cardRef]);

  React.useEffect(() => {
    // When undoing a Case Update, we need to re-open the client card
    // Check if this card's client is pending a `view`, if so, re-open the Case Card
    if (
      cardRef &&
      clientsStore.clientPendingView?.personExternalId ===
        client.personExternalId
    ) {
      viewClient();
    }
  }, [
    cardRef,
    clientsStore.clientPendingView,
    clientsStore.clientSearchString,
    client.personExternalId,
    viewClient,
  ]);

  // Counts in-progress (non-deprecated) actions
  const numInProgressActions = Object.values(CaseUpdateActionType).reduce(
    (accumulator, actionType) =>
      accumulator +
      (client.hasInProgressUpdate(actionType as CaseUpdateActionType) ? 1 : 0),
    0
  );

  const omsName = policyStore.policies?.omsName || "OMS";
  const tooltipOffset =
    cardRef === null || cardRef.current === null
      ? DEFAULT_IN_PROGRESS_INDICATOR_OFFSET
      : Math.max(
          -(
            cardRef.current.getBoundingClientRect().left +
            IN_PROGRESS_INDICATOR_SIZE
          ) / 2,
          DEFAULT_IN_PROGRESS_INDICATOR_OFFSET
        );

  const notOnCaseloadAction =
    client.caseUpdates[CaseUpdateActionType.NOT_ON_CASELOAD];
  let dueDateText;
  if (notOnCaseloadAction) {
    dueDateText = (
      <PendingText>
        Reported on{" "}
        {moment(notOnCaseloadAction.actionTs).format("MMMM Do, YYYY")}
      </PendingText>
    );
  } else if (client.caseUpdates[CaseUpdateActionType.CURRENTLY_IN_CUSTODY]) {
    dueDateText = <PendingText>In Custody</PendingText>;
  } else {
    dueDateText = <DueDate date={getNextContactDate(client)} />;
  }

  return (
    <ClientCard ref={cardRef} onClick={viewClient}>
      <FirstCardSection className="fs-exclude">
        <MainText>{client.name}</MainText>
        <SecondaryText>
          {titleCase(client.supervisionType)},{" "}
          {titleCase(client.supervisionLevelText)}
        </SecondaryText>
      </FirstCardSection>
      <SecondCardSection>
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
            state={
              client.needsMet.faceToFaceContact
                ? NeedState.MET
                : NeedState.NOT_MET
            }
          />
        </Tooltip>
      </SecondCardSection>
      <ThirdCardSection>{dueDateText}</ThirdCardSection>
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
          <InProgressIndicator style={{ left: tooltipOffset }} />
        </Tooltip>
      ) : null}
    </ClientCard>
  );
};

export default ClientComponent;
