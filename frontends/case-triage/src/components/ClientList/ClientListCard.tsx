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
import { IconSVG, NeedState } from "@recidiviz/design-system";
import { DecoratedClient } from "../../stores/ClientsStore";
import { useRootStore } from "../../stores";
import {
  ClientCard,
  ClientNeed,
  DEFAULT_IN_PROGRESS_INDICATOR_OFFSET,
  FirstCardSection,
  InProgressIndicator,
  IN_PROGRESS_INDICATOR_SIZE,
  MainText,
  PendingText,
  SecondCardSection,
  SecondaryText,
  ThirdCardSection,
} from "./ClientList.styles";
import { titleCase } from "../../utils";
import DueDate from "../DueDate";
import Tooltip from "../Tooltip";
import {
  CaseUpdateActionType,
  CaseUpdateStatus,
} from "../../stores/CaseUpdatesStore";
import { getNextContactDate } from "../../stores/ClientsStore/Client";

interface ClientProps {
  client: DecoratedClient;
  showInProgress?: boolean;
}

const getNeedsMetState = (
  needsMet: DecoratedClient["needsMet"],
  need: keyof DecoratedClient["needsMet"]
): NeedState => {
  return needsMet[need] ? NeedState.MET : NeedState.NOT_MET;
};

const ClientComponent: React.FC<ClientProps> = ({
  client,
  showInProgress,
}: ClientProps) => {
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

  const numInProgressActions =
    client.caseUpdates === undefined
      ? 0
      : Object.values(client.caseUpdates).reduce(
          (accumulator, val) =>
            accumulator + (val.status === CaseUpdateStatus.IN_PROGRESS ? 1 : 0),
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
  if (!notOnCaseloadAction) {
    dueDateText = <DueDate date={getNextContactDate(client)} />;
  } else {
    dueDateText = (
      <PendingText>
        Reported on{" "}
        {moment(notOnCaseloadAction.actionTs).format("MMMM Do, YYYY")}
      </PendingText>
    );
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
        <Tooltip title="Employment">
          <ClientNeed
            kind={IconSVG.NeedsEmployment}
            state={getNeedsMetState(client.needsMet, "employment")}
          />
        </Tooltip>
        <Tooltip title="Risk Assessment">
          <ClientNeed
            kind={IconSVG.NeedsRiskAssessment}
            state={getNeedsMetState(client.needsMet, "assessment")}
          />
        </Tooltip>
        <Tooltip title="Face to Face Contact">
          <ClientNeed
            kind={IconSVG.NeedsContact}
            state={getNeedsMetState(client.needsMet, "faceToFaceContact")}
          />
        </Tooltip>
      </SecondCardSection>
      <ThirdCardSection>{dueDateText}</ThirdCardSection>
      {showInProgress ? (
        <Tooltip
          title={
            <>
              <strong>{numInProgressActions}</strong> Task
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
