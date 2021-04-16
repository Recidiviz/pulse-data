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
  CardHeader,
  ClientCard,
  ClientNeed,
  FlexCardSection,
  MainText,
  SecondaryText,
} from "./ClientList.styles";
import { titleCase } from "../../utils";
import DueDate from "../DueDate";
import Tooltip from "../Tooltip";

interface ClientProps {
  client: DecoratedClient;
  isInProgress: boolean;
}

const getNeedsMetState = (
  isInProgress: boolean,
  needsMet: DecoratedClient["needsMet"],
  need: keyof DecoratedClient["needsMet"]
): NeedState => {
  if (isInProgress) {
    return needsMet[need] ? NeedState.DISABLED_MET : NeedState.DISABLED;
  }

  return needsMet[need] ? NeedState.MET : NeedState.NOT_MET;
};

const ClientComponent: React.FC<ClientProps> = ({
  client,
  isInProgress,
}: ClientProps) => {
  const { clientsStore } = useRootStore();
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

  return (
    <ClientCard
      className={isInProgress ? "client-card--in-progress" : ""}
      ref={cardRef}
      onClick={viewClient}
    >
      <CardHeader className="fs-exclude">
        <MainText>{client.name}</MainText>
        <SecondaryText>
          {titleCase(client.supervisionType)},{" "}
          {titleCase(client.supervisionLevelText)}
        </SecondaryText>
      </CardHeader>
      <FlexCardSection>
        <Tooltip title="Employment">
          <ClientNeed
            kind={IconSVG.NeedsEmployment}
            state={getNeedsMetState(
              isInProgress,
              client.needsMet,
              "employment"
            )}
          />
        </Tooltip>
        <Tooltip title="Risk Assessment">
          <ClientNeed
            kind={IconSVG.NeedsRiskAssessment}
            state={getNeedsMetState(
              isInProgress,
              client.needsMet,
              "assessment"
            )}
          />
        </Tooltip>
        <Tooltip title="Face to Face Contact">
          <ClientNeed
            kind={IconSVG.NeedsContact}
            state={getNeedsMetState(
              isInProgress,
              client.needsMet,
              "faceToFaceContact"
            )}
          />
        </Tooltip>
      </FlexCardSection>
      <FlexCardSection>
        {isInProgress ? (
          <MainText>In progress</MainText>
        ) : (
          <DueDate date={client.nextFaceToFaceDate} />
        )}
      </FlexCardSection>
    </ClientCard>
  );
};

export default ClientComponent;
