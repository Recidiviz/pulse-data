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
import { observer } from "mobx-react-lite";
import * as React from "react";
import { IconSVG } from "@recidiviz/case-triage-components";
import { useRootStore } from "../../stores";
import { DecoratedClient } from "../../stores/ClientsStore/Client";
import { titleCase } from "../../utils";
import {
  CardHeader,
  ClientCard,
  ClientListHeading,
  ClientListTableHeading,
  ClientNeed,
  FlexCardSection,
  MainText,
  SecondaryText,
} from "./ClientList.styles";
import DueDate from "../DueDate";

// Update this if the <ClientListHeading/> height changes
export const HEADING_HEIGHT_MAGIC_NUMBER = 118;

interface ClientProps {
  client: DecoratedClient;
}

const ClientComponent: React.FC<ClientProps> = ({ client }: ClientProps) => {
  const { clientsStore } = useRootStore();

  const cardRef = React.useRef<HTMLDivElement>(null);

  return (
    <ClientCard
      ref={cardRef}
      onClick={() => {
        clientsStore.view(
          client,
          cardRef !== null && cardRef.current !== null
            ? cardRef.current.offsetTop
            : 0
        );
      }}
    >
      <CardHeader className="fs-exclude">
        <MainText>{client.formalName}</MainText>
        <SecondaryText>
          {titleCase(client.supervisionType)},{" "}
          {titleCase(client.supervisionLevel)}
        </SecondaryText>
      </CardHeader>
      <FlexCardSection>
        <ClientNeed
          kind={IconSVG.NeedsEmployment}
          met={client.needsMet.employment}
        />
        <ClientNeed
          kind={IconSVG.NeedsRiskAssessment}
          met={client.needsMet.assessment}
        />
        <ClientNeed
          kind={IconSVG.NeedsContact}
          met={client.needsMet.faceToFaceContact}
        />
      </FlexCardSection>
      <FlexCardSection>
        <DueDate date={client.nextFaceToFaceDate} />
      </FlexCardSection>
    </ClientCard>
  );
};

const ClientList = () => {
  const { clientsStore, policyStore } = useRootStore();

  let clients;
  if (clientsStore.clients) {
    clients = clientsStore.clients.map((client) => (
      <ClientComponent client={client} key={client.personExternalId} />
    ));
  }

  return (
    <div>
      <ClientListHeading>Up Next</ClientListHeading>
      <ClientListTableHeading>
        <span>Client</span>
        <span>Needs</span>
        <span>Recommended Contact</span>
      </ClientListTableHeading>
      {clientsStore.isLoading || policyStore.isLoading || !clientsStore.clients
        ? `Loading... ${clientsStore.error || ""}`
        : clients}
    </div>
  );
};

export default observer(ClientList);
