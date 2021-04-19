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
import { useRootStore } from "../../stores";
import {
  ClientListContainer,
  ClientListHeading,
  ClientListTableHeading,
} from "./ClientList.styles";
import ClientListCard from "./ClientListCard";
import EmptyStateCard from "./EmptyStateCard";

const ClientList = () => {
  const { clientsStore } = useRootStore();

  let clients;
  if (clientsStore.clients.length > 0) {
    clients = clientsStore.clients.map((client) => (
      <ClientListCard
        client={client}
        isInProgress={client.inProgressSubmissionDate !== null}
        key={client.personExternalId}
      />
    ));
  } else {
    clients = <EmptyStateCard />;
  }

  return (
    <ClientListContainer>
      <ClientListHeading>Caseload</ClientListHeading>
      <ClientListTableHeading>
        <span>Client</span>
        <span>Needs Met</span>
        <span>Recommended Contact</span>
      </ClientListTableHeading>

      {clientsStore.isLoading && clientsStore.clients.length === 0
        ? `Loading... ${clientsStore.error || ""}`
        : clients}
    </ClientListContainer>
  );
};

export default observer(ClientList);
