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
import { CaseUpdateActionType } from "../../stores/CaseUpdatesStore";
import {
  ClientListContainer,
  ClientListHeading,
  ClientListTableHeading,
  FirstClientListHeading,
} from "./ClientList.styles";
import ClientListCard from "./ClientListCard";
import EmptyStateCard from "./EmptyStateCard";

const ClientList = () => {
  const { clientsStore } = useRootStore();

  const activeClients = clientsStore.clients.filter(
    ({ caseUpdates }) => !caseUpdates[CaseUpdateActionType.NOT_ON_CASELOAD]
  );
  const processingClients = clientsStore.clients.filter(
    ({ caseUpdates }) => caseUpdates[CaseUpdateActionType.NOT_ON_CASELOAD]
  );

  let activeClientList;
  if (activeClients.length > 0) {
    activeClientList = activeClients.map((client) => (
      <ClientListCard
        client={client}
        showInProgress={Object.keys(client.caseUpdates).length > 0}
        key={client.personExternalId}
      />
    ));
  } else {
    activeClientList = <EmptyStateCard />;
  }

  let processingList;
  if (processingClients.length > 0) {
    processingList = (
      <>
        <ClientListHeading>Processing Feedback</ClientListHeading>
        <ClientListTableHeading />
        {processingClients.map((client) => (
          <ClientListCard client={client} key={client.personExternalId} />
        ))}
      </>
    );
  }

  return (
    <>
      <ClientListContainer>
        <FirstClientListHeading>Up Next</FirstClientListHeading>
        <ClientListTableHeading>
          <span>Client</span>
          <span>Needs Met</span>
          <span>Recommended Contact</span>
        </ClientListTableHeading>

        {clientsStore.isLoading && clientsStore.clients.length === 0 ? (
          `Loading... ${clientsStore.error || ""}`
        ) : (
          <>
            {activeClientList}
            {processingList}
          </>
        )}
      </ClientListContainer>
    </>
  );
};

export default observer(ClientList);
