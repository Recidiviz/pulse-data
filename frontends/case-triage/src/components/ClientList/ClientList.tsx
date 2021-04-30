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
  CaseUpdateActionType,
  NotInCaseloadActions,
} from "../../stores/CaseUpdatesStore";
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
    ({ caseUpdates }) =>
      !NotInCaseloadActions.find(
        (action: CaseUpdateActionType) => caseUpdates[action]
      )
  );
  const inCustodyClients = clientsStore.clients.filter(
    ({ caseUpdates }) =>
      caseUpdates[CaseUpdateActionType.CURRENTLY_IN_CUSTODY] &&
      !caseUpdates[CaseUpdateActionType.NOT_ON_CASELOAD]
  );
  const notOnCaseloadClients = clientsStore.clients.filter(
    ({ caseUpdates }) => caseUpdates[CaseUpdateActionType.NOT_ON_CASELOAD]
  );

  const upNextClients = activeClients.concat(inCustodyClients);

  let activeClientList;
  if (upNextClients.length > 0) {
    activeClientList = upNextClients.map((client) => (
      <ClientListCard client={client} key={client.personExternalId} />
    ));
  } else {
    activeClientList = <EmptyStateCard />;
  }

  let processingList;
  if (notOnCaseloadClients.length > 0) {
    processingList = (
      <>
        <ClientListHeading>Processing Feedback</ClientListHeading>
        <ClientListTableHeading />
        {notOnCaseloadClients.map((client) => (
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
