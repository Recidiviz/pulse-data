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
import { autorun } from "mobx";
import { observer } from "mobx-react-lite";
import * as React from "react";
import { trackPersonSelected } from "../../analytics";
import { useRootStore } from "../../stores";
import {
  ACTION_TITLES,
  isErrorReport,
} from "../../stores/CaseUpdatesStore/CaseUpdates";
import Tooltip from "../Tooltip";
import CardPillsLayout from "./CardPillsLayout";
import {
  ClientListCardElement,
  InProgressIndicator,
} from "./ClientList.styles";
import { ClientProps } from "./ClientList.types";

const ClientComponent: React.FC<ClientProps> = observer(
  ({ client }: ClientProps) => {
    const { clientsStore, policyStore, userStore } = useRootStore();
    const cardRef = React.useRef<HTMLDivElement>(null);

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
        if (client.isActive) {
          viewClient();
        }
      });
    }, [client, clientsStore, viewClient]);

    const { omsName } = policyStore;

    const active =
      clientsStore.activeClient?.personExternalId === client.personExternalId;

    let tooltipTitle: React.ReactNode;

    if (userStore.canSeeProfileV2) {
      tooltipTitle = client.inProgressUpdates
        .filter(isErrorReport)
        .map((actionType) => `${ACTION_TITLES[actionType]} report pending.`)
        .join(" ");
    } else {
      tooltipTitle = client.inProgressUpdates.length ? (
        <>
          <strong>{client.inProgressUpdates.length}</strong> action
          {client.inProgressUpdates.length !== 1 ? "s" : ""} being confirmed
          with {omsName}
        </>
      ) : null;
    }

    return (
      <ClientListCardElement
        className={active ? "client-card--active" : ""}
        ref={cardRef}
        onClick={() => {
          trackPersonSelected(client);
          viewClient();
        }}
      >
        <CardPillsLayout client={client} />

        {tooltipTitle && (
          <Tooltip title={tooltipTitle}>
            <InProgressIndicator />
          </Tooltip>
        )}
      </ClientListCardElement>
    );
  }
);

export default ClientComponent;
