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
import CardPillsLayout from "./CardPillsLayout";
import { ClientListCardElement } from "./ClientList.styles";
import { ClientProps } from "./ClientList.types";

const ClientComponent: React.FC<ClientProps> = observer(
  ({ client }: ClientProps) => {
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

    React.useLayoutEffect(() => {
      // When undoing a Case Update, we need to re-open the client card
      // Check if this card's client is pending a `view`, if so, re-open the Case Card
      return autorun(() => {
        if (client.isActive) {
          viewClient();
        }
      });
    }, [client, clientsStore, viewClient]);

    const active =
      clientsStore.activeClient?.personExternalId === client.personExternalId;

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
      </ClientListCardElement>
    );
  }
);

export default ClientComponent;
