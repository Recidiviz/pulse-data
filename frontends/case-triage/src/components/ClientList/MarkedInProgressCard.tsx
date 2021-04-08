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
import { DecoratedClient } from "../../stores/ClientsStore";
import { useRootStore } from "../../stores";
import { ClientCard } from "./ClientList.styles";
import ClientMarkedInProgressOverlay from "../ClientMarkedInProgressOverlay";

interface MarkedInProgressCardProps {
  client: DecoratedClient;
}

const MarkedInProgressCard = ({
  client,
}: MarkedInProgressCardProps): JSX.Element => {
  const { clientsStore } = useRootStore();

  return (
    <ClientCard>
      <ClientMarkedInProgressOverlay
        clientMarkedInProgress={
          clientsStore.clientsMarkedInProgress[client.personExternalId]
        }
        key={client.personExternalId}
      />
    </ClientCard>
  );
};

export default MarkedInProgressCard;
