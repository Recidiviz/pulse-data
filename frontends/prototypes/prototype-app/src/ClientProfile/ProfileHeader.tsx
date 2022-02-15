// Recidiviz - a data platform for criminal justice reform
// Copyright (C) 2022 Recidiviz, Inc.
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

import { Icon, IconSVG } from "@recidiviz/design-system";
import { observer } from "mobx-react-lite";
import React from "react";

import { useDataStore } from "../StoreProvider";
import { titleCase } from "../utils";
import {
  ClientInfo,
  ClientName,
  ClientProfileHeading,
  CloseButton,
} from "./styles";

const ProfileHeader: React.FC = () => {
  const { caseStore } = useDataStore();

  const client = caseStore.activeClient;
  if (!client) return null;

  return (
    <ClientProfileHeading>
      <ClientName>{client.personName}</ClientName>

      <CloseButton onClick={() => caseStore.setActiveClient()}>
        <Icon kind={IconSVG.Close} size={14} />
      </CloseButton>

      <ClientInfo>
        {titleCase(client.supervisionType)}, {client.supervisionLevel},{" "}
        {client.personExternalId}
      </ClientInfo>
    </ClientProfileHeading>
  );
};

export default observer(ProfileHeader);
