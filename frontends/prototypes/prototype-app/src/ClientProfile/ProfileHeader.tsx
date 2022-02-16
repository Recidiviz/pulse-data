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

import {
  Button,
  CardSection,
  H3,
  Icon,
  IconSVG,
  spacing,
} from "@recidiviz/design-system";
import { observer } from "mobx-react-lite";
import { rem } from "polished";
import React from "react";
import styled from "styled-components/macro";

import { useDataStore } from "../StoreProvider";

export const CloseButton = styled(Button).attrs({
  kind: "borderless",
  shape: "block",
})`
  height: ${rem(spacing.xl)};
  width: ${rem(spacing.xl)};
  padding: ${rem(spacing.sm)};
`;

export const ClientName = styled(H3)`
  margin-top: 0;
  margin-right: auto;
  margin-bottom: 0;
`;

const ClientProfileHeading = styled(CardSection)`
  padding: ${rem(spacing.xl)};
  padding-bottom: ${rem(spacing.md)};
  display: flex;
  flex-wrap: wrap;
  grid-area: 1 / 1;
`;

const ClientInfo = styled.div`
  font-size: 14px;
  width: 100%;
  line-height: 1.3;
`;

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
        {client.personExternalId}, {client.officerName}
      </ClientInfo>
    </ClientProfileHeading>
  );
};

export default observer(ProfileHeader);
