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
import { observer } from "mobx-react-lite";
import {
  ClientNameSupervisionLevel,
  NameCardSection,
  MainText,
  StatusCardSection,
  SecondaryText,
  ClientNameTag,
} from "./ClientList.styles";
import { titleCase } from "../../utils";
import { ClientProps } from "./ClientList.types";
import { ClientStatusList } from "./ClientStatusList";

const CardPillsLayout: React.FC<ClientProps> = ({ client }: ClientProps) => {
  return (
    <>
      <NameCardSection className="fs-exclude">
        <ClientNameSupervisionLevel>
          <MainText>
            {client.name}
            {client.newToCaseload && (
              <ClientNameTag>{client.newToCaseload.previewText}</ClientNameTag>
            )}
          </MainText>
          <SecondaryText>
            {titleCase(client.supervisionType)},{" "}
            {titleCase(client.supervisionLevelText)}
          </SecondaryText>
        </ClientNameSupervisionLevel>
      </NameCardSection>
      <StatusCardSection>
        <ClientStatusList client={client} />
      </StatusCardSection>
    </>
  );
};
export default observer(CardPillsLayout);
