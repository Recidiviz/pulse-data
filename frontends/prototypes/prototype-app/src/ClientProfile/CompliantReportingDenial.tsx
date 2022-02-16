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
  Dropdown,
  DropdownMenu,
  DropdownMenuItem,
  DropdownToggle,
  Icon,
  IconSVG,
} from "@recidiviz/design-system";
import { observer } from "mobx-react-lite";
import React from "react";
import styled from "styled-components/macro";

import { useDataStore } from "../StoreProvider";

const REASONS_MAP = {
  DECF: "No effort to pay fine and costs",
  DECR: "Criminal record",
  DECT: "Insufficient time in supervision level",
  DEDF: "No effort to pay fees  ",
  DEDU: "Serious compliance problems ",
  DEIJ: "Not allowed per court",
  DEIO: "Denied for compliant reporting",
  DEIR: "Failure to report as instructed",
};

export const IconPad = styled.span`
  display: inline-block;
  margin-right: 8px;
`;

const Checkmark: React.FC = () => {
  return (
    <IconPad>
      <Icon kind={IconSVG.Check} size={12} />
    </IconPad>
  );
};

const CompliantReportingDenial: React.FC = () => {
  const { caseStore } = useDataStore();

  const client = caseStore.activeClient;
  if (!client) return null;

  let deniedCodes = "";
  if (client.deniedReasons?.length) {
    deniedCodes = `: ${client.deniedReasons.join(", ")}`;
  }

  return (
    <Dropdown>
      <DropdownToggle
        kind={client.status === "DENIED" ? "primary" : "secondary"}
        shape="block"
      >
        Not Eligible{deniedCodes}
      </DropdownToggle>
      <DropdownMenu>
        {Object.entries(REASONS_MAP).map(([code, desc]) => (
          <DropdownMenuItem
            key={code}
            onClick={() => {
              caseStore.setCompliantReportingStatus("DENIED", code);
            }}
          >
            {client.deniedReasons?.includes(code) ? <Checkmark /> : null}
            {code}: {desc}
          </DropdownMenuItem>
        ))}
      </DropdownMenu>
    </Dropdown>
  );
};

export default observer(CompliantReportingDenial);
