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

import { palette, spacing } from "@recidiviz/design-system";
import { observer } from "mobx-react-lite";
import React from "react";
import styled from "styled-components/macro";

import { UpcomingDischargeCase } from "../DataStores/CaseStore";
import { useDataStore } from "../StoreProvider";
import Tooltip from "../Tooltip";

const TooltipContent = styled.div`
  display: flex;
  flex-direction: column;
  font-size: 1rem;
  row-gap: ${spacing.sm}px;
  padding: ${spacing.md}px;
`;

const TooltipSourceLine = styled.div`
  color: ${palette.marble1};
`;

const TooltipUpdateLine = styled.div`
  color: ${palette.signal.highlight};
`;

const DischargeDateTooltipContent: React.FC<{ dischargeSource: string }> = ({
  dischargeSource,
}) => {
  return (
    <TooltipContent>
      <TooltipSourceLine>{dischargeSource}</TooltipSourceLine>
      <TooltipUpdateLine>Click to Update</TooltipUpdateLine>
    </TooltipContent>
  );
};

const DischargeTooltip: React.FC<{ record: UpcomingDischargeCase }> = ({
  children,
  record,
}) => {
  const { caseStore } = useDataStore();

  const dischargeSource = record.updatedBy
    ? `Set by ${caseStore.officers[record.updatedBy]?.name ?? record.updatedBy}`
    : "According to TOMIS";

  return (
    <Tooltip
      title={<DischargeDateTooltipContent dischargeSource={dischargeSource} />}
    >
      {children}
    </Tooltip>
  );
};

export default observer(DischargeTooltip);
