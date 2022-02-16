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
import { format, formatDistanceToNowStrict } from "date-fns";
import { observer } from "mobx-react-lite";
import React from "react";
import styled from "styled-components/macro";

import { CompliantReportingCase } from "../DataStores/CaseStore";
import { useDataStore } from "../StoreProvider";

const Table = styled.table`
  border-collapse: collapse;
  margin-top: ${spacing.xxl}px;
`;

const HeaderCell = styled.th`
  color: ${palette.slate85};
  padding: ${spacing.lg}px;
  text-align: left;
`;

const Row = styled.tr`
  border-color: ${palette.slate20};
  border-style: solid;
  border-width: 1px 0;
  cursor: pointer;

  &:hover {
    background-color: ${palette.signal.links}0A;
  }
`;

const Cell = styled.td`
  padding: ${spacing.lg}px;

  &:first-of-type {
    border-right: 1px solid ${palette.slate20};
  }
`;

const StatusCell = observer(
  ({ record }: { record: CompliantReportingCase }): JSX.Element => {
    const { status } = record;
    if (status === "ELIGIBLE") {
      return <span>Eligible</span>;
    }
    if (status === "DENIED") {
      return <span>{record.deniedReasons?.join(", ")}</span>;
    }
    return <span>Review Required</span>;
  }
);

const CompliantReportingClientTable: React.FC = () => {
  const { caseStore } = useDataStore();
  return (
    <Table>
      <thead>
        <tr>
          <HeaderCell>Client</HeaderCell>
          <HeaderCell>Status</HeaderCell>
          <HeaderCell>Supervision Level</HeaderCell>
          <HeaderCell>Offense Type</HeaderCell>
          <HeaderCell>Drug Screens past year</HeaderCell>
          <HeaderCell>Sanctions past year</HeaderCell>
        </tr>
      </thead>
      <tbody>
        {caseStore.compliantReportingCases.map((record) => (
          <Row
            key={record.personExternalId}
            onClick={() => caseStore.setActiveClient(record.personExternalId)}
          >
            <Cell>{record.personName}</Cell>
            <Cell>
              <StatusCell record={record} />
            </Cell>
            <Cell>
              {record.supervisionLevel} for{" "}
              {formatDistanceToNowStrict(record.supervisionLevelStart.toDate())}
            </Cell>
            <Cell>{record.offenseType}</Cell>
            <Cell>
              {record.lastDrun
                .map((d) => `DRUN on ${format(d.toDate(), "M/d/yyyy")}`)
                .join(", ")}
            </Cell>
            <Cell>{record.sanctionsPast1Yr || "No previous sanctions"}</Cell>
          </Row>
        ))}
      </tbody>
    </Table>
  );
};

export default observer(CompliantReportingClientTable);
