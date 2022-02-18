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

import { Icon, IconSVG, palette, spacing } from "@recidiviz/design-system";
import { observer } from "mobx-react-lite";
import React from "react";
import styled from "styled-components/macro";

import { CompliantReportingCase } from "../DataStores/CaseStore";
import DisplayDate from "../DisplayDate";
import LastUpdated from "../LastUpdated";
import { Pill } from "../Pill";
import { useDataStore } from "../StoreProvider";
import Tooltip from "../Tooltip";
import { formatTimestampRelative } from "../utils";

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
  line-height: 1.3;

  &:nth-of-type(2) {
    border-right: 1px solid ${palette.slate20};
  }
`;

const StatusWrapper = styled.div`
  background-color: ${palette.signal.links};
  color: ${palette.white};
  display: flex;
  justify-content: space-between;
  align-items: center;
  height: 100%;
  width: 100%;
  padding: 8px;
  border-radius: 2px;

  svg {
    flex: 0 0 auto;
    margin-left: 8px;
  }
`;

const StatusCell = observer(
  ({ record }: { record: CompliantReportingCase }): JSX.Element => {
    const { status, statusUpdated } = record;
    let statusText;
    switch (status) {
      case "ELIGIBLE":
        statusText = "Eligible";
        break;
      case "DENIED":
        statusText =
          record.deniedReasons?.join(", ") || "Denied, no reason given";
        break;
      default:
        statusText = "Review Required";
        break;
    }

    const statusDisplay = (
      <StatusWrapper>
        {statusText}
        <Icon kind={IconSVG.Arrow} size={14} color={palette.white} />
      </StatusWrapper>
    );

    return statusUpdated ? (
      <Tooltip title={<LastUpdated updated={statusUpdated} />}>
        {statusDisplay}
      </Tooltip>
    ) : (
      statusDisplay
    );
  }
);

const CompliantReportingClientTable: React.FC = () => {
  const { caseStore } = useDataStore();
  return (
    <Table>
      <thead>
        <tr>
          <HeaderCell>Name</HeaderCell>
          <HeaderCell />
          <HeaderCell>Status</HeaderCell>
          <HeaderCell>Supervision Type</HeaderCell>
          <HeaderCell>Supervision Level</HeaderCell>
          <HeaderCell>Offense Type</HeaderCell>
          <HeaderCell>Negative Drug Screens (past 12 mo)</HeaderCell>
          <HeaderCell>Last Sanction (past 12 mo)</HeaderCell>
          <HeaderCell>Officer</HeaderCell>
          <HeaderCell>Judicial District</HeaderCell>
        </tr>
      </thead>
      <tbody>
        {caseStore.compliantReportingCases.map((record) => (
          <Row
            key={record.personExternalId}
            onClick={() => caseStore.setActiveClient(record.personExternalId)}
          >
            <Cell>{record.personName} </Cell>
            <Cell>
              {record.updateCount > 0 && (
                <Pill kind="neutral" filled>
                  {record.updateCount}
                </Pill>
              )}
            </Cell>
            <Cell>
              <StatusCell record={record} />
            </Cell>
            <Cell>{record.supervisionType}</Cell>
            <Cell>
              {record.supervisionLevel} for{" "}
              {formatTimestampRelative(record.supervisionLevelStart)}
            </Cell>
            <Cell>
              {record.offenseType.map((o, i, arr) => (
                <>
                  {o}
                  {i !== arr.length - 1 && <br />}
                </>
              ))}
            </Cell>
            <Cell>
              {record.lastDrun.map((d, i, arr) => (
                <>
                  <DisplayDate date={d.toDate()} />
                  {i !== arr.length - 1 && <br />}
                </>
              ))}
            </Cell>
            <Cell>{record.lastSanction || "No previous sanctions"}</Cell>
            <Cell>{record.officerName}</Cell>
            <Cell>{record.judicialDistrict}</Cell>
          </Row>
        ))}
      </tbody>
    </Table>
  );
};

export default observer(CompliantReportingClientTable);
