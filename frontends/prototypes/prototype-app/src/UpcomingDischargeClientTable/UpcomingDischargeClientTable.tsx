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
import { Pill } from "../Pill";
import { useDataStore } from "../StoreProvider";
import { daysFromNow } from "../utils";

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
  color: ${palette.pine1};

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

const DischargeDate = styled.span`
  color: ${palette.slate85};
`;

const DischargeDateCell: React.FC<{ record: UpcomingDischargeCase }> = ({
  record,
}) => {
  if (!record.expectedDischargeDate) {
    return <>No date in TOMIS</>;
  }

  const daysAway = daysFromNow(record.expectedDischargeDate.toDate());

  const daysString =
    daysAway > 0 ? `In ${daysAway} days` : `${-daysAway} days ago`;

  return (
    <>
      {daysString}{" "}
      <DischargeDate>
        ({record.expectedDischargeDate.toDate().toLocaleDateString("en-US")})
      </DischargeDate>
    </>
  );
};

type UpcomingDischargeClientTableProps = {
  showUnknownDates?: boolean;
};

const UpcomingDischargeClientTable: React.FC<
  UpcomingDischargeClientTableProps
> = ({ showUnknownDates = false }) => {
  const { caseStore } = useDataStore();

  return (
    <Table>
      <thead>
        <tr>
          <HeaderCell>Name</HeaderCell>
          <HeaderCell />
          <HeaderCell>Supervision Type</HeaderCell>
          <HeaderCell>Expiration date</HeaderCell>
          <HeaderCell>Officer</HeaderCell>
        </tr>
      </thead>
      <tbody>
        {caseStore.upcomingDischargeCases
          .filter((record) =>
            showUnknownDates
              ? !record.expectedDischargeDate
              : record.expectedDischargeDate &&
                daysFromNow(record.expectedDischargeDate.toDate()) <= 180
          )
          .map((record) => (
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
              <Cell>{record.supervisionType}</Cell>
              <Cell>
                <DischargeDateCell record={record} />
              </Cell>
              <Cell>{record.officerName}</Cell>
            </Row>
          ))}
      </tbody>
    </Table>
  );
};

export default observer(UpcomingDischargeClientTable);
