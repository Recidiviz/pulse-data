// Recidiviz - a data platform for criminal justice reform
// Copyright (C) 2024 Recidiviz, Inc.
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
import { Table } from "antd";
import { compareAsc } from "date-fns";
import { observer } from "mobx-react-lite";
import { Link } from "react-router-dom";
import styled from "styled-components/macro";

import {
  Opportunity,
  updatedStringForOpportunity,
} from "../../WorkflowsStore/models/Opportunity";
import OpportunityPresenter from "../../WorkflowsStore/presenters/OpportunityPresenter";
import { buildColumns, buildRoute } from "./utils";

const TableContainer = styled.div`
  padding-top: 1rem;
`;

const OpportunitiesTable = ({
  presenter,
}: {
  presenter: OpportunityPresenter;
}): JSX.Element | null => {
  const { opportunities } = presenter;

  if (!opportunities) return null;

  const columns = buildColumns<Opportunity>({
    opportunityType: {
      title: "Opportunity Type",
      fixed: "left",
      render: (opportunityType, { stateCode }) => (
        <Link to={buildRoute(stateCode, opportunityType)}>
          {opportunityType}
        </Link>
      ),
    },
    systemType: {
      title: "System Type",
    },
    lastUpdatedAt: {
      title: "Last Updated",
      dataIndex: undefined,
      sorter: (a, b) => compareAsc(a.lastUpdatedAt ?? 0, b.lastUpdatedAt ?? 0),
      render: updatedStringForOpportunity,
    },
    // gatingFeatureVariant: {
    //   title: "Gating Feature Variant",
    // },
    // urlSection: {
    //   title: "URL Section",
    // },
    // completionEvent: {
    //   title: "Completion Event",
    // },
    // experimentId: {
    //   title: "Experiment ID",
    // },
  });

  return (
    <TableContainer>
      <Table
        columns={columns}
        dataSource={opportunities}
        rowKey="opportunityType"
        scroll={opportunities.length ? { x: "max-content" } : undefined}
      />
    </TableContainer>
  );
};

export default observer(OpportunitiesTable);
