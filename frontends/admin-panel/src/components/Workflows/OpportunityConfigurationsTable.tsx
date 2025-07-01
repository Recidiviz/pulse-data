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
import { Alert, Spin, Table } from "antd";
import { compareAsc } from "date-fns";
import { observer } from "mobx-react-lite";
import { Link } from "react-router-dom";
import styled from "styled-components/macro";

import { OpportunityConfiguration } from "../../WorkflowsStore/models/OpportunityConfiguration";
import OpportunityConfigurationPresenter from "../../WorkflowsStore/presenters/OpportunityConfigurationPresenter";
import { useWorkflowsStore } from "../StoreProvider";
import { buildColumns, buildRoute } from "./utils";

const TableContainer = styled.div`
  padding-top: 1rem;
`;

const OpportunityConfigurationsTable = ({
  presenter,
}: {
  presenter: OpportunityConfigurationPresenter;
}): JSX.Element | null => {
  const store = useWorkflowsStore();
  const { opportunityConfigurations } = presenter;

  if (!opportunityConfigurations) return null;

  const columns = buildColumns<OpportunityConfiguration>({
    variantDescription: {
      title: "",
      render: (variantDescription, { stateCode, id }) => (
        <Link to={buildRoute(stateCode, store.selectedOpportunityType, id)}>
          {variantDescription}
        </Link>
      ),
    },
    featureVariant: {
      title: "Feature Variant",
    },
    status: {
      title: "Status",
      filters: [
        {
          text: "ACTIVE",
          value: "ACTIVE",
        },
        {
          text: "INACTIVE",
          value: "INACTIVE",
        },
      ],
      defaultFilteredValue: ["ACTIVE"],
    },
    revisionDescription: {
      title: "Revision",
    },
    createdAt: {
      title: "Updated",
      dataIndex: undefined,
      sorter: (a, b) => compareAsc(a.createdAt, b.createdAt),
      render: (opp) =>
        `${new Date(`${opp.createdAt}Z`).toLocaleString("en-US")} by ${
          opp.createdBy
        }`,
    },
  });

  return (
    <TableContainer>
      <Table
        columns={columns}
        dataSource={opportunityConfigurations}
        rowKey="id"
        scroll={
          opportunityConfigurations.length ? { x: "max-content" } : undefined
        }
      />
    </TableContainer>
  );
};

const OpportunityConfigurationsTableContainer = ({
  presenter,
}: {
  presenter: OpportunityConfigurationPresenter;
}): JSX.Element => {
  const { opportunityConfigurations, hydrationState } = presenter;

  if (hydrationState.status === "loading") {
    return (
      <div className="center">
        <Spin size="large" />
      </div>
    );
  }

  if (hydrationState.status === "failed") {
    return (
      <div className="center">
        <Alert
          message="Error"
          description={hydrationState.error.message}
          type="error"
          closable
        />
      </div>
    );
  }

  if (hydrationState.status !== "hydrated" || !opportunityConfigurations) {
    return <div />;
  }

  return <OpportunityConfigurationsTable presenter={presenter} />;
};

export default observer(OpportunityConfigurationsTableContainer);
