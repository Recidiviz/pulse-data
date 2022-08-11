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

import { Spin, Table } from "antd";
import classNames from "classnames";
import { getAllIngestInstanceStatuses } from "../../AdminPanelAPI";
import { useFetchedDataJSON } from "../../hooks";
import { INGEST_ACTIONS_ROUTE } from "../../navigation/IngestOperations";
import NewTabLink from "../NewTabLink";
import { IngestInstanceStatusResponse } from "./constants";
import { getStatusBoxColor } from "./ingestUtils";

const IngestInstanceCurrentStatusSummary = (): JSX.Element => {
  const { loading, data: ingestInstanceStatuses } =
    useFetchedDataJSON<IngestInstanceStatusResponse>(
      getAllIngestInstanceStatuses
    );

  if (loading) {
    return (
      <div className="center">
        <Spin size="large" />
      </div>
    );
  }

  if (ingestInstanceStatuses === undefined) {
    throw new Error(
      "Expected ingestInstanceStatuses to be defined once loading is complete."
    );
  }

  const dataSource = Object.keys(ingestInstanceStatuses).map((key) => {
    const stateCode = ingestInstanceStatuses[key];
    const statuses = Object.values(stateCode);

    return {
      stateCode: key,
      primary: statuses[0] == null ? "No recorded statuses" : statuses[0],
      secondary: statuses[1] == null ? "No recorded statuses" : statuses[1],
    };
  });

  const renderStatusColor = (status: string) => {
    const statusColorClassName = getStatusBoxColor(status);

    return <div className={classNames(statusColorClassName)}>{status}</div>;
  };

  const columns = [
    {
      title: "State Code",
      dataIndex: "stateCode",
      key: "stateCode",
      render: (stateCode: string) => (
        <NewTabLink href={`${INGEST_ACTIONS_ROUTE}?stateCode=${stateCode}`}>
          {stateCode}
        </NewTabLink>
      ),
    },
    {
      title: "Primary Instance Status",
      dataIndex: "primary",
      key: "primary",
      render: (primary: string) => <span>{renderStatusColor(primary)}</span>,
    },
    {
      title: "Secondary Instance Status",
      dataIndex: "secondary",
      key: "secondary",
      render: (secondary: string) => (
        <span>{renderStatusColor(secondary)}</span>
      ),
    },
  ];
  return (
    <>
      <Table dataSource={dataSource} columns={columns} />
    </>
  );
};

export default IngestInstanceCurrentStatusSummary;
