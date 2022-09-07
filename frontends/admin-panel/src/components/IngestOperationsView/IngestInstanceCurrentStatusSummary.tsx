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

import { Alert, Layout, Spin, Table } from "antd";
import { ColumnsType } from "antd/lib/table";
import classNames from "classnames";
import { useHistory } from "react-router-dom";
import { getAllIngestInstanceStatuses } from "../../AdminPanelAPI";
import { useFetchedDataJSON } from "../../hooks";
import {
  INGEST_ACTIONS_PRIMARY_ROUTE,
  INGEST_ACTIONS_ROUTE,
} from "../../navigation/IngestOperations";
import NewTabLink from "../NewTabLink";
import {
  IngestInstanceStatusInfo,
  IngestInstanceStatusResponse,
  StateCodeInfo,
} from "./constants";
import IngestPageHeader from "./IngestPageHeader";
import {
  getStatusBoxColor,
  getStatusMessage,
  getStatusSortedOrder,
} from "./ingestStatusUtils";

const IngestInstanceCurrentStatusSummary = (): JSX.Element => {
  const history = useHistory();
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
    const stateInstanceStatuses = ingestInstanceStatuses[key];
    const primaryInstanceInfo: IngestInstanceStatusInfo | undefined =
      stateInstanceStatuses.primary;

    const primaryStatus: string = !primaryInstanceInfo?.status
      ? "No recorded statuses"
      : primaryInstanceInfo?.status;

    const secondaryInstanceInfo: IngestInstanceStatusInfo | undefined =
      stateInstanceStatuses.secondary;

    const secondaryStatus: string = !secondaryInstanceInfo?.status
      ? "No recorded statuses"
      : secondaryInstanceInfo?.status;

    return {
      stateCode: key,
      primary: primaryStatus,
      secondary: secondaryStatus,
    };
  });

  const renderStatusCell = (status: string) => {
    const statusColorClassName = getStatusBoxColor(status);
    const statusMessage = getStatusMessage(status);

    return (
      <div className={classNames(statusColorClassName)}>{statusMessage}</div>
    );
  };

  const columns: ColumnsType<{
    stateCode: string;
    primary: string;
    secondary: string;
  }> = [
    {
      title: "State Code",
      dataIndex: "stateCode",
      key: "stateCode",
      render: (stateCode: string) => (
        <NewTabLink href={`${INGEST_ACTIONS_ROUTE}?stateCode=${stateCode}`}>
          {stateCode}
        </NewTabLink>
      ),
      sorter: (a, b) => a.stateCode.localeCompare(b.stateCode),
      defaultSortOrder: "ascend",
    },
    {
      title: "Primary Instance Status",
      dataIndex: "primary",
      key: "primary",

      render: (primary: string) => <span>{renderStatusCell(primary)}</span>,
      sorter: (a, b) =>
        getStatusSortedOrder().indexOf(a.primary) -
        getStatusSortedOrder().indexOf(b.primary),
    },
    {
      title: "Secondary Instance Status",
      dataIndex: "secondary",
      key: "secondary",
      render: (secondary: string) => <span>{renderStatusCell(secondary)}</span>,
      sorter: (a, b) =>
        getStatusSortedOrder().indexOf(a.secondary) -
        getStatusSortedOrder().indexOf(b.secondary),
    },
  ];

  const stateCodeChange = (value: StateCodeInfo) => {
    history.push(
      INGEST_ACTIONS_PRIMARY_ROUTE.replace(":stateCode", value.code)
    );
  };

  return (
    <>
      <IngestPageHeader onChange={stateCodeChange} />
      <Layout style={{ padding: "0 24px 24px" }}>
        <Alert
          message="Select a region to view region-specific ingest details"
          type="warning"
          showIcon
        />
        <Table
          dataSource={dataSource}
          columns={columns}
          pagination={{
            hideOnSinglePage: true,
            size: "small",
            showSizeChanger: true,
            defaultPageSize: 25,
            pageSizeOptions: ["25", "50", "100"],
          }}
        />
      </Layout>
    </>
  );
};

export default IngestInstanceCurrentStatusSummary;
