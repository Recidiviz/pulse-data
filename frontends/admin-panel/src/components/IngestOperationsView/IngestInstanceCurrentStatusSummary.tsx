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
import { useEffect, useState } from "react";
import { useHistory } from "react-router-dom";
import {
  getAllIngestInstanceDataflowEnabledStatuses,
  getAllIngestInstanceStatuses,
  getIngestQueuesState,
} from "../../AdminPanelAPI";
import { useFetchedDataJSON } from "../../hooks";
import {
  INGEST_ACTIONS_PRIMARY_ROUTE,
  INGEST_ACTIONS_ROUTE,
} from "../../navigation/IngestOperations";
import {
  IngestInstanceStatusInfo,
  IngestInstanceStatusResponse,
  QueueMetadata,
  QueueState,
  StateIngestQueuesStatuses,
} from "../IngestDataflow/constants";
import NewTabLink from "../NewTabLink";
import StateSelectorPageHeader from "../general/StateSelectorPageHeader";
import { StateCodeInfo } from "../general/constants";
import {
  IngestInstanceDataflowEnabledStatusResponse,
  IngestInstanceStatusTableInfo,
} from "./constants";

import {
  getDataflowEnabledSortedOrder,
  getIngestQueuesCumalativeState,
  getQueueColor,
  getQueueStatusSortedOrder,
  getStatusSortedOrder,
  renderStatusCell,
} from "../IngestDataflow/ingestStatusUtils";

const IngestInstanceCurrentStatusSummary = (): JSX.Element => {
  const history = useHistory();
  const { loading, data: ingestInstanceStatuses } =
    useFetchedDataJSON<IngestInstanceStatusResponse>(
      getAllIngestInstanceStatuses
    );

  const { data: stateDataflowEnabledStatuses } =
    useFetchedDataJSON<IngestInstanceDataflowEnabledStatusResponse>(
      getAllIngestInstanceDataflowEnabledStatuses
    );

  const [stateIngestQueueStatuses, setStateIngestQueueStatuses] =
    useState<StateIngestQueuesStatuses | undefined>(undefined);
  useEffect(() => {
    if (ingestInstanceStatuses === undefined) {
      return;
    }
    const loadAllStatesQueueStatuses = async (): Promise<void> => {
      const stateCodes = Object.keys(ingestInstanceStatuses);
      // For each state code, request info about all ingest queues for that state
      const queueStatusResponses = await Promise.all(
        stateCodes.map(async (stateCode) => getIngestQueuesState(stateCode))
      );
      // Extract JSON data from each of those responses
      const queueStatusResponsesJson = await Promise.all(
        queueStatusResponses.map(
          async (queueStatusResponse): Promise<QueueMetadata[]> =>
            queueStatusResponse.json()
        )
      );
      // For each list of queue info objects, collapse into a single summary
      // QueueState for that state.
      const queueStatusSummaries: QueueState[] = queueStatusResponsesJson.map(
        (queueInfos): QueueState => {
          return getIngestQueuesCumalativeState(queueInfos);
        }
      );
      // Turn lists back into map of stateCode -> QueueStatus
      const stateCodeToQueueStatus = Object.fromEntries(
        stateCodes.map((stateCode, index) => {
          return [stateCode, queueStatusSummaries[index]];
        })
      );
      setStateIngestQueueStatuses(stateCodeToQueueStatus);
    };
    loadAllStatesQueueStatuses();
  }, [ingestInstanceStatuses]);

  if (loading) {
    return (
      <div className="center">
        <Spin size="large" />
      </div>
    );
  }

  if (ingestInstanceStatuses === undefined) {
    return (
      <>
        <Alert
          message="Failed to load ingest instance summary info."
          type="error"
        />
      </>
    );
  }

  const dataSource = Object.keys(ingestInstanceStatuses).map((key) => {
    const stateInstanceStatuses = ingestInstanceStatuses[key];
    const primaryInstanceInfo: IngestInstanceStatusInfo =
      stateInstanceStatuses.primary;

    const primaryStatus: string = primaryInstanceInfo.status;

    const secondaryInstanceInfo: IngestInstanceStatusInfo =
      stateInstanceStatuses.secondary;

    const secondaryStatus: string = secondaryInstanceInfo.status;

    const primaryTimestamp: string = primaryInstanceInfo.statusTimestamp;
    const secondaryTimestamp: string = secondaryInstanceInfo.statusTimestamp;

    return {
      stateCode: key,
      primary: primaryStatus,
      secondary: secondaryStatus,
      queueInfo: stateIngestQueueStatuses
        ? stateIngestQueueStatuses[key]
        : undefined,
      dataflowEnabledPrimary: stateDataflowEnabledStatuses
        ? stateDataflowEnabledStatuses[key].primary
        : undefined,
      dataflowEnabledSecondary: stateDataflowEnabledStatuses
        ? stateDataflowEnabledStatuses[key].secondary
        : undefined,
      timestampPrimary: primaryTimestamp,
      timestampSecondary: secondaryTimestamp,
    };
  });

  const renderIngestQueuesCell = (queueInfo: string | undefined) => {
    if (queueInfo === undefined) {
      return <Spin />;
    }
    const queueColor = getQueueColor(queueInfo);

    return <div className={classNames(queueColor)}>{queueInfo}</div>;
  };

  const renderDataflowEnabledCell = (enabled: boolean | undefined) => {
    if (enabled === undefined) {
      return <Spin />;
    }
    const enabledColor = enabled ? "dataflow-enabled" : "dataflow-disabled";
    const enabledInfo = enabled ? "YES" : "NO";

    return <div className={classNames(enabledColor)}>{enabledInfo}</div>;
  };

  const columns: ColumnsType<IngestInstanceStatusTableInfo> = [
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

      render: (value, record: IngestInstanceStatusTableInfo) => (
        <span>{renderStatusCell(record.primary, record.timestampPrimary)}</span>
      ),
      sorter: (a, b) =>
        getStatusSortedOrder().indexOf(a.primary) -
        getStatusSortedOrder().indexOf(b.primary),
    },
    {
      title: "Secondary Instance Status",
      dataIndex: "secondary",
      key: "secondary",
      render: (value, record: IngestInstanceStatusTableInfo) => (
        <span>
          {renderStatusCell(record.secondary, record.timestampSecondary)}
        </span>
      ),
      sorter: (a, b) =>
        getStatusSortedOrder().indexOf(a.secondary) -
        getStatusSortedOrder().indexOf(b.secondary),
    },
    {
      title: "Queue Status",
      dataIndex: "queueInfo",
      key: "queueInfo",
      render: (queueInfo: string | undefined) => (
        <span>{renderIngestQueuesCell(queueInfo)}</span>
      ),
      sorter: (a, b) =>
        getQueueStatusSortedOrder(a.queueInfo) -
        getQueueStatusSortedOrder(b.queueInfo),
    },
    // TODO(#20930): Delete this column once ingest in dataflow enabled for all states
    {
      title: "Dataflow Enabled? (Primary)",
      dataIndex: "dataflowEnabledPrimary",
      key: "dataflowEnabledPrimary",
      render: (dataflowEnabled: boolean | undefined) => (
        <span>{renderDataflowEnabledCell(dataflowEnabled)}</span>
      ),
      sorter: (a, b) =>
        getDataflowEnabledSortedOrder(a.dataflowEnabledPrimary) -
        getDataflowEnabledSortedOrder(b.dataflowEnabledPrimary),
    },
    {
      title: "Dataflow Enabled? (Secondary)",
      dataIndex: "dataflowEnabledSecondary",
      key: "dataflowEnabledSecondary",
      render: (dataflowEnabled: boolean | undefined) => (
        <span>{renderDataflowEnabledCell(dataflowEnabled)}</span>
      ),
      sorter: (a, b) =>
        getDataflowEnabledSortedOrder(a.dataflowEnabledSecondary) -
        getDataflowEnabledSortedOrder(b.dataflowEnabledSecondary),
    },
  ];

  const stateCodeChange = (value: StateCodeInfo) => {
    history.push(
      INGEST_ACTIONS_PRIMARY_ROUTE.replace(":stateCode", value.code)
    );
  };

  return (
    <>
      <StateSelectorPageHeader
        title="Ingest Status Summary (Legacy)"
        stateCode={null}
        onChange={stateCodeChange}
      />
      <Layout className="content-side-padding">
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
