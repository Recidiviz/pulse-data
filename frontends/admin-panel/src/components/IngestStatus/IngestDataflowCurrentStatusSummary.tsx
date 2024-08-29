// Recidiviz - a data platform for criminal justice reform
// Copyright (C) 2023 Recidiviz, Inc.
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
import { useEffect, useState } from "react";
import { useHistory } from "react-router-dom";

import {
  getAllIngestInstanceStatuses,
  getAllLatestDataflowJobs,
  getAllLatestRawDataImportRunInfo,
  getIngestQueuesState,
} from "../../AdminPanelAPI/IngestOperations";
import { useFetchedDataJSON } from "../../hooks";
import {
  INGEST_DATAFLOW_ROUTE,
  INGEST_DATAFLOW_WITH_STATE_CODE_ROUTE,
} from "../../navigation/IngestOperations";
import { StateCodeInfo } from "../general/constants";
import StateSelectorPageHeader from "../general/StateSelectorPageHeader";
import NewTabLink from "../NewTabLink";
import {
  DataflowIngestPipelineJobResponse,
  DataflowJobStatusMetadata,
  IngestInstanceStatusResponse,
  QueueMetadata,
  QueueState,
  RawDataImportRunStatusInfo,
  RawDataImportRunStatusResponse,
  StateIngestQueuesStatuses,
} from "./constants";
import {
  getIngestQueuesCumulativeState,
  getJobMetadataForCell,
  getLegacyIngestStatusSortedOrder,
  getQueueStatusSortedOrder,
  getRawDataImportRunStatusSortedOrder,
  renderDataflowStatusCell,
  renderIngestQueuesCell,
  renderLegacyIngestStatusCell,
  renderRawDataImportRunStatusCell,
} from "./ingestStatusUtils";

export type IngestInstanceDataflowStatusTableInfo = {
  stateCode: string;
  ingestPipelineStatus: DataflowJobStatusMetadata;
  primaryRawDataStatus: string;
  secondaryRawDataStatus: string;
  primaryRawDataTimestamp: string;
  secondaryRawDataTimestamp: string;
  queueInfo: string | undefined;
  rawDataImportRunStatus: RawDataImportRunStatusInfo;
};

const IngestDataflowCurrentStatusSummary = (): JSX.Element => {
  const history = useHistory();
  const { loading: dataflowPipelinesLoading, data: dataflowPipelines } =
    useFetchedDataJSON<DataflowIngestPipelineJobResponse>(
      getAllLatestDataflowJobs
    );

  const { loading: loadingRawDataStatuses, data: rawDataStatuses } =
    useFetchedDataJSON<IngestInstanceStatusResponse>(
      getAllIngestInstanceStatuses
    );

  const {
    loading: loadingRawDataImportRunStatuses,
    data: rawDataImportRunStatuses,
  } = useFetchedDataJSON<RawDataImportRunStatusResponse>(
    getAllLatestRawDataImportRunInfo
  );

  const [stateIngestQueueStatuses, setStateIngestQueueStatuses] = useState<
    StateIngestQueuesStatuses | undefined
  >(undefined);

  useEffect(() => {
    if (rawDataStatuses === undefined) {
      return;
    }
    const loadAllStatesQueueStatuses = async (): Promise<void> => {
      const stateCodes = Object.keys(rawDataStatuses);
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
          return getIngestQueuesCumulativeState(queueInfos);
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
  }, [rawDataStatuses]);

  if (
    dataflowPipelinesLoading ||
    loadingRawDataStatuses ||
    loadingRawDataImportRunStatuses
  ) {
    return (
      <div className="center">
        <Spin size="large" />
      </div>
    );
  }

  if (dataflowPipelines === undefined) {
    return (
      <Alert message="Failed to load ingest pipeline statuses." type="error" />
    );
  }

  if (rawDataStatuses === undefined) {
    return <Alert message="Failed to load raw data statuses." type="error" />;
  }

  if (rawDataImportRunStatuses === undefined) {
    return (
      <Alert
        message="Failed to load raw data import dag statuses"
        type="error"
      />
    );
  }

  const dataSource: IngestInstanceDataflowStatusTableInfo[] = Object.keys(
    dataflowPipelines
  ).map((key) => {
    const stateRawDataStatuses = rawDataStatuses[key];
    const queueInfo = stateIngestQueueStatuses
      ? stateIngestQueueStatuses[key]
      : undefined;

    const primaryRawDataStatus: string = stateRawDataStatuses.primary.status;
    const secondaryRawDataStatus: string =
      stateRawDataStatuses.secondary.status;

    const primaryRawDataTimestamp: string =
      stateRawDataStatuses.primary.statusTimestamp;
    const secondaryRawDataTimestamp: string =
      stateRawDataStatuses.secondary.statusTimestamp;

    const rawDataImportRunStatus = rawDataImportRunStatuses[key];

    return {
      stateCode: key,
      ingestPipelineStatus: getJobMetadataForCell(key, dataflowPipelines),
      primaryRawDataStatus,
      secondaryRawDataStatus,
      primaryRawDataTimestamp,
      secondaryRawDataTimestamp,
      queueInfo,
      rawDataImportRunStatus,
    };
  });

  const columns: ColumnsType<IngestInstanceDataflowStatusTableInfo> = [
    {
      title: "State Code",
      dataIndex: "stateCode",
      key: "stateCode",
      render: (stateCode: string) => (
        <NewTabLink href={`${INGEST_DATAFLOW_ROUTE}/${stateCode}`}>
          {stateCode}
        </NewTabLink>
      ),
      sorter: (a, b) => a.stateCode.localeCompare(b.stateCode),
      defaultSortOrder: "ascend",
    },
    {
      title: "Ingest Pipeline Status",
      dataIndex: "ingestPipelineStatus",
      key: "ingestPipelineStatus",
      render: (ingestPipelineStatus: DataflowJobStatusMetadata) => (
        <span>{renderDataflowStatusCell(ingestPipelineStatus)}</span>
      ),
    },
    // TODO(#28239): remove once the raw data import dag is fully rolled out
    {
      title: "Legacy Raw Data Status (Primary)",
      dataIndex: "primary",
      key: "primary",

      render: (value, record: IngestInstanceDataflowStatusTableInfo) => (
        <span>
          {renderLegacyIngestStatusCell(
            record.primaryRawDataStatus,
            record.primaryRawDataTimestamp
          )}
        </span>
      ),
      sorter: (a, b) =>
        getLegacyIngestStatusSortedOrder().indexOf(a.primaryRawDataStatus) -
        getLegacyIngestStatusSortedOrder().indexOf(b.primaryRawDataStatus),
    },
    // TODO(#28239): remove once the raw data import dag is fully rolled out
    {
      title: "Legacy Raw Data Status (Secondary)",
      dataIndex: "secondary",
      key: "secondary",
      render: (value, record: IngestInstanceDataflowStatusTableInfo) => (
        <span>
          {renderLegacyIngestStatusCell(
            record.secondaryRawDataStatus,
            record.secondaryRawDataTimestamp
          )}
        </span>
      ),
      sorter: (a, b) =>
        getLegacyIngestStatusSortedOrder().indexOf(a.secondaryRawDataStatus) -
        getLegacyIngestStatusSortedOrder().indexOf(b.secondaryRawDataStatus),
    },
    // TODO(#28239): remove once the raw data import dag is fully rolled out
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
    {
      title: "Most Recent Raw Data Import Status",
      dataIndex: "rawDataImportRunStatus",
      key: "rawDataImportRunStatus",

      render: (rawDataImportRunStatus: RawDataImportRunStatusInfo) => (
        <span>{renderRawDataImportRunStatusCell(rawDataImportRunStatus)}</span>
      ),
      sorter: (a, b) =>
        getRawDataImportRunStatusSortedOrder(a.rawDataImportRunStatus) -
        getRawDataImportRunStatusSortedOrder(b.rawDataImportRunStatus),
    },
  ];

  const stateCodeChange = (value: StateCodeInfo) => {
    history.push(
      INGEST_DATAFLOW_WITH_STATE_CODE_ROUTE.replace(":stateCode", value.code)
    );
  };

  return (
    <>
      <StateSelectorPageHeader
        title="Ingest Status Summary"
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

export default IngestDataflowCurrentStatusSummary;
