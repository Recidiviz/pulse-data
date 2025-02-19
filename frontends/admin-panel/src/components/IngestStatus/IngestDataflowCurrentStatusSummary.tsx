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
  fetchIngestStateCodes,
  getAllIngestInstanceStatuses,
  getAllLatestDataflowJobs,
  getAllLatestRawDataImportRunInfo,
  getAllLatestRawDataResourceLockInfo,
  getIngestQueuesState,
  rawDataImportDagEnabledForAllStates,
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
  RawDataDagEnabledAllResponse,
  RawDataDagEnabledType,
  RawDataImportRunStatusInfo,
  RawDataImportRunStatusResponse,
  RawDataResourceLockStatuses,
  RawDataResourceLockStatusesResponse,
  StateIngestQueuesStatuses,
} from "./constants";
import {
  getIngestQueuesCumulativeState,
  getJobMetadataForCell,
  getLegacyIngestStatusSortedOrder,
  getQueueStatusSortedOrder,
  getRawDataImportRunStatusSortedOrder,
  getRawDataResourceLockStateSortedOrder,
  renderDataflowStatusCell,
  renderIngestQueuesCell,
  renderLegacyIngestStatusCell,
  renderRawDataImportRunStatusCell,
  renderRawDataResourceLockStatusesCell,
} from "./ingestStatusUtils";

export type IngestInstanceDataflowStatusTableInfo = {
  stateCode: string;
  ingestPipelineStatus?: DataflowJobStatusMetadata;
  primaryRawDataStatus?: string;
  secondaryRawDataStatus?: string;
  primaryRawDataTimestamp?: string;
  secondaryRawDataTimestamp?: string;
  queueInfo?: string;
  rawDataImportRunStatus?: RawDataImportRunStatusInfo;
  rawDataResourceLockStatus?: RawDataResourceLockStatuses;
  rawDataEnabled?: RawDataDagEnabledType;
};

const IngestDataflowCurrentStatusSummary = (): JSX.Element => {
  const history = useHistory();

  const { loading: loadingStateCodeInfos, data: stateCodeInfos } =
    useFetchedDataJSON<StateCodeInfo[]>(fetchIngestStateCodes);

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

  const { loading: loadingRawDataDagEnabled, data: rawDataDagEnabledStatus } =
    useFetchedDataJSON<RawDataDagEnabledAllResponse>(
      rawDataImportDagEnabledForAllStates
    );

  const {
    loading: loadingRawDataResourceLockStatuses,
    data: rawDataResourceLockStatuses,
  } = useFetchedDataJSON<RawDataResourceLockStatusesResponse>(
    getAllLatestRawDataResourceLockInfo
  );

  const [stateIngestQueueStatuses, setStateIngestQueueStatuses] = useState<
    StateIngestQueuesStatuses | undefined
  >(undefined);

  useEffect(() => {
    if (stateCodeInfos === undefined) {
      return;
    }
    const loadAllStatesQueueStatuses = async (): Promise<void> => {
      const stateCodes = stateCodeInfos.map(
        (stateCodeInfo) => stateCodeInfo.code
      );
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
  }, [stateCodeInfos]);

  if (loadingStateCodeInfos) {
    return (
      <div className="center">
        <Spin size="large" />
      </div>
    );
  }

  if (stateCodeInfos === undefined) {
    return <Alert message="Failed to load state codes." type="error" />;
  }

  if (!dataflowPipelinesLoading && dataflowPipelines === undefined) {
    return (
      <Alert message="Failed to load ingest pipeline statuses." type="error" />
    );
  }

  if (!loadingRawDataStatuses && rawDataStatuses === undefined) {
    return <Alert message="Failed to load raw data statuses." type="error" />;
  }

  if (!loadingRawDataDagEnabled && rawDataDagEnabledStatus === undefined) {
    return (
      <Alert
        message="Failed to load raw data import dag gating info"
        type="error"
      />
    );
  }

  if (
    !loadingRawDataImportRunStatuses &&
    rawDataImportRunStatuses === undefined
  ) {
    return (
      <Alert
        message="Failed to load raw data import dag statuses"
        type="error"
      />
    );
  }
  if (
    !loadingRawDataResourceLockStatuses &&
    rawDataResourceLockStatuses === undefined
  ) {
    return (
      <Alert
        message="Failed to load raw data resource lock statuses"
        type="error"
      />
    );
  }

  const dataSource: IngestInstanceDataflowStatusTableInfo[] = stateCodeInfos
    .map((info) => info.code)
    .map((key) => {
      const stateRawDataStatuses = rawDataStatuses
        ? rawDataStatuses[key]
        : undefined;
      const queueInfo = stateIngestQueueStatuses
        ? stateIngestQueueStatuses[key]
        : undefined;

      const primaryRawDataStatus: string | undefined =
        stateRawDataStatuses?.primary.status;
      const secondaryRawDataStatus: string | undefined =
        stateRawDataStatuses?.secondary.status;

      const primaryRawDataTimestamp: string | undefined =
        stateRawDataStatuses?.primary.statusTimestamp;
      const secondaryRawDataTimestamp: string | undefined =
        stateRawDataStatuses?.secondary.statusTimestamp;

      const rawDataImportRunStatus = rawDataImportRunStatuses
        ? rawDataImportRunStatuses[key]
        : undefined;

      const rawDataResourceLockStatus = rawDataResourceLockStatuses
        ? rawDataResourceLockStatuses[key]
        : undefined;

      const rawDataEnabled = rawDataDagEnabledStatus
        ? rawDataDagEnabledStatus[key]
        : undefined;

      return {
        stateCode: key,
        ingestPipelineStatus: getJobMetadataForCell(key, dataflowPipelines),
        primaryRawDataStatus,
        secondaryRawDataStatus,
        primaryRawDataTimestamp,
        secondaryRawDataTimestamp,
        queueInfo,
        rawDataImportRunStatus,
        rawDataResourceLockStatus,
        rawDataEnabled,
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
            record.primaryRawDataTimestamp,
            record.rawDataEnabled?.primary
          )}
        </span>
      ),
      sorter: (a, b) => {
        if (
          a.primaryRawDataStatus === undefined ||
          b.primaryRawDataStatus === undefined
        )
          return 0;
        return (
          getLegacyIngestStatusSortedOrder().indexOf(a.primaryRawDataStatus) -
          getLegacyIngestStatusSortedOrder().indexOf(b.primaryRawDataStatus)
        );
      },
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
            record.secondaryRawDataTimestamp,
            record.rawDataEnabled?.secondary
          )}
        </span>
      ),
      sorter: (a, b) => {
        if (
          a.secondaryRawDataStatus === undefined ||
          b.secondaryRawDataStatus === undefined
        )
          return 0;
        return (
          getLegacyIngestStatusSortedOrder().indexOf(a.secondaryRawDataStatus) -
          getLegacyIngestStatusSortedOrder().indexOf(b.secondaryRawDataStatus)
        );
      },
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
      sorter: (a, b) => {
        if (
          a.rawDataImportRunStatus === undefined ||
          b.rawDataImportRunStatus === undefined
        )
          return 0;
        return (
          getRawDataImportRunStatusSortedOrder(a.rawDataImportRunStatus) -
          getRawDataImportRunStatusSortedOrder(b.rawDataImportRunStatus)
        );
      },
    },
    {
      title: "New Raw Data Import Blocked by Manual Hold?",
      dataIndex: "rawDataResourceLockStatus",
      key: "rawDataResourceLockStatus",

      render: (rawDataResourceLockStatus: RawDataResourceLockStatuses) => (
        <span>
          {renderRawDataResourceLockStatusesCell(rawDataResourceLockStatus)}
        </span>
      ),
      sorter: (a, b) => {
        if (
          a.rawDataResourceLockStatus === undefined ||
          b.rawDataResourceLockStatus === undefined
        )
          return 0;
        return (
          getRawDataResourceLockStateSortedOrder(a.rawDataResourceLockStatus) -
          getRawDataResourceLockStateSortedOrder(b.rawDataResourceLockStatus)
        );
      },
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
