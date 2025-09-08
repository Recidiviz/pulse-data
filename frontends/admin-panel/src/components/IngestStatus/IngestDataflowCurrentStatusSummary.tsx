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
import { useHistory } from "react-router-dom";

import {
  fetchIngestStateCodes,
  getAllLatestDataflowJobs,
  getAllLatestRawDataImportRunInfo,
  getAllLatestRawDataResourceLockInfo,
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
  RawDataImportRunStatusInfo,
  RawDataImportRunStatusResponse,
  RawDataResourceLockStatuses,
  RawDataResourceLockStatusesResponse,
} from "./constants";
import {
  getJobMetadataForCell,
  getRawDataImportRunStatusSortedOrder,
  getRawDataResourceLockStateSortedOrder,
  renderDataflowStatusCell,
  renderRawDataImportRunStatusCell,
  renderRawDataResourceLockStatusesCell,
} from "./ingestStatusUtils";

export type IngestInstanceDataflowStatusTableInfo = {
  stateCode: string;
  ingestPipelineStatus?: DataflowJobStatusMetadata;
  rawDataImportRunStatus?: RawDataImportRunStatusInfo;
  rawDataResourceLockStatus?: RawDataResourceLockStatuses;
};

const IngestDataflowCurrentStatusSummary = (): JSX.Element => {
  const history = useHistory();

  const { loading: loadingStateCodeInfos, data: stateCodeInfos } =
    useFetchedDataJSON<StateCodeInfo[]>(fetchIngestStateCodes);

  const { loading: dataflowPipelinesLoading, data: dataflowPipelines } =
    useFetchedDataJSON<DataflowIngestPipelineJobResponse>(
      getAllLatestDataflowJobs
    );

  const {
    loading: loadingRawDataImportRunStatuses,
    data: rawDataImportRunStatuses,
  } = useFetchedDataJSON<RawDataImportRunStatusResponse>(
    getAllLatestRawDataImportRunInfo
  );

  const {
    loading: loadingRawDataResourceLockStatuses,
    data: rawDataResourceLockStatuses,
  } = useFetchedDataJSON<RawDataResourceLockStatusesResponse>(
    getAllLatestRawDataResourceLockInfo
  );

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
      const rawDataImportRunStatus = rawDataImportRunStatuses
        ? rawDataImportRunStatuses[key]
        : undefined;

      const rawDataResourceLockStatus = rawDataResourceLockStatuses
        ? rawDataResourceLockStatuses[key]
        : undefined;

      return {
        stateCode: key,
        ingestPipelineStatus: getJobMetadataForCell(key, dataflowPipelines),
        rawDataImportRunStatus,
        rawDataResourceLockStatus,
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
      width: "10%",
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
      width: "30%",
    },
    {
      title: "Ingest Pipeline Status",
      dataIndex: "ingestPipelineStatus",
      key: "ingestPipelineStatus",
      render: (ingestPipelineStatus: DataflowJobStatusMetadata) => (
        <span>{renderDataflowStatusCell(ingestPipelineStatus)}</span>
      ),
      width: "30%",
    },
    {
      title: "Raw Data Import Blocked by Manual Hold?",
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
      width: "30%",
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
          rowKey={(record) => record.stateCode}
        />
      </Layout>
    </>
  );
};

export default IngestDataflowCurrentStatusSummary;
