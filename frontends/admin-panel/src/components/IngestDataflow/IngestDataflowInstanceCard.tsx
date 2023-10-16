// Recidiviz - a data platform for criminal justice reform
// Copyright (C) 2021 Recidiviz, Inc.
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
import { InfoCircleOutlined } from "@ant-design/icons";
import { Alert, Card, Descriptions, Spin, Table, Tooltip } from "antd";
import { ColumnsType } from "antd/lib/table";
import Title from "antd/lib/typography/Title";
import moment from "moment";
import { useCallback } from "react";
import {
  getLatestDataflowJobByInstance,
  getLatestDataflowRawDataWatermarks,
  getLatestRunIngestViewResults,
  getLatestRunStateDatasetRowCounts,
} from "../../AdminPanelAPI/IngestOperations";
import { useFetchedDataJSON } from "../../hooks";
import { DirectIngestInstance } from "../IngestOperationsView/constants";
import NewTabLink from "../NewTabLink";
import { formatDatetimeFromTimestamp } from "../Utilities/GeneralUtilities";
import {
  ANCHOR_DATAFLOW_LATEST_JOB,
  DataflowIngestPipelineStatus,
  DataflowIngestRawDataWatermarks,
  IngestViewResultRowCounts,
  JobState,
  StateDatasetRowCounts,
} from "./constants";

interface RawFileTagColumns {
  fileTag: string;
  lastRunDate: Date;
}

interface IngestViewResultsColumns {
  ingestViewName: string;
  numRows: number;
}
interface StateDatasetColumns {
  dataset: string;
  numRows: number;
}

interface IngestDataflowInstanceCardProps {
  instance: DirectIngestInstance;
  env: string;
  stateCode: string;
}

function displayJobState(status: string): JobState {
  if (status === "JOB_STATE_DONE") {
    return JobState.SUCCEEDED;
  }
  if (status === "JOB_STATE_FAILED") {
    return JobState.FAILED;
  }
  throw new Error(`Found unknown job state ${status}`);
}

const IngestDataflowInstanceCard: React.FC<IngestDataflowInstanceCardProps> = ({
  instance,
  env,
  stateCode,
}) => {
  const isProduction = window.RUNTIME_GCP_ENVIRONMENT === "production";

  const fetchDataflowPipelineInstance = useCallback(async () => {
    return getLatestDataflowJobByInstance(stateCode, instance);
  }, [stateCode, instance]);

  const {
    loading: mostRecentPipelineInfoLoading,
    data: mostRecentPipelineInfo,
  } = useFetchedDataJSON<DataflowIngestPipelineStatus>(
    fetchDataflowPipelineInstance
  );

  const fetchRawDataWatermarks = useCallback(async () => {
    return getLatestDataflowRawDataWatermarks(stateCode, instance);
  }, [stateCode, instance]);

  const { loading: loadingWatermarks, data: latestRawDataWatermarks } =
    useFetchedDataJSON<DataflowIngestRawDataWatermarks>(fetchRawDataWatermarks);

  const fetchIngestViewResults = useCallback(async () => {
    return getLatestRunIngestViewResults(stateCode, instance);
  }, [stateCode, instance]);

  const { loading: loadingIngestViewResults, data: latestIngestViewResults } =
    useFetchedDataJSON<IngestViewResultRowCounts>(fetchIngestViewResults);

  const fetchStateDatasetRowCounts = useCallback(async () => {
    return getLatestRunStateDatasetRowCounts(stateCode, instance);
  }, [stateCode, instance]);

  const { loading: loadingStateDatasetCounts, data: latestStateDatasetCounts } =
    useFetchedDataJSON<StateDatasetRowCounts>(fetchStateDatasetRowCounts);

  if (mostRecentPipelineInfoLoading) {
    return (
      <div className="center">
        <Spin size="large" />
      </div>
    );
  }

  if (mostRecentPipelineInfo === undefined) {
    return (
      <>
        <Alert
          message="Failed to load latest ingest pipeline metadata."
          type="error"
        />
      </>
    );
  }

  if (mostRecentPipelineInfo === null) {
    return <Alert message="No latest run found." />;
  }

  const fileTagColumns: ColumnsType<RawFileTagColumns> = [
    {
      title: "File Tag",
      dataIndex: "fileTag",
      key: "fileTag",
      render: (fileTag: string) =>
        isProduction ? (
          <NewTabLink
            href={`https://go/prod-raw-data-file/${stateCode}/${fileTag}`}
          >
            {fileTag}
          </NewTabLink>
        ) : (
          <NewTabLink
            href={`https://go/staging-raw-data-file/${stateCode}/${fileTag}`}
          >
            {fileTag}
          </NewTabLink>
        ),
    },
    {
      title: "Upper bound input data date",
      dataIndex: "lastRunDate",
      key: "lastRunDate",
      defaultSortOrder: "ascend",
      sorter: (a, b) =>
        moment(a.lastRunDate).unix() - moment(b.lastRunDate).unix(),
    },
  ];

  const ingestViewColumns: ColumnsType<IngestViewResultsColumns> = [
    {
      title: "Ingest View Name",
      dataIndex: "ingestViewName",
      key: "ingestViewName",
      render: (ingestViewName: string) =>
        isProduction ? (
          <NewTabLink
            href={`https://go/prod-ingest-view-results/${stateCode}/${instance}/${ingestViewName}`}
          >
            {ingestViewName}
          </NewTabLink>
        ) : (
          <NewTabLink
            href={`https://go/staging-ingest-view-results/${stateCode}/${instance}/${ingestViewName}`}
          >
            {ingestViewName}
          </NewTabLink>
        ),
    },
    {
      title: "Number of Rows",
      dataIndex: "numRows",
      key: "numRows",
    },
  ];

  const stateDatasetColumns: ColumnsType<StateDatasetColumns> = [
    {
      title: "State Dataset Counts",
      dataIndex: "dataset",
      key: "dataset",
      render: (tableName: string) =>
        isProduction ? (
          <NewTabLink
            href={`https://go/prod-state-dataset-table/${stateCode}/${instance}/${tableName}`}
          >
            {tableName}
          </NewTabLink>
        ) : (
          <NewTabLink
            href={`https://go/staging-state-dataset-table/${stateCode}/${instance}/${tableName}`}
          >
            {tableName}
          </NewTabLink>
        ),
    },
    {
      title: "Number of Rows",
      dataIndex: "numRows",
      key: "numRows",
    },
  ];

  return (
    <Card>
      <Title id={ANCHOR_DATAFLOW_LATEST_JOB} level={4}>
        Latest Pipeline Run
      </Title>
      <Descriptions bordered>
        <Descriptions.Item label="Job Name" span={3}>
          {mostRecentPipelineInfo.name}
        </Descriptions.Item>
        <Descriptions.Item label="End State" span={3}>
          {displayJobState(mostRecentPipelineInfo.terminationState)}
        </Descriptions.Item>
        <Descriptions.Item label="Start Time" span={3}>
          {formatDatetimeFromTimestamp(mostRecentPipelineInfo.startTime)}
        </Descriptions.Item>
        <Descriptions.Item label="End Time" span={3}>
          {formatDatetimeFromTimestamp(mostRecentPipelineInfo.terminationTime)}
        </Descriptions.Item>
        <Descriptions.Item label="Duration" span={3}>
          {moment
            .utc(mostRecentPipelineInfo.duration * 1000)
            .format("HH [hours] mm [minutes] ss [seconds]")}
        </Descriptions.Item>
        <Descriptions.Item label="Dataflow Job" span={3}>
          <NewTabLink
            href={`https://console.cloud.google.com/dataflow/jobs/${mostRecentPipelineInfo.location}/${mostRecentPipelineInfo.id}`}
          >
            link
          </NewTabLink>
        </Descriptions.Item>
        <Descriptions.Item label="Logs" span={3}>
          {isProduction ? (
            <NewTabLink
              href={`http://go/prod-ingest-dataflow-logs/${mostRecentPipelineInfo.name}`}
            >
              go/staging-ingest-dataflow-logs/
              {mostRecentPipelineInfo.name}
            </NewTabLink>
          ) : (
            <NewTabLink
              href={`http://go/staging-ingest-dataflow-logs/${mostRecentPipelineInfo.name}`}
            >
              go/prod-ingest-dataflow-logs/
              {mostRecentPipelineInfo.name}
            </NewTabLink>
          )}
        </Descriptions.Item>
      </Descriptions>
      <br />
      <Card
        title="Raw Data Freshness"
        extra={
          <Tooltip title="Each row indicates the version of the raw data that the latest pipeline used">
            <InfoCircleOutlined />
          </Tooltip>
        }
      >
        {latestRawDataWatermarks === undefined ? (
          <>
            <Alert
              message="Failed to load latest watermark data."
              type="error"
            />
          </>
        ) : null}
        <Table
          dataSource={
            latestRawDataWatermarks
              ? Object.entries(latestRawDataWatermarks).map(
                  ([key, value], _) => ({
                    fileTag: key,
                    lastRunDate: value,
                  })
                )
              : undefined
          }
          loading={loadingWatermarks}
          columns={fileTagColumns}
          rowKey={(record: { fileTag: string; lastRunDate: Date }) =>
            record.fileTag
          }
          pagination={{
            hideOnSinglePage: true,
            showSizeChanger: true,
            defaultPageSize: 5,
            size: "small",
          }}
        />
      </Card>
      <br />
      {latestIngestViewResults === undefined ? (
        <>
          <Alert
            message="Failed to load latest ingest view results."
            type="error"
          />
        </>
      ) : null}
      <Card
        title="Ingest View Results"
        extra={
          <Tooltip title="Each row indicates how many rows were generated for the given ingest view during the latest pipeline run">
            <InfoCircleOutlined />
          </Tooltip>
        }
      >
        <Table
          dataSource={
            latestIngestViewResults
              ? Object.entries(latestIngestViewResults).map(
                  ([key, value], _) => ({
                    ingestViewName: key,
                    numRows: value,
                  })
                )
              : undefined
          }
          loading={loadingIngestViewResults}
          columns={ingestViewColumns}
          rowKey={(record: { ingestViewName: string; numRows: number }) =>
            record.ingestViewName
          }
          pagination={{
            hideOnSinglePage: true,
            showSizeChanger: true,
            defaultPageSize: 5,
            size: "small",
          }}
        />
      </Card>
      <br />
      {latestStateDatasetCounts === undefined ? (
        <>
          <Alert
            message="Failed to load latest state dataset counts."
            type="error"
          />
        </>
      ) : null}
      <Card
        title="State Dataset Results"
        extra={
          <Tooltip title="Each row indicates how many rows were generated for the given state dataset table during the latest pipeline run">
            <InfoCircleOutlined />
          </Tooltip>
        }
      >
        <Table
          dataSource={
            latestStateDatasetCounts
              ? Object.entries(latestStateDatasetCounts).map(
                  ([key, value], _) => ({
                    dataset: key,
                    numRows: value,
                  })
                )
              : undefined
          }
          loading={loadingStateDatasetCounts}
          columns={stateDatasetColumns}
          rowKey={(record: { dataset: string; numRows: number }) =>
            record.dataset
          }
          pagination={{
            hideOnSinglePage: true,
            showSizeChanger: true,
            defaultPageSize: 5,
            size: "small",
          }}
        />
      </Card>
    </Card>
  );
};

export default IngestDataflowInstanceCard;
