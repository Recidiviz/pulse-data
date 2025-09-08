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
import { useCallback, useEffect, useRef, useState } from "react";
import { useLocation } from "react-router-dom";

import {
  getDataflowJobAdditionalMetadataByInstance,
  getIngestInstanceResources,
  getIngestRawFileProcessingStatus,
  getLatestDataflowJob,
  getLatestDataflowRawDataWatermarks,
  getLatestRawDataTagsNotMeetingWatermark,
  getLatestRunIngestViewResults,
  getLatestRunStateDatasetRowCounts,
} from "../../AdminPanelAPI/IngestOperations";
import { useFetchedDataJSON } from "../../hooks";
import { GCP_STORAGE_BASE_URL } from "../general/constants";
import NewTabLink from "../NewTabLink";
import { isAbortException } from "../Utilities/exceptions";
import {
  formatDatetimeFromTimestamp,
  scrollToAnchor,
} from "../Utilities/GeneralUtilities";
import {
  ANCHOR_DATAFLOW_LATEST_JOB,
  ANCHOR_INGEST_RAW_DATA,
  ANCHOR_INGEST_RESOURCES,
  DataflowIngestPipelineAdditionalMetadata,
  DataflowIngestPipelineStatus,
  DataflowIngestRawDataWatermarks,
  DataflowJobState,
  DirectIngestInstance,
  IngestInstanceResources,
  IngestRawFileProcessingStatus,
  IngestViewResultRowCounts,
  StateDatasetRowCounts,
} from "./constants";
import IngestRawFileProcessingStatusTable from "./IngestRawFileProcessingStatusTable";
import InstanceRawFileMetadata from "./InstanceRawFileMetadata";

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

function displayJobState(status: string): DataflowJobState {
  if (status === "JOB_STATE_DONE") {
    return DataflowJobState.SUCCEEDED;
  }
  if (status === "JOB_STATE_FAILED") {
    return DataflowJobState.FAILED;
  }
  throw new Error(`Found unknown job state ${status}`);
}

function bigQueryStateDatasetUrl(
  isProduction: boolean,
  instance: string,
  stateCode: string,
  tableName?: string
): string {
  if (instance !== DirectIngestInstance.PRIMARY) {
    throw new Error(
      `Only valid to get bigQueryStateDatasetUrl for the PRIMARY instance, 
      found ${instance}`
    );
  }
  // Returns a URL for the provided state's `us_xx_state` dataset in the BQ UI, or to a
  // table in that dataset if tableName is provided.
  const base = isProduction
    ? "https://go/prod-state-dataset-table"
    : "https://go/staging-state-dataset-table";

  const datasetURL = `${base}/${stateCode.toLowerCase()}`;
  if (tableName === undefined) {
    return datasetURL;
  }
  return `${datasetURL}/${tableName}`;
}

function bigQueryIngestViewResultsDatasetUrl(
  isProduction: boolean,
  instance: string,
  stateCode: string,
  ingestViewName?: string
): string {
  if (instance !== DirectIngestInstance.PRIMARY) {
    throw new Error(
      `Only valid to get bigQueryIngestViewResultsDatasetUrl for the PRIMARY instance, 
      found ${instance}`
    );
  }

  // Returns a URL for the provided state's ingest view results dataset in the BQ UI,
  // or to a table in that dataset if ingestViewName is provided.
  const base = isProduction
    ? "https://go/prod-ingest-view-results"
    : "https://go/staging-ingest-view-results";

  const datasetURL = `${base}/${stateCode.toLowerCase()}`;
  if (ingestViewName === undefined) {
    return datasetURL;
  }
  return `${datasetURL}/${ingestViewName}`;
}

const IngestDataflowInstanceCard: React.FC<IngestDataflowInstanceCardProps> = ({
  instance,
  env,
  stateCode,
}) => {
  // Enable scrolling to various sections based on the URL
  const { hash } = useLocation();

  useEffect(() => {
    scrollToAnchor(hash);
  }, [hash]);

  const isProduction = window.RUNTIME_GCP_ENVIRONMENT === "production";

  // Uses useRef so abort controller not re-initialized every render cycle.
  const abortControllerRefResources = useRef<AbortController | undefined>(
    undefined
  );
  const abortControllerRefRaw = useRef<AbortController | undefined>(undefined);

  // INGEST DATAFLOW PIPELINE DATA LOADING
  const fetchLatestIngestDataflowJob = useCallback(async () => {
    return getLatestDataflowJob(stateCode);
  }, [stateCode]);

  const {
    loading: mostRecentPipelineInfoLoading,
    data: mostRecentPipelineInfo,
  } = useFetchedDataJSON<DataflowIngestPipelineStatus>(
    fetchLatestIngestDataflowJob
  );

  const fetchDataflowPipelineAdditionalMetadataInstance =
    useCallback(async () => {
      return getDataflowJobAdditionalMetadataByInstance(stateCode, instance);
    }, [stateCode, instance]);
  const { data: datasetNames } =
    useFetchedDataJSON<DataflowIngestPipelineAdditionalMetadata>(
      fetchDataflowPipelineAdditionalMetadataInstance
    );

  // WATERMARKS LOADING
  const fetchRawDataWatermarks = useCallback(async () => {
    return getLatestDataflowRawDataWatermarks(stateCode, instance);
  }, [stateCode, instance]);

  const { loading: loadingWatermarks, data: latestRawDataWatermarks } =
    useFetchedDataJSON<DataflowIngestRawDataWatermarks>(fetchRawDataWatermarks);

  const fetchRawDataTagsNotMeetingWatermark = useCallback(async () => {
    return getLatestRawDataTagsNotMeetingWatermark(stateCode, instance);
  }, [stateCode, instance]);

  const { data: rawFileTagsNotMeetingWatermark } = useFetchedDataJSON<string[]>(
    fetchRawDataTagsNotMeetingWatermark
  );

  // INGEST VIEW RESULTS LOADING
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

  // WATERMARK DETAIL TABLE
  const fileTagColumns: ColumnsType<RawFileTagColumns> = [
    {
      title: "File Tag",
      dataIndex: "fileTag",
      key: "fileTag",
      render: (fileTag: string) =>
        isProduction ? (
          <NewTabLink
            href={`https://go/prod-raw-data-file/${stateCode.toLowerCase()}/${fileTag}`}
          >
            {fileTag}
          </NewTabLink>
        ) : (
          <NewTabLink
            href={`https://go/staging-raw-data-file/${stateCode.toLowerCase()}/${fileTag}`}
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

  // INGEST VIEW DETAIL TABLE
  const ingestViewColumns: ColumnsType<IngestViewResultsColumns> = [
    {
      title: "Ingest View Name",
      dataIndex: "ingestViewName",
      key: "ingestViewName",
      render: (ingestViewName: string) => (
        <NewTabLink
          href={bigQueryIngestViewResultsDatasetUrl(
            isProduction,
            instance,
            stateCode,
            ingestViewName
          )}
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
      render: (tableName: string) => (
        <NewTabLink
          href={bigQueryStateDatasetUrl(
            isProduction,
            instance,
            stateCode,
            tableName
          )}
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

  // INGEST DATAFLOW PIPELINE VIEW
  const renderLatestPipelineInfoView = () => {
    if (mostRecentPipelineInfoLoading) {
      return (
        <div className="center">
          <Spin size="large" />
        </div>
      );
    }

    if (mostRecentPipelineInfo === undefined) {
      return (
        <Alert
          message="Failed to load latest ingest pipeline metadata."
          type="error"
        />
      );
    }

    if (mostRecentPipelineInfo === null) {
      return <Alert message="No latest run found." />;
    }

    return (
      <>
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
            {formatDatetimeFromTimestamp(
              mostRecentPipelineInfo.terminationTime
            )}
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
                go/prod-ingest-dataflow-logs/
                {mostRecentPipelineInfo.name}
              </NewTabLink>
            ) : (
              <NewTabLink
                href={`http://go/staging-ingest-dataflow-logs/${mostRecentPipelineInfo.name}`}
              >
                go/staging-ingest-dataflow-logs/
                {mostRecentPipelineInfo.name}
              </NewTabLink>
            )}
          </Descriptions.Item>
          <Descriptions.Item label="Ingest View Results Output Table" span={3}>
            <NewTabLink
              href={bigQueryIngestViewResultsDatasetUrl(
                isProduction,
                instance,
                stateCode
              )}
            >
              {datasetNames?.ingestViewResultsDatasetName}
            </NewTabLink>
          </Descriptions.Item>
          <Descriptions.Item
            label="State Dataset Results Output Table"
            span={3}
          >
            <NewTabLink
              href={bigQueryStateDatasetUrl(isProduction, instance, stateCode)}
            >
              {datasetNames?.stateResultsDatasetName}
            </NewTabLink>
          </Descriptions.Item>
          <Descriptions.Item label="Ingest DAG" span={3}>
            {isProduction ? (
              <NewTabLink href="http://go/prod-ingest-dag">link</NewTabLink>
            ) : (
              <NewTabLink href="http://go/staging-ingest-dag">link</NewTabLink>
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
            <Alert
              message="Failed to load latest watermark data."
              type="error"
            />
          ) : null}
          {rawFileTagsNotMeetingWatermark !== undefined &&
          rawFileTagsNotMeetingWatermark?.length > 0 ? (
            <Alert
              message={`Some files are stale: ${rawFileTagsNotMeetingWatermark}. This means that the last pipeline that ran used data with fresher update_datetime values than currently exist for these files in ${instance}. If you are sure you want to run ingest pipelines with this older data, you can invalidate the Dataflow job_id associated with these watermarks.

            `}
              type="error"
            />
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
        {latestIngestViewResults === undefined && !loadingIngestViewResults ? (
          <Alert
            message="Failed to load latest ingest view results."
            type="error"
          />
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
        {latestStateDatasetCounts === undefined &&
        !loadingStateDatasetCounts ? (
          <Alert
            message="Failed to load latest state dataset counts."
            type="error"
          />
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
      </>
    );
  };

  // RAW DATA IMPORT DATA LOADING
  const [ingestInstanceResources, setIngestInstanceResources] = useState<
    IngestInstanceResources | undefined
  >(undefined);

  const fetchIngestInstanceResources = useCallback(async () => {
    if (!instance) {
      return;
    }
    if (abortControllerRefResources.current) {
      abortControllerRefResources.current.abort();
      abortControllerRefResources.current = undefined;
    }
    try {
      abortControllerRefResources.current = new AbortController();
      const primaryResponse = await getIngestInstanceResources(
        stateCode,
        instance,
        abortControllerRefResources.current
      );
      const result: IngestInstanceResources = await primaryResponse.json();
      setIngestInstanceResources(result);
    } catch (err) {
      if (!isAbortException(err)) {
        throw err;
      }
    }
  }, [instance, stateCode]);

  useEffect(() => {
    fetchIngestInstanceResources();
  }, [fetchIngestInstanceResources, instance, stateCode]);

  const [
    ingestRawFileProcessingStatusLoading,
    setIngestRawFileProcessingStatusLoading,
  ] = useState<boolean>(true);
  const [ingestRawFileProcessingStatus, setIngestRawFileProcessingStatus] =
    useState<IngestRawFileProcessingStatus[]>([]);

  const getRawFileProcessingStatusData = useCallback(async () => {
    setIngestRawFileProcessingStatusLoading(true);
    if (abortControllerRefRaw.current) {
      abortControllerRefRaw.current.abort();
      abortControllerRefRaw.current = undefined;
    }
    try {
      abortControllerRefRaw.current = new AbortController();
      const response = await Promise.all([
        getIngestRawFileProcessingStatus(
          stateCode,
          instance,
          abortControllerRefRaw.current
        ),
      ]);
      setIngestRawFileProcessingStatus(await response[0].json());
    } catch (err) {
      if (!isAbortException(err)) {
        throw err;
      }
    }

    setIngestRawFileProcessingStatusLoading(false);
  }, [instance, stateCode]);

  useEffect(() => {
    getRawFileProcessingStatusData();
  }, [getRawFileProcessingStatusData, instance, stateCode]);

  // RAW DATA IMPORT DATA VIEWS
  const rawDataImportStatusView = (
    <>
      <InstanceRawFileMetadata
        stateCode={stateCode}
        loading={ingestRawFileProcessingStatusLoading}
        ingestRawFileProcessingStatus={ingestRawFileProcessingStatus}
      />
      <br />
      <IngestRawFileProcessingStatusTable
        ingestInstanceResources={ingestInstanceResources}
        statusLoading={ingestRawFileProcessingStatusLoading}
        ingestRawFileProcessingStatus={ingestRawFileProcessingStatus}
        stateCode={stateCode}
        instance={instance}
      />
    </>
  );

  const rawDataImportResourcesView = (
    <Descriptions bordered>
      <Descriptions.Item label="Raw Data Ingest Bucket" span={3}>
        {!ingestInstanceResources ? (
          <Spin />
        ) : (
          <NewTabLink
            href={GCP_STORAGE_BASE_URL.concat(
              ingestInstanceResources.ingestBucketPath
            )}
          >
            {ingestInstanceResources.ingestBucketPath}
          </NewTabLink>
        )}
      </Descriptions.Item>
      <Descriptions.Item label="Raw Data Storage Bucket" span={3}>
        {!ingestInstanceResources ? (
          <Spin />
        ) : (
          <NewTabLink
            href={GCP_STORAGE_BASE_URL.concat(
              ingestInstanceResources.storageDirectoryPath
            )}
          >
            {ingestInstanceResources.storageDirectoryPath}
          </NewTabLink>
        )}
      </Descriptions.Item>
    </Descriptions>
  );

  // OVERALL COMPONENT STRUCTURE
  return (
    <Card>
      {instance === "PRIMARY" ? (
        <Title id={ANCHOR_DATAFLOW_LATEST_JOB} level={4}>
          Latest Ingest Pipeline Run
        </Title>
      ) : null}
      {instance === "PRIMARY" ? renderLatestPipelineInfoView() : null}
      <br />
      <Title id={ANCHOR_INGEST_RAW_DATA} level={4}>
        Raw Data Import
      </Title>
      {ingestRawFileProcessingStatusLoading ? (
        <div className="center">
          <Spin size="large" />
        </div>
      ) : (
        rawDataImportStatusView
      )}
      <br />
      <Title id={ANCHOR_INGEST_RESOURCES} level={4}>
        Raw Data Import Resources
      </Title>
      {rawDataImportResourcesView}
    </Card>
  );
};

export default IngestDataflowInstanceCard;
