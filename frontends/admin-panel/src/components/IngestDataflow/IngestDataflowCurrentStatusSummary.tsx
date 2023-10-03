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
import classNames from "classnames";
import { useHistory } from "react-router-dom";
import { getAllIngestInstanceDataflowEnabledStatuses } from "../../AdminPanelAPI";
import { getAllLatestDataflowJobs } from "../../AdminPanelAPI/IngestOperations";
import { useFetchedDataJSON } from "../../hooks";
import {
  INGEST_DATAFLOW_ROUTE,
  INGEST_DATAFLOW_WITH_STATE_CODE_ROUTE,
} from "../../navigation/IngestOperations";
import {
  DirectIngestInstance,
  IngestInstanceDataflowEnabledStatusResponse,
  StateCodeInfo,
} from "../IngestOperationsView/constants";
import NewTabLink from "../NewTabLink";
import StateSelectorPageHeader from "../general/StateSelectorPageHeader";
import {
  DataflowIngestPipelineJobResponse,
  DataflowIngestPipelineStatus,
  JobState,
} from "./constants";

export interface IngestDataflowJobCellFormattingInfo {
  color: string;
  sortRank: number;
}

export type IngestInstanceDataflowStatusTableInfo = {
  stateCode: string;
  dataflowPipelineStatusPrimary: JobState;
  dataflowPipelineStatusSecondary: JobState;
};

const IngestDataflowCurrentStatusSummary = (): JSX.Element => {
  const history = useHistory();
  const { loading: dataflowPipelinesLoading, data: dataflowPipelines } =
    useFetchedDataJSON<DataflowIngestPipelineJobResponse>(
      getAllLatestDataflowJobs
    );

  // TODO(#20390): stop loading dataflow enabled statuses once dataflow is fully enabled
  const {
    loading: dataflowEnabledStatusesLoading,
    data: stateDataflowEnabledStatuses,
  } = useFetchedDataJSON<IngestInstanceDataflowEnabledStatusResponse>(
    getAllIngestInstanceDataflowEnabledStatuses
  );

  if (dataflowPipelinesLoading || dataflowEnabledStatusesLoading) {
    return (
      <div className="center">
        <Spin size="large" />
      </div>
    );
  }

  if (stateDataflowEnabledStatuses === undefined) {
    return (
      <>
        <Alert
          message="Failed to load dataflow enabled statuses."
          type="error"
        />
      </>
    );
  }

  if (dataflowPipelines === undefined) {
    return (
      <>
        <Alert
          message="Failed to load ingest pipeline statuses."
          type="error"
        />
      </>
    );
  }

  function getCurrentStatus(
    pipelineStatus: DataflowIngestPipelineStatus | null
  ): JobState {
    if (pipelineStatus == null) {
      return JobState.NO_JOB_RUNS;
    }
    if (pipelineStatus.terminationState === "JOB_STATE_DONE") {
      return JobState.SUCCEEDED;
    }
    if (pipelineStatus.terminationState === "JOB_STATE_FAILED") {
      return JobState.FAILED;
    }
    throw new Error("Unknown job state found");
  }

  const queueStatusColorDict: {
    [color: string]: IngestDataflowJobCellFormattingInfo;
  } = {
    SUCCEEDED: {
      color: "job-succeeded",
      sortRank: 1,
    },
    FAILED: {
      color: "job-failed",
      sortRank: 2,
    },
    NO_JOB_RUNS: {
      color: "job-no-runs",
      sortRank: 3,
    },
    NOT_ENABLED: {
      color: "job-dataflow-not-enabled",
      sortRank: 4,
    },
  };

  function getJobStateColor(currentState: JobState): string {
    return queueStatusColorDict[currentState].color;
  }

  function getPipelineStatus(
    key: string,
    instance: DirectIngestInstance,
    enabledStatuses: IngestInstanceDataflowEnabledStatusResponse,
    pipelineStatuses: DataflowIngestPipelineJobResponse
  ): JobState {
    const enabled =
      instance === DirectIngestInstance.PRIMARY
        ? enabledStatuses[key].primary
        : enabledStatuses[key].secondary;
    if (enabled) {
      if (instance === DirectIngestInstance.PRIMARY) {
        return getCurrentStatus(pipelineStatuses[key].primary);
      }
      return getCurrentStatus(pipelineStatuses[key].secondary);
    }

    return JobState.NOT_ENABLED;
  }

  const dataSource: IngestInstanceDataflowStatusTableInfo[] = Object.keys(
    dataflowPipelines
  ).map((key) => {
    return {
      stateCode: key,
      dataflowPipelineStatusPrimary: getPipelineStatus(
        key,
        DirectIngestInstance.PRIMARY,
        stateDataflowEnabledStatuses,
        dataflowPipelines
      ),
      dataflowPipelineStatusSecondary: getPipelineStatus(
        key,
        DirectIngestInstance.SECONDARY,
        stateDataflowEnabledStatuses,
        dataflowPipelines
      ),
    };
  });

  // TODO(#24305): add X mins/hours ago to status cell info
  const renderDataflowStatusCell = (status: JobState) => {
    return <div className={classNames(getJobStateColor(status))}>{status}</div>;
  };

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
      title: "Ingest Pipeline Status (Primary)",
      dataIndex: "dataflowPipelineStatusPrimary",
      key: "dataflowPipelineStatusPrimary",
      render: (dataflowPipelineStatusPrimary: JobState) => (
        <span>{renderDataflowStatusCell(dataflowPipelineStatusPrimary)}</span>
      ),
    },
    {
      title: "Ingest Pipeline Status (Secondary)",
      dataIndex: "dataflowPipelineStatusSecondary",
      key: "dataflowPipelineStatusSecondary",
      render: (dataflowPipelineStatusSecondary: JobState) => (
        <span>{renderDataflowStatusCell(dataflowPipelineStatusSecondary)}</span>
      ),
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
        title="Ingest Pipeline Status Summary"
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
