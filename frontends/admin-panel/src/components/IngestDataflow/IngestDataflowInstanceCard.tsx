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
import { Alert, Card, Descriptions, Spin } from "antd";
import Title from "antd/lib/typography/Title";
import moment from "moment";
import { useCallback } from "react";
import { getLatestDataflowPipelineByInstance } from "../../AdminPanelAPI/IngestOperations";
import { useFetchedDataJSON } from "../../hooks";
import { DirectIngestInstance } from "../IngestOperationsView/constants";
import NewTabLink from "../NewTabLink";
import { formatDatetimeFromTimestamp } from "../Utilities/GeneralUtilities";
import {
  ANCHOR_DATAFLOW_LATEST_JOB,
  DataflowIngestPipelineStatus,
  JobState,
} from "./constants";

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
  const fetchDataflowPipelineInstance = useCallback(async () => {
    return getLatestDataflowPipelineByInstance(stateCode, instance);
  }, [stateCode, instance]);

  const {
    loading: mostRecentPipelineInfoLoading,
    data: mostRecentPipelineInfo,
  } = useFetchedDataJSON<DataflowIngestPipelineStatus>(
    fetchDataflowPipelineInstance
  );

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
          {env === "staging" || env === "development" ? (
            <NewTabLink
              href={`http://go/staging-ingest-dataflow-logs/${mostRecentPipelineInfo.name}`}
            >
              go/staging-ingest-dataflow-logs/
              {mostRecentPipelineInfo.name}
            </NewTabLink>
          ) : (
            <NewTabLink
              href={`http://go/prod-ingest-dataflow-logs/${mostRecentPipelineInfo.name}`}
            >
              go/prod-ingest-dataflow-logs/
              {mostRecentPipelineInfo.name}
            </NewTabLink>
          )}
        </Descriptions.Item>
      </Descriptions>
      <br />
    </Card>
  );
};

export default IngestDataflowInstanceCard;
