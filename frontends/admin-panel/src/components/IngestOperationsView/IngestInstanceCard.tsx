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
import { Button, Card, Descriptions } from "antd";
import * as React from "react";
import NewTabLink from "../NewTabLink";
import {
  actionNames,
  DirectIngestInstance,
  IngestActions,
  IngestInstanceSummary,
} from "./constants";
import InstanceRawFileMetadata from "./InstanceRawFileMetadata";
import InstanceIngestViewMetadata from "./IntanceIngestViewMetadata";

interface IngestInstanceCardProps {
  data: IngestInstanceSummary;
  env: string;
  stateCode: string;
  handleOnClick: (
    action: IngestActions,
    instance: DirectIngestInstance
  ) => void;
}

const IngestInstanceCard: React.FC<IngestInstanceCardProps> = ({
  data,
  env,
  stateCode,
  handleOnClick,
}) => {
  const baseBucketUrl = `https://console.cloud.google.com/storage/browser/`;
  const logsEnv = env === "production" ? "prod" : "staging";
  const logsUrl = `http://go/${logsEnv}-ingest-${data.instance.toLowerCase()}-logs/${stateCode.toLowerCase()}`;
  const non200Url = `http://go/${logsEnv}-non-200-ingest-${data.instance.toLowerCase()}-responses/${stateCode.toLowerCase()}`;
  const pauseUnpauseInstanceAction = data.operations.isPaused
    ? IngestActions.UnpauseIngestInstance
    : IngestActions.PauseIngestInstance;
  return (
    <Card
      title={data.instance}
      extra={
        <>
          <Button
            style={{ marginRight: 5 }}
            onClick={() =>
              handleOnClick(IngestActions.ExportToGCS, data.instance)
            }
          >
            {actionNames[IngestActions.ExportToGCS]}
          </Button>
          <Button
            style={
              data.operations.isPaused
                ? { display: "none" }
                : { marginRight: 5 }
            }
            onClick={() => {
              handleOnClick(IngestActions.StartIngestRun, data.instance);
            }}
          >
            {actionNames[IngestActions.StartIngestRun]}
          </Button>
          <Button
            onClick={() => {
              handleOnClick(pauseUnpauseInstanceAction, data.instance);
            }}
            type="primary"
          >
            {actionNames[pauseUnpauseInstanceAction]}
          </Button>
        </>
      }
    >
      <Descriptions bordered>
        <Descriptions.Item label="Status" span={3}>
          {data.operations.isPaused ? "PAUSED" : "UNPAUSED"}
        </Descriptions.Item>
      </Descriptions>
      <br />
      <h1>Raw data</h1>
      <InstanceRawFileMetadata
        stateCode={stateCode}
        instance={data.instance}
        operationsInfo={data.operations}
        numFilesIngestBucket={data.ingestBucketNumFiles}
      />
      <br />
      <h1>Ingest views</h1>
      <InstanceIngestViewMetadata stateCode={stateCode} data={data} />
      <br />
      <h1>Resources</h1>
      <Descriptions bordered>
        <Descriptions.Item label="Ingest Bucket" span={3}>
          <NewTabLink href={baseBucketUrl.concat(data.ingestBucketPath)}>
            {data.ingestBucketPath}
          </NewTabLink>
        </Descriptions.Item>
        <Descriptions.Item label="Storage Bucket" span={3}>
          <NewTabLink href={baseBucketUrl.concat(data.storageDirectoryPath)}>
            {data.storageDirectoryPath}
          </NewTabLink>
        </Descriptions.Item>
        <Descriptions.Item label="Postgres database" span={3}>
          {data.dbName}
        </Descriptions.Item>
      </Descriptions>
      <br />
      <h1>Logs</h1>
      <Descriptions bordered>
        <Descriptions.Item label="Logs Explorer" span={3}>
          <NewTabLink href={logsUrl}>{logsUrl}</NewTabLink>
        </Descriptions.Item>
        <Descriptions.Item label="Non 200 Responses" span={3}>
          <NewTabLink href={non200Url}>{non200Url}</NewTabLink>
        </Descriptions.Item>
      </Descriptions>
    </Card>
  );
};

export default IngestInstanceCard;
