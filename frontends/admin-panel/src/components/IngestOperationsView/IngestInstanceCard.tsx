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
import { Button, Card, Col, Descriptions, Spin } from "antd";
import { useCallback, useEffect, useState } from "react";
import { getIngestRawFileProcessingStatus } from "../../AdminPanelAPI/IngestOperations";
import NewTabLink from "../NewTabLink";
import {
  regionActionNames,
  RegionAction,
} from "../Utilities/ActionRegionConfirmationForm";
import {
  DirectIngestInstance,
  IngestInstanceSummary,
  IngestRawFileProcessingStatus,
} from "./constants";
import IngestRawFileProcessingStatusTable from "./IngestRawFileProcessingStatusTable";
import InstanceRawFileMetadata from "./InstanceRawFileMetadata";
import InstanceIngestViewMetadata from "./IntanceIngestViewMetadata";

interface IngestInstanceCardProps {
  instance: DirectIngestInstance;
  dataLoading: boolean;
  data: IngestInstanceSummary | undefined;
  env: string;
  stateCode: string;
  handleOnClick: (action: RegionAction, instance: DirectIngestInstance) => void;
}

// TODO(#13406) Remove this gating once Start Ingest Rerun button can be displayed on the Admin Panel.
const ingestRerunButtonEnabled = true;

const IngestInstanceCard: React.FC<IngestInstanceCardProps> = ({
  instance,
  data,
  dataLoading,
  env,
  stateCode,
  handleOnClick,
}) => {
  const baseBucketUrl = `https://console.cloud.google.com/storage/browser/`;
  const logsEnv = env === "production" ? "prod" : "staging";
  const logsUrl = `http://go/${logsEnv}-ingest-${instance.toLowerCase()}-logs/${stateCode.toLowerCase()}`;
  const non200Url = `http://go/${logsEnv}-non-200-ingest-${instance.toLowerCase()}-responses/${stateCode.toLowerCase()}`;
  const pauseUnpauseInstanceAction = data?.operations.isPaused
    ? RegionAction.UnpauseIngestInstance
    : RegionAction.PauseIngestInstance;

  const [
    ingestRawFileProcessingStatusLoading,
    setIngestRawFileProcessingStatusLoading,
  ] = useState<boolean>(true);
  const [ingestRawFileProcessingStatus, setIngestRawFileProcessingStatus] =
    useState<IngestRawFileProcessingStatus[]>([]);

  const getRawFileProcessingStatusData = useCallback(async () => {
    setIngestRawFileProcessingStatusLoading(true);
    if (instance === DirectIngestInstance.PRIMARY) {
      const primaryResponse = await getIngestRawFileProcessingStatus(
        stateCode,
        instance
      );
      setIngestRawFileProcessingStatus(await primaryResponse.json());
    } else {
      // TODO(#12387): Update this also pull from endpoint if the current secondary rerun is using raw data in secondary.
      setIngestRawFileProcessingStatus([]);
    }
    setIngestRawFileProcessingStatusLoading(false);
  }, [instance, stateCode]);

  useEffect(() => {
    getRawFileProcessingStatusData();
  }, [getRawFileProcessingStatusData, data, instance, stateCode]);

  if (dataLoading) {
    return (
      <Col span={12} key={instance}>
        <Card title={instance} loading />
      </Col>
    );
  }

  if (data === undefined) {
    throw new Error(`No summary data for ${instance}`);
  }

  return (
    <Card
      title={instance}
      extra={
        <>
          <Button
            style={{ marginRight: 5 }}
            onClick={() => handleOnClick(RegionAction.ExportToGCS, instance)}
          >
            {regionActionNames[RegionAction.ExportToGCS]}
          </Button>
          <Button
            style={
              data.operations.isPaused
                ? { display: "none" }
                : { marginRight: 5 }
            }
            onClick={() => {
              handleOnClick(RegionAction.TriggerTaskScheduler, instance);
            }}
          >
            {regionActionNames[RegionAction.TriggerTaskScheduler]}
          </Button>
          <Button
            style={
              // TODO(#13406) Remove check if rerun button should be present for PRIMARY as well.
              // TODO(#13406) Remove gating for ingest rerun button once backend is ready to trigger ingest reruns
              // in secondary.
              data.instance === "SECONDARY" && ingestRerunButtonEnabled
                ? { marginRight: 5 }
                : { display: "none" }
            }
            onClick={() => {
              handleOnClick(RegionAction.StartIngestRerun, data.instance);
            }}
          >
            {regionActionNames[RegionAction.StartIngestRerun]}
          </Button>
          <Button
            onClick={() => {
              handleOnClick(pauseUnpauseInstanceAction, instance);
            }}
            type="primary"
          >
            {regionActionNames[pauseUnpauseInstanceAction]}
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
      {ingestRawFileProcessingStatusLoading ? (
        <Spin />
      ) : (
        <>
          <InstanceRawFileMetadata
            stateCode={stateCode}
            instance={instance}
            ingestRawFileProcessingStatus={ingestRawFileProcessingStatus}
          />
          <br />
          <IngestRawFileProcessingStatusTable
            ingestRawFileProcessingStatus={ingestRawFileProcessingStatus}
          />
        </>
      )}
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
