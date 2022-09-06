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
import { Card, Col, Descriptions, Spin } from "antd";
import { useCallback, useEffect, useState } from "react";
import {
  getIngestInstanceSummary,
  getIngestRawFileProcessingStatus,
} from "../../AdminPanelAPI/IngestOperations";
import NewTabLink from "../NewTabLink";
import {
  regionActionNames,
  RegionAction,
} from "../Utilities/ActionRegionConfirmationForm";
import {
  GCP_STORAGE_BASE_URL,
  DirectIngestInstance,
  IngestInstanceSummary,
  IngestRawFileProcessingStatus,
} from "./constants";
import IngestActionButton from "./IngestActionButton";
import IngestRawFileProcessingStatusTable from "./IngestRawFileProcessingStatusTable";
import InstanceRawFileMetadata from "./InstanceRawFileMetadata";
import InstanceIngestViewMetadata from "./IntanceIngestViewMetadata";

interface IngestInstanceCardProps {
  instance: DirectIngestInstance;
  env: string;
  stateCode: string;
}

const IngestInstanceCard: React.FC<IngestInstanceCardProps> = ({
  instance,
  env,
  stateCode,
}) => {
  const logsEnv = env === "production" ? "prod" : "staging";
  const logsUrl = `http://go/${logsEnv}-ingest-${instance.toLowerCase()}-logs/${stateCode.toLowerCase()}`;
  const non200Url = `http://go/${logsEnv}-non-200-ingest-${instance.toLowerCase()}-responses/${stateCode.toLowerCase()}`;

  const [ingestInstanceSummaryLoading, setIngestInstanceSummaryLoading] =
    useState<boolean>(true);
  const [ingestInstanceSummary, setIngestInstanceSummary] =
    useState<IngestInstanceSummary | undefined>(undefined);

  const pauseUnpauseInstanceAction = ingestInstanceSummary?.operations.isPaused
    ? RegionAction.UnpauseIngestInstance
    : RegionAction.PauseIngestInstance;

  const [
    ingestRawFileProcessingStatusLoading,
    setIngestRawFileProcessingStatusLoading,
  ] = useState<boolean>(true);
  const [ingestRawFileProcessingStatus, setIngestRawFileProcessingStatus] =
    useState<IngestRawFileProcessingStatus[]>([]);

  const fetchIngestInstanceSummary = useCallback(async () => {
    setIngestInstanceSummaryLoading(true);
    const primaryResponse = await getIngestInstanceSummary(stateCode, instance);
    const result: IngestInstanceSummary = await primaryResponse.json();
    setIngestInstanceSummary(result);
    setIngestInstanceSummaryLoading(false);
  }, [instance, stateCode]);

  useEffect(() => {
    fetchIngestInstanceSummary();
  }, [fetchIngestInstanceSummary]);

  const getRawFileProcessingStatusData = useCallback(async () => {
    setIngestRawFileProcessingStatusLoading(true);
    if (instance === DirectIngestInstance.PRIMARY) {
      const primaryResponse = await getIngestRawFileProcessingStatus(
        stateCode,
        instance
      );
      setIngestRawFileProcessingStatus(await primaryResponse.json());
    } else {
      // TODO(#12387): Update this to also pull from endpoint if the current secondary rerun is using raw data in secondary.
      setIngestRawFileProcessingStatus([]);
    }
    setIngestRawFileProcessingStatusLoading(false);
  }, [instance, stateCode]);

  useEffect(() => {
    getRawFileProcessingStatusData();
  }, [
    getRawFileProcessingStatusData,
    ingestInstanceSummary,
    instance,
    stateCode,
  ]);

  if (ingestInstanceSummaryLoading) {
    return (
      <Col span={12} key={instance}>
        <Card title={instance} loading />
      </Col>
    );
  }

  if (ingestInstanceSummary === undefined) {
    throw new Error(`No summary data for ${instance}`);
  }

  return (
    <Card
      title={instance}
      extra={
        <>
          <IngestActionButton
            style={{ marginRight: 5 }}
            action={RegionAction.ExportToGCS}
            buttonText={regionActionNames[RegionAction.ExportToGCS]}
            instance={instance}
            stateCode={stateCode}
          />
          <IngestActionButton
            style={
              ingestInstanceSummary.operations.isPaused
                ? { display: "none" }
                : { marginRight: 5 }
            }
            action={RegionAction.TriggerTaskScheduler}
            buttonText={regionActionNames[RegionAction.TriggerTaskScheduler]}
            instance={instance}
            stateCode={stateCode}
          />
          <IngestActionButton
            style={
              // TODO(#13406) Remove check if rerun button should be present for PRIMARY as well.
              instance === "SECONDARY"
                ? { marginRight: 5 }
                : { display: "none" }
            }
            action={RegionAction.StartIngestRerun}
            buttonText={regionActionNames[RegionAction.StartIngestRerun]}
            instance={instance}
            stateCode={stateCode}
          />
          <IngestActionButton
            action={pauseUnpauseInstanceAction}
            buttonText={regionActionNames[pauseUnpauseInstanceAction]}
            instance={instance}
            stateCode={stateCode}
            onActionConfirmed={() => {
              fetchIngestInstanceSummary();
            }}
            type="primary"
          />
        </>
      }
    >
      <Descriptions bordered>
        <Descriptions.Item label="Status" span={3}>
          {ingestInstanceSummary.operations.isPaused ? "PAUSED" : "UNPAUSED"}
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
            ingestBucketPath={ingestInstanceSummary.ingestBucketPath}
            storageDirectoryPath={ingestInstanceSummary.storageDirectoryPath}
          />
        </>
      )}
      <br />
      <h1>Ingest views</h1>
      <InstanceIngestViewMetadata
        stateCode={stateCode}
        data={ingestInstanceSummary}
      />
      <br />
      <h1>Resources</h1>
      <Descriptions bordered>
        <Descriptions.Item label="Ingest Bucket" span={3}>
          <NewTabLink
            href={GCP_STORAGE_BASE_URL.concat(
              ingestInstanceSummary.ingestBucketPath
            )}
          >
            {ingestInstanceSummary.ingestBucketPath}
          </NewTabLink>
        </Descriptions.Item>
        <Descriptions.Item label="Storage Bucket" span={3}>
          <NewTabLink
            href={GCP_STORAGE_BASE_URL.concat(
              ingestInstanceSummary.storageDirectoryPath
            )}
          >
            {ingestInstanceSummary.storageDirectoryPath}
          </NewTabLink>
        </Descriptions.Item>
        <Descriptions.Item label="Postgres database" span={3}>
          {ingestInstanceSummary.dbName}
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
