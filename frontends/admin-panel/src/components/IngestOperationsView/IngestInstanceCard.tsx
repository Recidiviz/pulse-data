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
import { getIngestRawFileProcessingStatus } from "../../AdminPanelAPI/IngestOperations";
import NewTabLink from "../NewTabLink";
import {
  GCP_STORAGE_BASE_URL,
  DirectIngestInstance,
  IngestInstanceSummary,
  IngestRawFileProcessingStatus,
} from "./constants";
import IngestRawFileProcessingStatusTable from "./IngestRawFileProcessingStatusTable";
import InstanceRawFileMetadata from "./InstanceRawFileMetadata";
import InstanceIngestViewMetadata from "./IntanceIngestViewMetadata";

interface IngestInstanceCardProps {
  instance: DirectIngestInstance;
  env: string;
  stateCode: string;
  ingestInstanceSummary: IngestInstanceSummary | undefined;
  ingestInstanceSummaryLoading: boolean;
}

const IngestInstanceCard: React.FC<IngestInstanceCardProps> = ({
  instance,
  env,
  stateCode,
  ingestInstanceSummary,
  ingestInstanceSummaryLoading,
}) => {
  const logsEnv = env === "production" ? "prod" : "staging";
  const logsUrl = `http://go/${logsEnv}-ingest-${instance.toLowerCase()}-logs/${stateCode.toLowerCase()}`;
  const non200Url = `http://go/${logsEnv}-non-200-ingest-${instance.toLowerCase()}-responses/${stateCode.toLowerCase()}`;

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
    <Card>
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
