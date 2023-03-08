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
import { Card, Descriptions, Spin } from "antd";
import Title from "antd/lib/typography/Title";
import { useCallback, useEffect, useRef, useState } from "react";
import { useLocation } from "react-router-dom";
import {
  getIngestInstanceResources,
  getIngestRawFileProcessingStatus,
} from "../../AdminPanelAPI/IngestOperations";
import NewTabLink from "../NewTabLink";
import { scrollToAnchor } from "../Utilities/GeneralUtilities";
import { isAbortException } from "../Utilities/exceptions";
import IngestRawFileProcessingStatusTable from "./IngestRawFileProcessingStatusTable";
import InstanceRawFileMetadata from "./InstanceRawFileMetadata";
import InstanceIngestViewMetadata from "./IntanceIngestViewMetadata";
import {
  ANCHOR_INGEST_LOGS,
  ANCHOR_INGEST_RAW_DATA,
  ANCHOR_INGEST_RESOURCES,
  ANCHOR_INGEST_VIEWS,
  DirectIngestInstance,
  GCP_STORAGE_BASE_URL,
  IngestInstanceResources,
  IngestRawFileProcessingStatus,
} from "./constants";

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
  const { hash } = useLocation();
  // Uses useRef so abort controller not re-initialized every render cycle.
  const abortControllerRefResources =
    useRef<AbortController | undefined>(undefined);
  const abortControllerRefRaw = useRef<AbortController | undefined>(undefined);

  const [ingestInstanceResources, setIngestInstanceResources] =
    useState<IngestInstanceResources | undefined>(undefined);

  const fetchingestInstanceResources = useCallback(async () => {
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
    fetchingestInstanceResources();
  }, [fetchingestInstanceResources, instance, stateCode]);

  const [
    ingestRawFileProcessingStatusLoading,
    setIngestRawFileProcessingStatusLoading,
  ] = useState<boolean>(true);
  const [ingestRawFileProcessingStatus, setIngestRawFileProcessingStatus] =
    useState<IngestRawFileProcessingStatus[]>([]);

  useEffect(() => {
    scrollToAnchor(hash);
  }, [hash, ingestRawFileProcessingStatus]);

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

  return (
    <Card>
      <Title id={ANCHOR_INGEST_RAW_DATA} level={4}>
        Raw data
      </Title>
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
      />
      <br />
      <Title id={ANCHOR_INGEST_VIEWS} level={4}>
        Ingest views
      </Title>
      <InstanceIngestViewMetadata stateCode={stateCode} instance={instance} />
      <br />
      <Title id={ANCHOR_INGEST_RESOURCES} level={4}>
        Resources
      </Title>
      <Descriptions bordered>
        <Descriptions.Item label="Ingest Bucket" span={3}>
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
        <Descriptions.Item label="Storage Bucket" span={3}>
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
        <Descriptions.Item label="Postgres database" span={3}>
          {!ingestInstanceResources ? <Spin /> : ingestInstanceResources.dbName}
        </Descriptions.Item>
      </Descriptions>
      <br />
      <Title id={ANCHOR_INGEST_LOGS} level={4}>
        Logs
      </Title>
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
