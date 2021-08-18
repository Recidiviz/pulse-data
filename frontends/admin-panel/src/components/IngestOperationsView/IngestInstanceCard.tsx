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
import { Button, Card, Col, Descriptions, Row, Statistic } from "antd";
import * as React from "react";
import NewTabLink from "../NewTabLink";
import {
  actionNames,
  DirectIngestInstance,
  IngestActions,
  IngestInstanceSummary,
} from "./constants";

interface IngestInstanceCardProps {
  data: IngestInstanceSummary;
  handleOnClick: (
    action: IngestActions,
    instance: DirectIngestInstance
  ) => void;
}

const IngestInstanceCard: React.FC<IngestInstanceCardProps> = ({
  data,
  handleOnClick,
}) => {
  const baseBucketUrl = `https://console.cloud.google.com/storage/browser/`;
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
        <Descriptions.Item label="Ingest Bucket" span={3}>
          <NewTabLink href={baseBucketUrl.concat(data.ingest.name)}>
            {data.ingest.name}
          </NewTabLink>
        </Descriptions.Item>
        <Descriptions.Item label="Raw Data Files" span={3}>
          <Row gutter={16}>
            <Col span={12}>
              <Statistic
                title="Unprocessed"
                value={data.ingest.unprocessedFilesRaw}
              />
            </Col>
            <Col span={12}>
              <Statistic
                title="Processed"
                value={data.ingest.processedFilesRaw}
              />
            </Col>
          </Row>
        </Descriptions.Item>
        <Descriptions.Item label="Ingest View Files" span={3}>
          <Row gutter={16}>
            <Col span={12}>
              <Statistic
                title="Unprocessed"
                value={data.ingest.unprocessedFilesIngestView}
              />
            </Col>
            <Col span={12}>
              <Statistic
                title="Processed"
                value={data.ingest.processedFilesIngestView}
              />
            </Col>
          </Row>
        </Descriptions.Item>
        <Descriptions.Item label="Storage Bucket" span={3}>
          <NewTabLink href={baseBucketUrl.concat(data.storage)}>
            {data.storage}
          </NewTabLink>
        </Descriptions.Item>
        <Descriptions.Item label="Database" span={3}>
          {data.dbName}
        </Descriptions.Item>
      </Descriptions>
      <br />
      <Descriptions title="Operations Database" bordered>
        <Descriptions.Item label="Unprocessed Files" span={3}>
          <Row gutter={16}>
            <Col span={12}>
              <Statistic
                title="Raw"
                value={data.operations.unprocessedFilesRaw}
              />
            </Col>
            <Col span={12}>
              <Statistic
                title="Ingest View"
                value={data.operations.unprocessedFilesIngestView}
              />
            </Col>
          </Row>
        </Descriptions.Item>
        <Descriptions.Item
          label="Date of Earliest Unprocessed Ingest File"
          span={3}
        >
          {data.operations.dateOfEarliestUnprocessedIngestView}
        </Descriptions.Item>
      </Descriptions>
    </Card>
  );
};

export default IngestInstanceCard;
