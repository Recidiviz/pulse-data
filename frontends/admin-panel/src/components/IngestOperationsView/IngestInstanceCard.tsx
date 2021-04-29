import * as React from "react";
import { Button, Card, Col, Descriptions, Row, Statistic } from "antd";
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
  return (
    <Card
      title={data.instance}
      extra={
        <Button
          onClick={() => {
            handleOnClick(IngestActions.StartIngestRun, data.instance);
          }}
          type="primary"
          block
        >
          {actionNames[IngestActions.StartIngestRun]}
        </Button>
      }
    >
      <Descriptions bordered>
        <Descriptions.Item label="Ingest Bucket" span={3}>
          <a href={baseBucketUrl.concat(data.ingest.name)}>
            {data.ingest.name}
          </a>
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
          <a href={baseBucketUrl.concat(data.storage)}>{data.storage}</a>
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
