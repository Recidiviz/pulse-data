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
import * as React from "react";
import { Card, Col, Row } from "antd";
import { QueueMetadata } from "./constants";

interface IngestQueueStatusCardProps {
  projectId: string;
  queueStates: QueueMetadata[];
  loading: boolean;
}

const IngestQueueStatusCard: React.FC<IngestQueueStatusCardProps> = ({
  projectId,
  queueStates,
  loading,
}) => {
  const cloudTasksUrl = `https://console.cloud.google.com/cloudtasks?organizationId=448885369991&project=${projectId}`;

  return (
    <Row gutter={[16, 16]}>
      <Col span={24}>
        <Card
          title="Queue States"
          extra={<a href={cloudTasksUrl}>Go to Cloud Task Queues</a>}
          loading={loading}
        >
          <ul>
            {queueStates?.map((queueData: QueueMetadata) => {
              return (
                <li key={queueData.name}>
                  {queueData.name}: {queueData.state}
                </li>
              );
            })}
          </ul>
        </Card>
      </Col>
    </Row>
  );
};

export default IngestQueueStatusCard;
