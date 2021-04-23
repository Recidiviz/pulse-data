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
import { Table } from "antd";
import { QueueMetadata } from "./constants";

interface IngestQueueStatusCardProps {
  projectId: string;
  queueStates: QueueMetadata[];
  loading: boolean;
}

const IngestQueuesTable: React.FC<IngestQueueStatusCardProps> = ({
  projectId,
  queueStates,
  loading,
}) => {
  const columns = [
    {
      title: "Queue Name",
      dataIndex: "name",
      key: "name",
      render: (name: string) => (
        <a
          href={`https://console.cloud.google.com/cloudtasks/queue/us-east1/${name}/tasks?project=${projectId}`}
        >
          {name}
        </a>
      ),
    },
    {
      title: "State",
      dataIndex: "state",
      key: "state",
    },
  ];

  return (
    <div
      style={{
        display: "flex",
        alignItems: "center",
        justifyContent: "center",
      }}
    >
      {queueStates ? (
        <Table
          columns={columns}
          dataSource={queueStates}
          pagination={false}
          loading={loading}
          style={{ width: 1000 }}
        />
      ) : null}
    </div>
  );
};

export default IngestQueuesTable;
