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

import { SyncOutlined } from "@ant-design/icons";
import { Button, Card, Space, Spin, Table } from "antd";
import * as React from "react";
import moment from "moment";
import { getListBatchInfo } from "../../AdminPanelAPI/LineStaffTools";
import { StateCodeInfo } from "../IngestOperationsView/constants";

interface ListBatchesProps {
  stateInfo: StateCodeInfo;
}

const ListBatches: React.FC<ListBatchesProps> = ({ stateInfo }) => {
  interface TableData {
    batchId: string;
    date: string;
  }

  const isProduction = window.RUNTIME_GCP_ENVIRONMENT === "production";
  const projectId = isProduction ? "recidiviz-123" : "recidiviz-staging";
  const [showSpinner, setShowSpinner] = React.useState(true);
  const [tableData, setTableData] =
    React.useState<TableData[] | undefined>(undefined);

  const generateBatches = React.useCallback(() => {
    const getBatches = async () => {
      const r = await getListBatchInfo(stateInfo.code);
      const json = await r.json();
      formatTableData(json.batchIds);
      setShowSpinner(false);
    };
    getBatches();
  }, [stateInfo.code]);

  React.useEffect(() => {
    generateBatches();
  }, [generateBatches]);

  const onStateRefresh = async () => {
    setShowSpinner(true);
    await generateBatches();
  };

  const formatTableData = (batches: string[]) => {
    const data = [];
    for (let i = 0; i < batches.length; i += 1) {
      const date = moment(batches[i].substring(0, 8)).format("l");
      data.push({ batchId: batches[i], date });
    }
    setTableData(data);
  };

  const columns = [
    {
      title: "Date",
      dataIndex: "date",
    },
    {
      title: "Batch ID",
      key: "batchId",
      render: (_: string, record: TableData) => (
        <Space size="middle">
          <a
            href={`https://console.cloud.google.com/storage/browser/${projectId}-report-html/${stateInfo?.code}/${record.batchId}`}
          >
            {record.batchId}
          </a>
        </Space>
      ),
    },
  ];

  return (
    <Card
      title={`${stateInfo?.name} Previously Generated Batches`}
      extra={
        <Button
          type="primary"
          size="small"
          shape="circle"
          icon={<SyncOutlined />}
          onClick={onStateRefresh}
        />
      }
    >
      {showSpinner ? (
        <Spin />
      ) : (
        <Table
          bordered
          columns={columns}
          dataSource={tableData}
          rowKey="batchId"
          pagination={{ pageSize: 5, size: "small", simple: true }}
        />
      )}
    </Card>
  );
};

export default ListBatches;
