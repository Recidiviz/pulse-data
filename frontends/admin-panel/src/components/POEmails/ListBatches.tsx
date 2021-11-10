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
import { Button, Card, Empty, Space, Spin, Table } from "antd";
import moment from "moment";
import * as React from "react";
import { getListBatchInfo } from "../../AdminPanelAPI/LineStaffTools";
import { BatchInfoType, POEmailsFormProps } from "./constants";

const ListBatches: React.FC<POEmailsFormProps> = ({
  stateInfo,
  reportType,
}) => {
  interface TableData {
    batchId: string | undefined;
    dateGenerated: string | undefined;
    sentDate?: string | undefined;
    totalDelivered?: string | undefined;
    redirectAddress?: string | undefined;
    numTimesSent?: number | undefined;
  }

  const isProduction = window.RUNTIME_GCP_ENVIRONMENT === "production";
  const projectId = isProduction ? "recidiviz-123" : "recidiviz-staging";
  const [showSpinner, setShowSpinner] = React.useState(true);
  const [tableData, setTableData] =
    React.useState<TableData[] | undefined>(undefined);

  const formatTableData = React.useCallback((batchInfo: BatchInfoType[]) => {
    const data: TableData[] = batchInfo.map((x) => {
      const dateGenerated = moment(x.batchId.substring(0, 8)).format("l");
      if (x.sendResults.length > 0) {
        x.sendResults.reverse();
        return {
          batchId: x.batchId,
          dateGenerated,
          sentDate: moment(x.sendResults[0].sentDate.substring(0, 10)).format(
            "l"
          ),
          totalDelivered: x.sendResults[0].totalDelivered,
          redirectAddress: x.sendResults[0].redirectAddress,
          numTimesSent: x.sendResults.length,
        };
      }
      return { batchId: x.batchId, dateGenerated };
    });
    setTableData(data);
  }, []);

  const refreshBatches = React.useCallback(() => {
    if (!stateInfo?.code || !reportType) return;

    setShowSpinner(true);
    const getBatches = async () => {
      const r = await getListBatchInfo(stateInfo.code, reportType);
      const json = await r.json();
      formatTableData(json.batchInfo);
      setShowSpinner(false);
    };
    getBatches();
  }, [stateInfo?.code, reportType, formatTableData]);

  React.useEffect(() => {
    refreshBatches();
  }, [refreshBatches]);

  const columns = [
    {
      title: "Date Generated",
      dataIndex: "dateGenerated",
      key: "dateGenerated",
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
    {
      title: "Sent Date",
      dataIndex: "sentDate",
      key: "sentDate",
    },
    {
      title: "Total Delivered",
      dataIndex: "totalDelivered",
      key: "totalDelivered",
    },
    {
      title: "Redirect Address",
      dataIndex: "redirectAddress",
      key: "redirectAddress",
    },
    {
      title: "# Times Sent",
      dataIndex: "numTimesSent",
      key: "numTimesSent",
    },
  ];

  return (
    <Card
      title={`Previously Generated ${stateInfo?.name || ""} ${
        reportType || ""
      } Batches`}
      extra={
        <Button
          type="primary"
          size="small"
          shape="circle"
          icon={<SyncOutlined />}
          onClick={refreshBatches}
        />
      }
    >
      {stateInfo && reportType ? (
        <>
          {showSpinner ? (
            <Spin />
          ) : (
            <Table
              columns={columns}
              dataSource={tableData}
              rowKey="batchId"
              pagination={{ pageSize: 5, size: "small", simple: true }}
            />
          )}
        </>
      ) : (
        <Empty />
      )}
    </Card>
  );
};

export default ListBatches;
