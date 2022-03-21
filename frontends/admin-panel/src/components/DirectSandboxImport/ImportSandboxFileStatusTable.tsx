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
import { Table, Tag } from "antd";
import * as React from "react";

import { FileStatusList } from "./constants";

const ImportSandboxFileStatusTable: React.FC<FileStatusList> = ({
  fileStatusList,
}) => {
  interface TableData {
    fileTag?: string | undefined;
    status?: string | undefined;
  }
  const [tableData, setTableData] =
    React.useState<TableData[] | undefined>(undefined);

  const formatTableData = React.useCallback(() => {
    const data: TableData[] = fileStatusList?.map((x) => {
      return { fileTag: x.fileTag, status: x.status };
    });
    setTableData(data);
  }, [fileStatusList]);

  const formatStatusTag = (status: string) => {
    let color = "";
    if (status.includes("succeeded")) {
      color = "green";
    } else if (status.includes("skipped")) {
      color = "grey";
    } else {
      color = "red";
    }
    return (
      <Tag color={color} key={status}>
        {status.toUpperCase()}
      </Tag>
    );
  };

  React.useEffect(() => {
    formatTableData();
  }, [formatTableData]);

  const columns = [
    {
      title: "File Tag",
      dataIndex: "fileTag",
      key: "fileTag",
    },
    {
      title: "Status",
      dataIndex: "status",
      key: "status",
      render: (status: string) => <span>{formatStatusTag(status)}</span>,
    },
  ];

  return (
    <>
      <Table
        columns={columns}
        dataSource={tableData}
        rowKey="fileTag"
        pagination={{
          defaultPageSize: 5,
          pageSizeOptions: ["5", "10", "20", "50"],
          size: "small",
          showSizeChanger: true,
        }}
      />
    </>
  );
};

export default ImportSandboxFileStatusTable;
