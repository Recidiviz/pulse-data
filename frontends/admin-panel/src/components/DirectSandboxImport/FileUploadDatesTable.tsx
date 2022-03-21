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
import { Table } from "antd";
import moment from "moment";
import * as React from "react";

import { FileUploadList } from "./constants";

const FileUploadDatesTable: React.FC<FileUploadList> = ({ fileUploadList }) => {
  interface TableData {
    fileTag?: string | undefined;
    uploadDate?: string | undefined;
  }

  const [tableData, setTableData] =
    React.useState<TableData[] | undefined>(undefined);

  const formatTableData = React.useCallback(() => {
    const data: TableData[] = fileUploadList?.map((x) => {
      const date = moment(x.uploadDate?.substring(0, 10)).format("l");
      return { fileTag: x.fileTag, uploadDate: date };
    });
    setTableData(data);
  }, [fileUploadList]);

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
      title: "Date Last Uploaded ",
      dataIndex: "uploadDate",
      key: "uploadDate",
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

export default FileUploadDatesTable;
