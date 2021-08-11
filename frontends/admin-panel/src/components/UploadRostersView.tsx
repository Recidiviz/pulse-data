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
import { UploadOutlined } from "@ant-design/icons";
import { Alert, Button, message, PageHeader, Space, Upload } from "antd";
import * as React from "react";

const UploadRostersView = (): JSX.Element => {
  const [uploading, setUploading] = React.useState(false);

  return (
    <>
      <PageHeader title="Upload Idaho Roster" />
      <Space direction="vertical">
        <Alert
          message="Instructions"
          description={
            <Space direction="vertical">
              <div>
                This form should be used to completely overwrite the existing
                roster. It requires a CSV to be uploaded according to a precise
                format in order to ensure that the data is imported correctly.
              </div>
              <div>
                The inputted CSV must have a header row with exactly these five
                columns (in any order):
                <ul>
                  <li>
                    <code>employee_name</code>
                  </li>
                  <li>
                    <code>email_address</code>
                  </li>
                  <li>
                    <code>job_title</code>
                  </li>
                  <li>
                    <code>district</code>
                  </li>
                  <li>
                    <code>external_id</code>
                  </li>
                </ul>
              </div>
            </Space>
          }
          type="info"
          style={{ width: "100%" }}
          showIcon
        />
        <Upload
          accept=".csv"
          action="/admin/api/line_staff_tools/upload_idaho_roster"
          maxCount={1}
          onChange={(info) => {
            if (info.file.status === "uploading" && !uploading) {
              setUploading(true);
              message.info("Uploading roster...");
            } else if (info.file.status === "error") {
              setUploading(false);
              message.error("Something went wrong with the upload.");
            } else if (info.file.status === "done") {
              setUploading(false);
              message.success("Roster uploaded successfully!");
            }
          }}
          showUploadList={false}
          withCredentials
        >
          <Button icon={<UploadOutlined />} loading={uploading}>
            Upload Roster
          </Button>
        </Upload>
      </Space>
    </>
  );
};

export default UploadRostersView;
