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
import { useState } from "react";
import { fetchRosterStateCodes } from "../../AdminPanelAPI";
import StateSelector from "../Utilities/StateSelector";

const UploadRostersView = (): JSX.Element => {
  const [stateCode, setStateCode] = useState<string | undefined>();
  const [uploading, setUploading] = useState(false);
  const [errorMessage, setErrorMessage] = useState<string | void>();

  const disabled = !stateCode;

  return (
    <>
      <PageHeader title="Upload Line Staff Roster" />
      <Space direction="vertical">
        <StateSelector
          fetchStateList={fetchRosterStateCodes}
          onChange={(stateInfo) => setStateCode(stateInfo.code)}
        />

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
          action={`/admin/api/line_staff_tools/${stateCode}/upload_roster`}
          maxCount={1}
          disabled={disabled}
          onChange={(info) => {
            if (info.file.status === "uploading" && !uploading) {
              setUploading(true);
              setErrorMessage();
              message.info("Uploading roster...");
            } else if (info.file.status === "error") {
              setUploading(false);
              setErrorMessage(
                `Response status: ${info.file.error?.status}. Message: ${info.file.response}`
              );
              message.error("An error occurred.");
            } else if (info.file.status === "done") {
              setUploading(false);
              setErrorMessage();
              message.success("Roster uploaded successfully!");
            }
          }}
          showUploadList={false}
          withCredentials
        >
          <Button
            icon={<UploadOutlined />}
            loading={uploading}
            disabled={disabled}
          >
            Upload Roster
          </Button>
        </Upload>
        {errorMessage && (
          <Alert
            message="Upload failed."
            type="error"
            description={errorMessage}
          />
        )}
      </Space>
    </>
  );
};

export default UploadRostersView;
