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
import { Alert, Button, Input, message, Select, Space, Upload } from "antd";
import { useState } from "react";

import { StateRolePermissionsResponse } from "../../types";
import { STATE_CODES_TO_NAMES } from "../constants";

type UploadRosterProps = {
  action: string;
  method: "PUT" | "POST";
  columns: string[];
  setStateCode: (stateCode: string) => void;
  stateCode?: string;
  setReason: (reason: string) => void;
  reason?: string;
  stateRoleData?: StateRolePermissionsResponse[];
  warningMessage: string;
};

const UploadRoster = ({
  action,
  method,
  columns,
  setStateCode,
  stateCode,
  setReason,
  reason,
  stateRoleData = [],
  warningMessage,
}: UploadRosterProps): JSX.Element => {
  const [uploading, setUploading] = useState(false);
  const [errorMessage, setErrorMessage] = useState<string | void>();

  const disabled = !stateCode || !reason;

  // Determine state codes and role data for state user roster upload
  const stateRoleStateCodes = stateRoleData
    .map((d) => d.stateCode)
    .filter((v, i, a) => a.indexOf(v) === i);

  return (
    <Space direction="vertical">
      <Select
        placeholder="Select a state"
        onChange={(s) => setStateCode(s)}
        options={stateRoleStateCodes.map((c) => {
          return {
            value: c,
            label:
              STATE_CODES_TO_NAMES[c as keyof typeof STATE_CODES_TO_NAMES] || c,
          };
        })}
      />
      <Input
        name="reason"
        placeholder="Reason for roster upload"
        onChange={(
          e: React.ChangeEvent<HTMLInputElement | HTMLTextAreaElement>
        ) => {
          setReason(e.target.value);
        }}
      />

      <Alert
        message="Instructions"
        description={
          <Space direction="vertical">
            <div>{warningMessage}</div>
            <div>
              It requires a CSV to be uploaded according to a precise format in
              order to ensure that the data is imported correctly.
            </div>
            <div>
              The inputted CSV must have a header row with exactly these columns
              (in any order):
              <ul>
                {columns.map((column) => {
                  return (
                    <li key={column}>
                      <code>{column}</code>
                    </li>
                  );
                })}
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
        action={action}
        method={method}
        data={{ reason }}
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
              `Response status: ${info.file.error?.status}. Message: ${info.file.response.message}`
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
  );
};

export default UploadRoster;
