// Recidiviz - a data platform for criminal justice reform
// Copyright (C) 2022 Recidiviz, Inc.
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
import {
  Alert,
  AlertProps,
  Button,
  Form,
  PageHeader,
  Select,
  Spin,
  Upload,
} from "antd";
import { useState } from "react";
import {
  bulkUpload,
  getAgencies,
  getUsers,
} from "../../AdminPanelAPI/JusticeCountsTools";
import { useFetchedDataJSON } from "../../hooks";
import { formLayout } from "../constants";
import { AgenciesResponse, UsersResponse } from "./constants";

const JusticeCountsBulkUploadView = (): JSX.Element => {
  const [uploadResult, setUploadResult] = useState<AlertProps | void>();
  const [showSpinner, setShowSpinner] = useState(false);

  const { data: usersData } = useFetchedDataJSON<UsersResponse>(getUsers);
  const { data: agenciesData } =
    useFetchedDataJSON<AgenciesResponse>(getAgencies);
  const [form] = Form.useForm();

  const usersToShow = usersData?.users.filter((user) => user.id != null);

  interface BulkUploadFormData {
    agency: string;
    user: string;
    system: string;
    upload: File[];
    inferAggregateValue: boolean;
  }

  const onFinish = async (values: BulkUploadFormData) => {
    setShowSpinner(true);
    const formData = new FormData();
    formData.append("agency_id", values.agency);
    formData.append("user_id", values.user);
    formData.append("system", values.system);
    formData.append("file", values.upload[0]);
    formData.append(
      "infer_aggregate_value",
      String(values.inferAggregateValue)
    );
    const response = await bulkUpload(formData);
    if (response.status === 200) {
      setUploadResult({
        type: "success",
        message: "Upload succeeded",
      });
    } else {
      const text = await response.text();
      setUploadResult({
        type: "error",
        message: "Upload failed",
        description: text,
      });
    }
    setShowSpinner(false);
  };
  return (
    <>
      <PageHeader
        title="Bulk Upload"
        subTitle="Form to upload Justice Counts data for an agency."
      />

      <Form
        {...formLayout}
        form={form}
        onFinish={onFinish}
        requiredMark={false}
      >
        <Form.Item label="Agency" name="agency" rules={[{ required: true }]}>
          <Select disabled={showSpinner} style={{ width: 200 }}>
            {agenciesData?.agencies?.map((agency) => (
              <Select.Option key={agency.id} value={agency.id}>
                {agency.name}
              </Select.Option>
            ))}
          </Select>
        </Form.Item>
        <Form.Item label="System" name="system" rules={[{ required: true }]}>
          <Select disabled={showSpinner} style={{ width: 200 }}>
            {agenciesData?.systems?.map((system) => (
              <Select.Option key={system} value={system}>
                {system}
              </Select.Option>
            ))}
          </Select>
        </Form.Item>
        <Form.Item label="User" name="user" rules={[{ required: true }]}>
          <Select disabled={showSpinner} style={{ width: 200 }}>
            {usersToShow?.map((user) => (
              <Select.Option key={user.id} value={user.id}>
                {`${user.id}: ${
                  user.email_address != null
                    ? user.email_address
                    : "<no email address>"
                }`}
              </Select.Option>
            ))}
          </Select>
        </Form.Item>
        <Form.Item
          label="File"
          name="upload"
          valuePropName="fileList"
          getValueFromEvent={(e) => {
            return [e.file];
          }}
          rules={[{ required: true }]}
        >
          <Upload
            accept=".xlsx"
            maxCount={1}
            beforeUpload={(file) => {
              // Allows us to wait to make our POST request with the file until the submit buton is pressed.
              return false;
            }}
          >
            <Button icon={<UploadOutlined />} style={{ width: 200 }}>
              Upload Spreadsheet
            </Button>
          </Upload>
        </Form.Item>
        <Form.Item
          label="Infer Aggregate Value"
          name="inferAggregateValue"
          valuePropName="checked"
        >
          <input type="checkbox" />
        </Form.Item>
        <Form.Item wrapperCol={{ offset: 4, span: 20 }}>
          <Button type="primary" htmlType="submit" disabled={showSpinner}>
            Submit
          </Button>
        </Form.Item>
      </Form>

      {showSpinner && <Spin />}
      {uploadResult && <Alert {...uploadResult} />}
    </>
  );
};

export default JusticeCountsBulkUploadView;
