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
  DatePicker,
  Form,
  PageHeader,
  Select,
  Spin,
  Upload,
} from "antd";
import * as React from "react";
import moment, { Moment } from "moment";
import StateSelector from "./Utilities/StateSelector";
import { fetchRawFilesStateCodes } from "../AdminPanelAPI";
import { formLayout } from "./constants";

const UploadRawFilesView = (): JSX.Element => {
  const [uploadResult, setUploadResult] = React.useState<AlertProps | void>();
  const [showSpinner, setShowSpinner] = React.useState(false);
  const [form] = Form.useForm();

  interface UploadRawFilesFormData {
    state: string;
    date: Moment;
    uploadType: string;
    upload: File[];
  }

  const onFinish = async (values: UploadRawFilesFormData) => {
    setShowSpinner(true);
    const formData = new FormData();
    formData.append("dateOfStandards", values.date.format("YYYY-MM-DD"));
    formData.append("uploadType", values.uploadType);
    formData.append("file", values.upload[0]);
    const response = await fetch(
      `/admin/api/line_staff_tools/${values.state}/upload_raw_files`,
      {
        method: "POST",
        body: formData,
      }
    );
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
        title="Upload Raw Files"
        subTitle="Form to upload raw excel files directly into BigQuery. Currently accepts TN standards data only."
      />

      <Form
        {...formLayout}
        form={form}
        onFinish={onFinish}
        requiredMark={false}
      >
        <Form.Item label="State" name="state" rules={[{ required: true }]}>
          <StateSelector
            fetchStateList={fetchRawFilesStateCodes}
            onChange={(stateInfo) => {
              form.setFieldsValue({ state: stateInfo.code });
            }}
          />
        </Form.Item>

        <Form.Item
          label="Date of standards"
          name="date"
          rules={[{ required: true }]}
        >
          <DatePicker
            disabledDate={(current) =>
              current && current > moment().endOf("day")
            }
            style={{ width: 200 }}
          />
        </Form.Item>
        <Form.Item
          label="Upload Type"
          name="uploadType"
          rules={[{ required: true }]}
        >
          <Select style={{ width: 200 }} placeholder="Select an upload type">
            {/* It's a bit silly that these are hardcoded and the state values are fetched, but in
            the future once we have additional non-standards file upload types we should have a
            better sense of how we want to return+display this information. */}
            <Select.Option key="STANDARDS_ADMIN" value="STANDARDS_ADMIN">
              STANDARDS_ADMIN
            </Select.Option>
            <Select.Option key="STANDARDS_DUE" value="STANDARDS_DUE">
              STANDARDS_DUE
            </Select.Option>
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
              Upload Raw Files
            </Button>
          </Upload>
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

export default UploadRawFilesView;
