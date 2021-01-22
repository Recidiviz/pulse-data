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
import { Alert, Button, Form, Input, PageHeader, message } from "antd";
import { WarningFilled } from "@ant-design/icons";

import { runCloudSQLImport } from "../AdminPanelAPI";

const CloudSQLImportView = (): JSX.Element => {
  const layout = {
    labelCol: { span: 4 },
    wrapperCol: { span: 20 },
  };
  const tailLayout = {
    wrapperCol: { offset: 4, span: 20 },
  };

  const onFinish = async (values: { [key: string]: string }) => {
    let columns = [];
    try {
      columns = JSON.parse(values.columns);
    } catch (e) {
      message.error("Invalid JSON provided");
      return;
    }
    if (!Array.isArray(columns)) {
      message.error("The provided columns are not a valid json array");
      return;
    }
    message.info("Import started");
    const r = await runCloudSQLImport(
      values.destinationTable,
      values.gcsURI,
      columns
    );
    if (r.status >= 400) {
      const text = await r.text();
      message.error(`Error loading data: ${text}`);
      return;
    }
    message.success("Import succeeded!");
  };
  return (
    <>
      <PageHeader title="GCS CSV to Cloud SQL Import" />
      <Alert
        message={
          <>
            <WarningFilled /> Caution!
          </>
        }
        description="You should only use this form if you are a member of the Line Staff Tools team, and you absolutely know what you are doing."
        type="warning"
      />
      <Form {...layout} className="buffer" onFinish={onFinish}>
        <Form.Item
          label="Destination Table"
          name="destinationTable"
          rules={[{ required: true }]}
        >
          <Input />
        </Form.Item>

        <Form.Item label="GCS URI" name="gcsURI" rules={[{ required: true }]}>
          <Input />
        </Form.Item>

        <Form.Item
          label="Columns (as JSON)"
          name="columns"
          rules={[{ required: true }]}
        >
          <Input />
        </Form.Item>

        <Form.Item {...tailLayout}>
          <Button type="primary" htmlType="submit">
            Start Import
          </Button>
        </Form.Item>
      </Form>
    </>
  );
};

export default CloudSQLImportView;
