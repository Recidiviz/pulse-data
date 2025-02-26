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
import { Alert, Button, Form, PageHeader, Result, Select, Spin } from "antd";
import { useState } from "react";
import { fetchETLViewIds, runCloudSQLImport } from "../AdminPanelAPI";
import { useFetchedDataJSON } from "../hooks";
import { formLayout, formTailLayout } from "./constants";

const CloudSQLImportView = (): JSX.Element => {
  const [importStatus, setImportStatus] =
    useState<"not-started" | "started" | "done" | "errored">("not-started");
  const [errorText, setErrorText] = useState<string>("");
  const { loading, data } = useFetchedDataJSON<string[]>(fetchETLViewIds);

  if (loading) {
    return (
      <div className="center">
        <Spin size="large" />
      </div>
    );
  }

  if (importStatus === "started") {
    return (
      <Result
        title="Import has been started..."
        subTitle="This page will update when import has completed successfully."
      />
    );
  }
  if (importStatus === "done") {
    return (
      <Result status="success" title="Import has completed successfully!" />
    );
  }
  if (importStatus === "errored") {
    return (
      <Result
        status="error"
        title="Something went wrong during import."
        subTitle={errorText}
      />
    );
  }

  const onFinish = async (values: { [key: string]: string }) => {
    // This is a hack needed to get typescript to realize that the provided value is
    // a string[] and not just a string. See
    // https://basarat.gitbook.io/typescript/type-system/type-assertion#double-assertion
    // for more.
    const viewIds = values.viewIds as unknown as string[];
    setImportStatus("started");
    const r = await runCloudSQLImport(viewIds);
    if (r.status >= 400) {
      setErrorText(await r.text());
      setImportStatus("errored");
      return;
    }
    setImportStatus("done");
  };
  return (
    <>
      <PageHeader title="GCS CSV to Cloud SQL Import" />
      <Alert
        message="Caution!"
        description="You should only use this form if you are a member of the Line Staff Tools team, and you absolutely know what you are doing."
        type="warning"
        showIcon
      />
      <Form {...formLayout} className="buffer" onFinish={onFinish}>
        <Form.Item
          label="Views to Import"
          name="viewIds"
          rules={[{ required: true }]}
        >
          <Select mode="multiple">
            {data?.map((viewId: string) => {
              return (
                <Select.Option key={viewId} value={viewId}>
                  {viewId}
                </Select.Option>
              );
            })}
          </Select>
        </Form.Item>

        <Form.Item {...formTailLayout}>
          <Button type="primary" htmlType="submit">
            Start Import
          </Button>
        </Form.Item>
      </Form>
    </>
  );
};

export default CloudSQLImportView;
