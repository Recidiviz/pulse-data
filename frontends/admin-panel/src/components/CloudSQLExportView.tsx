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
import { Alert, Button, Form, PageHeader, Result } from "antd";
import { WarningFilled } from "@ant-design/icons";

import { generateCaseUpdatesExport } from "../AdminPanelAPI";

const CloudSQLExportView = (): JSX.Element => {
  const [exportStatus, setExportStatus] =
    React.useState<"not-started" | "started" | "done" | "errored">(
      "not-started"
    );
  const [errorText, setErrorText] = React.useState<string>("");

  if (exportStatus === "started") {
    return (
      <Result
        title="Export has been started..."
        subTitle="This page will update when the export has completed successfully."
      />
    );
  }
  if (exportStatus === "done") {
    return (
      <Result status="success" title="Export has completed successfully!" />
    );
  }
  if (exportStatus === "errored") {
    return (
      <Result
        status="error"
        title="Something went wrong during export."
        subTitle={errorText}
      />
    );
  }

  const tailLayout = {
    wrapperCol: { offset: 4, span: 20 },
  };

  const startExport = async () => {
    setExportStatus("started");
    const r = await generateCaseUpdatesExport();
    if (r.status >= 400) {
      setErrorText(await r.text());
      setExportStatus("errored");
      return;
    }
    setExportStatus("done");
  };
  return (
    <>
      <PageHeader title="GCS CSV to Cloud SQL Export" />
      <Alert
        message={
          <>
            <WarningFilled /> Caution!
          </>
        }
        description={
          <>
            <p>
              You should only use this form if you are a member of the Line
              Staff Tools team, and you absolutely know what you are doing.
            </p>
            <p>This button exports the current set of recorded case actions.</p>
            <p>
              If a CSV has previously been exported and exists in the bucket,
              this request will fail.
            </p>
          </>
        }
        type="warning"
      />

      <Form className="buffer" onFinish={startExport}>
        <Form.Item {...tailLayout}>
          <Button type="primary" htmlType="submit">
            Export Case Actions
          </Button>
        </Form.Item>
      </Form>
    </>
  );
};

export default CloudSQLExportView;
