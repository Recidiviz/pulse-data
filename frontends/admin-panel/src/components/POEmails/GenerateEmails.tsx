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
import { Button, Card, Form, Input, message, Select, Spin } from "antd";
import * as React from "react";
import {
  fetchEmailStateCodes,
  generateEmails,
} from "../../AdminPanelAPI/LineStaffTools";
import useFetchedData from "../../hooks";
import { StateCodeInfo } from "../IngestOperationsView/constants";
import ActionRegionConfirmationForm from "../Utilities/ActionRegionConfirmationForm";
import { layout, tailLayout } from "./constants";
import StateSelector from "../Utilities/StateSelector";
import DataFreshnessInfo from "../Utilities/DataFreshnessInfo";

interface GenerateFormData {
  state: string;
  testAddress: string;
  regionCode: string;
  messageBodyOverride: string;
  emailAllowlist: string[];
}

const GenerateEmails = (): JSX.Element => {
  const isProduction = window.RUNTIME_GCP_ENVIRONMENT === "production";
  const projectId = isProduction ? "recidiviz-123" : "recidiviz-staging";

  const [formData, setFormData] =
    React.useState<GenerateFormData | undefined>(undefined);
  const [batchId, setBatchId] = React.useState<string | null>(null);
  const [showSpinner, setShowSpinner] = React.useState(false);
  const [isConfirmationModalVisible, setIsConfirmationModalVisible] =
    React.useState(false);
  const [stateCode, setStateCode] =
    React.useState<string | undefined>(undefined);

  const { loading, data } =
    useFetchedData<StateCodeInfo[]>(fetchEmailStateCodes);

  const onFinish = (values?: GenerateFormData | undefined) => {
    setFormData(values);
    showConfirmationModal();
  };

  const onEmailActionConfirmation = async () => {
    setIsConfirmationModalVisible(false);
    setShowSpinner(true);
    message.info("Generating emails...");
    if (formData?.state) {
      const r = await generateEmails(
        formData.state,
        formData.testAddress,
        formData.regionCode,
        formData.messageBodyOverride
      );
      if (r.status >= 400) {
        const text = await r.text();
        message.error(`Generate emails... failed: ${text}`);
        setBatchId(null);
      } else {
        const json = await r.json();
        message.success(`Generate emails... succeeded! ${json.statusText}`);
        setBatchId(json.batchId);
      }
    }
    setShowSpinner(false);
  };

  const onConfirmationCancel = () => {
    setIsConfirmationModalVisible(false);
    setFormData(undefined);
  };

  const showConfirmationModal = () => {
    setIsConfirmationModalVisible(true);
  };

  return (
    <>
      <Card title="Generate Emails" style={{ margin: 10, height: "95%" }}>
        <Form
          {...layout}
          className="buffer"
          onFinish={(values) => {
            onFinish(values);
          }}
        >
          <Form.Item label="State" name="state" rules={[{ required: true }]}>
            <StateSelector
              loading={loading}
              data={data}
              onChange={(state) => setStateCode(state)}
            />
          </Form.Item>
          {stateCode && <DataFreshnessInfo state={stateCode} />}
          <Form.Item
            label="Test Address"
            name="testAddress"
            rules={[{ type: "email" }]}
          >
            <Input placeholder="xxx@recidiviz.org" />
          </Form.Item>
          <Form.Item label="Region Code" name="regionCode">
            <Input placeholder="ex. US_ID_D5" />
          </Form.Item>
          <Form.Item label="Message Body Override" name="messageBodyOverride">
            <Input.TextArea />
          </Form.Item>
          <Form.Item
            label="Email Allowlist"
            name="emailAllowlist"
            rules={[{ type: "array" }]}
          >
            <Select
              mode="tags"
              tokenSeparators={[",", " "]}
              placeholder="cc1@domain.gov, cc2@domain.gov"
            >
              <option value="user@recidiviz.org">user@recidiviz.org</option>
            </Select>
          </Form.Item>
          <Form.Item {...tailLayout}>
            <Button type="primary" htmlType="submit">
              Generate Emails
            </Button>
          </Form.Item>
        </Form>
        {showSpinner ? <Spin /> : null}
        {batchId ? (
          <p>
            Bucket link to {projectId}-report-html for {formData?.state}, batch
            <a
              style={{ margin: 10 }}
              href={`https://console.cloud.google.com/storage/browser/${projectId}-report-html/${formData?.state}/${batchId}`}
            >
              {batchId}
            </a>
          </p>
        ) : null}
      </Card>

      {formData?.state ? (
        <ActionRegionConfirmationForm
          visible={isConfirmationModalVisible}
          onConfirm={onEmailActionConfirmation}
          onCancel={onConfirmationCancel}
          action="generate"
          actionName="Generate Emails"
          regionCode={formData.state}
        />
      ) : null}
    </>
  );
};

export default GenerateEmails;
