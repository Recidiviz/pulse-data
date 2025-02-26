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
import {
  Alert,
  AlertProps,
  Button,
  Card,
  Empty,
  Form,
  Input,
  message,
  Select,
  Spin,
  Typography,
} from "antd";
import * as React from "react";
import { generateEmails } from "../../AdminPanelAPI/LineStaffTools";
import ActionRegionConfirmationForm, {
  RegionAction,
  regionActionNames,
} from "../Utilities/ActionRegionConfirmationForm";
import DataFreshnessInfo from "../Utilities/DataFreshnessInfo";
import { layout, POEmailsFormProps, tailLayout } from "./constants";

interface GenerateFormData {
  state: string;
  testAddress: string;
  regionCode: string;
  messageBodyOverride: string;
  emailAllowlist: string[];
}

const GenerateEmails: React.FC<POEmailsFormProps> = ({
  stateInfo,
  reportType,
}) => {
  const isProduction = window.RUNTIME_GCP_ENVIRONMENT === "production";
  const projectId = isProduction ? "recidiviz-123" : "recidiviz-staging";

  const [formData, setFormData] =
    React.useState<GenerateFormData | undefined>(undefined);
  const [showSpinner, setShowSpinner] = React.useState(false);
  const [isConfirmationModalVisible, setIsConfirmationModalVisible] =
    React.useState(false);
  const [generationResult, setGenerationResult] =
    React.useState<AlertProps | void>();

  const onFinish = (values?: GenerateFormData | undefined) => {
    setFormData(values);
    showConfirmationModal();
  };

  const onEmailActionConfirmation = async () => {
    if (!stateInfo || !reportType) return;

    setGenerationResult();
    setIsConfirmationModalVisible(false);
    setShowSpinner(true);
    message.info("Generating emails...");
    const r = await generateEmails(
      stateInfo.code,
      reportType,
      formData?.testAddress,
      formData?.regionCode,
      formData?.messageBodyOverride,
      formData?.emailAllowlist
    );
    if (r.status === 200) {
      const { batchId, statusText } = await r.json();
      setGenerationResult({
        type: "success",
        message: "Email generation succeeded",
        description: (
          <>
            <p>{statusText}</p>
            <p>
              Bucket link to {projectId}-report-html for {stateInfo.name}, batch
              <a
                href={`https://console.cloud.google.com/storage/browser/${projectId}-report-html/${stateInfo.code}/${batchId}`}
                rel="noreferrer"
                style={{ margin: 10 }}
                target="_blank"
              >
                {batchId}
              </a>
            </p>
          </>
        ),
      });
    } else {
      const isPartialFailure = r.status === 207;
      const text = await r.text();
      message.error("Error generating emails");
      setGenerationResult({
        type: isPartialFailure ? "warning" : "error",
        message: `Email generation ${
          isPartialFailure ? "partially" : ""
        } failed`,
        description: (
          <>
            <p>{text}</p>
            <p>
              See{" "}
              <a
                href={`https://console.cloud.google.com/logs/query;query=resource.type%3D%22gae_app%22%20resource.labels.module_id%3D%22default%22%20httpRequest.requestUrl%3D%22%2Fadmin%2Fapi%2Fline_staff_tools%2F${stateInfo.code}%2Fgenerate_emails%22%0A?project=${projectId}`}
                rel="noreferrer"
                target="_blank"
              >
                logs
              </a>{" "}
              for more detail
            </p>
          </>
        ),
      });
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
      <Card
        title={`Generate ${stateInfo?.name || ""} ${reportType || ""} Emails`}
        style={{ margin: 10, height: "95%" }}
      >
        {stateInfo && reportType ? (
          <>
            <Form
              {...layout}
              className="buffer"
              onFinish={(values) => {
                onFinish(values);
              }}
            >
              {stateInfo && <DataFreshnessInfo state={stateInfo.code} />}
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
              <Form.Item
                label="Message Body Override"
                name="messageBodyOverride"
              >
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
                <Button type="primary" htmlType="submit" disabled={showSpinner}>
                  Generate Emails
                </Button>
                <br />
                <Typography.Text type="secondary">
                  (May take a few minutes)
                </Typography.Text>
              </Form.Item>
            </Form>
            {showSpinner && <Spin />}
            {generationResult && <Alert {...generationResult} />}
          </>
        ) : (
          <Empty />
        )}
      </Card>

      {stateInfo ? (
        <ActionRegionConfirmationForm
          visible={isConfirmationModalVisible}
          onConfirm={onEmailActionConfirmation}
          onCancel={onConfirmationCancel}
          action={RegionAction.GenerateEmails}
          actionName={regionActionNames[RegionAction.GenerateEmails]}
          regionCode={stateInfo.code}
        />
      ) : null}
    </>
  );
};

export default GenerateEmails;
