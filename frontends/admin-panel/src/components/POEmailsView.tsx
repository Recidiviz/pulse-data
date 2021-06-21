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
import { WarningFilled } from "@ant-design/icons";
import {
  Alert,
  Button,
  Card,
  Form,
  Input,
  message,
  PageHeader,
  Spin,
} from "antd";
import * as React from "react";
import {
  fetchEmailStateCodes,
  generateEmails,
} from "../AdminPanelAPI/LineStaffTools";
import useFetchedData from "../hooks";
import { StateCodeInfo } from "./IngestOperationsView/constants";
import ActionRegionConfirmationForm from "./Utilities/ActionRegionConfirmationForm";
import StateSelector from "./Utilities/StateSelector";

const POEmailsView = (): JSX.Element => {
  const isProduction = window.RUNTIME_GCP_ENVIRONMENT === "production";
  const projectId = isProduction ? "recidiviz-123" : "recidiviz-staging";
  enum EmailActions {
    GenerateEmails = "generate",
  }

  const actionNames = { [EmailActions.GenerateEmails]: "Generate Emails" };
  const [isConfirmationModalVisible, setIsConfirmationModalVisible] =
    React.useState(false);
  const [emailsGenerated, setEmailsGenerated] = React.useState(false);
  const [formData, setFormData] =
    React.useState<{ [key: string]: string } | null>(null);
  const [showSpinner, setShowSpinner] = React.useState(false);
  const { loading, data } =
    useFetchedData<StateCodeInfo[]>(fetchEmailStateCodes);

  const layout = {
    labelCol: { span: 6 },
    wrapperCol: { span: 16 },
  };

  const tailLayout = {
    wrapperCol: { offset: 6, span: 16 },
  };

  const onFinish = (values: { [key: string]: string }) => {
    setFormData(values);
    showConfirmationModal();
  };

  const onEmailActionConfirmation = async () => {
    setIsConfirmationModalVisible(false);
    if (formData?.state) {
      message.info("Generating emails...");
      const r = await generateEmails(
        formData.state,
        formData.testAddress,
        formData.regionCode,
        formData.messageBodyOverride
      );
      message.info("This might take a while...");
      setShowSpinner(true);
      if (r.status >= 400) {
        const text = await r.text();
        message.error(`Generate emails failed: ${text}`);
      } else {
        message.success("Generate emails succeeded!");
        setEmailsGenerated(true);
      }
      setShowSpinner(false);
    }
  };

  const showConfirmationModal = () => {
    setIsConfirmationModalVisible(true);
  };

  function bucketLink(environment: string, stateCode?: string) {
    const projectName =
      environment === "production" ? "recidiviz-123" : "recidiviz-staging";
    return (
      <a
        href={`https://console.cloud.google.com/storage/browser/${projectName}-report-html/${stateCode}`}
      >
        Bucket link to {projectName}-report-html for {stateCode}.
      </a>
    );
  }

  return (
    <>
      <PageHeader title="PO Emails" />
      <Alert
        message={
          <>
            <WarningFilled /> Caution!
          </>
        }
        description="You should only use this form if you are a member of the Line Staff Tools team, and you absolutely know what you are doing."
        type="warning"
      />
      <Card style={{ margin: 10 }}>
        <Form
          {...layout}
          className="buffer"
          onFinish={(values) => {
            onFinish(values);
          }}
        >
          <Form.Item label="State" name="state" rules={[{ required: true }]}>
            <StateSelector loading={loading} data={data} />
          </Form.Item>
          <Form.Item
            label="Test Address"
            name="testAddress"
            rules={[{ required: false, type: "email" }]}
          >
            <Input placeholder="xxx@recidiviz.org" />
          </Form.Item>
          <Form.Item
            label="Region Code"
            name="regionCode"
            rules={[{ required: false }]}
          >
            <Input placeholder="ex. US_ID_D5" />
          </Form.Item>
          <Form.Item
            label="Message Body Override"
            name="messageBodyOverride"
            rules={[{ required: false }]}
          >
            <Input.TextArea />
          </Form.Item>
          <Form.Item {...tailLayout}>
            <Button type="primary" htmlType="submit">
              Generate Emails
            </Button>
          </Form.Item>
          {showSpinner ? <Spin /> : null}
        </Form>
      </Card>
      {formData?.state ? (
        <ActionRegionConfirmationForm
          visible={isConfirmationModalVisible}
          onConfirm={onEmailActionConfirmation}
          onCancel={() => {
            setIsConfirmationModalVisible(false);
          }}
          action={EmailActions.GenerateEmails}
          actionName={actionNames[EmailActions.GenerateEmails]}
          regionCode={formData.state}
        />
      ) : null}

      {emailsGenerated ? bucketLink(projectId, formData?.state) : null}
    </>
  );
};

export default POEmailsView;
