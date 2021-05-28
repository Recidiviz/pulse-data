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
import { Alert, Button, Form, PageHeader } from "antd";
import { WarningFilled } from "@ant-design/icons";
import { useState } from "react";
import { useHistory, useLocation } from "react-router-dom";
import { StateCodeInfo } from "./IngestOperationsView/constants";
import { fetchEmailStateCodes } from "../AdminPanelAPI";
import ActionRegionConfirmationForm from "./Utilities/ActionRegionConfirmationForm";
import useFetchedData from "../hooks";
import StateSelector from "./Utilities/StateSelector";

const POEmailsView = (): JSX.Element => {
  const env = window.RUNTIME_GCP_ENVIRONMENT || "uknown env";
  const projectId =
    env === "production" ? "recidiviz-123" : "recidiviz-staging";

  enum EmailActions {
    GenerateEmails = "generate",
  }

  const actionNames = { [EmailActions.GenerateEmails]: "Generate Emails" };

  const [stateCode, setStateCode] = React.useState<string | null>(null);
  const [isConfirmationModalVisible, setIsConfirmationModalVisible] =
    React.useState(false);

  const layout = {
    labelCol: { span: 2 },
    wrapperCol: { span: 20 },
  };

  const tailLayout = {
    wrapperCol: { offset: 2, span: 20 },
  };

  const onEmailActionConfirmation = async (
    emailActionToExecute: string | null
  ) => {
    setIsConfirmationModalVisible(false);
  };

  const showConfirmationModal = () => {
    setIsConfirmationModalVisible(true);
  };

  const { loading, data } =
    useFetchedData<StateCodeInfo[]>(fetchEmailStateCodes);

  return (
    <>
      <PageHeader title="PO Emails" />
      <Alert
        message={
          <>
            <WarningFilled /> Caution!
          </>
        }
        description="You should only use this form if you are a member of the Line Staff Tools team, and you know absolutely know what you are doing."
        type="warning"
      />
      <Form {...layout} className="buffer">
        <Form.Item label="State" name="state" rules={[{ required: true }]}>
          <StateSelector
            handleStateCodeChange={setStateCode}
            loading={false}
            data={data}
          />
        </Form.Item>
        <Form.Item {...tailLayout}>
          <Button
            type="primary"
            htmlType="submit"
            onClick={() => {
              showConfirmationModal();
            }}
          >
            Generate Emails
          </Button>
        </Form.Item>
      </Form>

      {stateCode ? (
        <ActionRegionConfirmationForm
          visible={isConfirmationModalVisible}
          onConfirm={onEmailActionConfirmation}
          onCancel={() => {
            setIsConfirmationModalVisible(false);
          }}
          action={EmailActions.GenerateEmails}
          actionName={actionNames[EmailActions.GenerateEmails]}
          regionCode={stateCode}
        />
      ) : null}
    </>
  );
};

export default POEmailsView;
