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

import { Form, Input, Modal } from "antd";
import * as React from "react";

import { DirectIngestInstance } from "../IngestStatus/constants";

export enum RegionAction {
  TriggerTaskScheduler = "trigger_task_scheduler",
  TriggerStateSpecificRawDataDAG = "trigger_state_specific_raw_data_dag",

  GenerateEmails = "generate",
  SendEmails = "send",

  StartRawDataReimport = "start_raw_data_reimport",
}

export const regionActionNames = {
  [RegionAction.TriggerTaskScheduler]: "Trigger Raw Data Import Scheduler",
  [RegionAction.TriggerStateSpecificRawDataDAG]:
    "Trigger State-Specific Raw Data Import DAG",

  [RegionAction.GenerateEmails]: "Generate Emails",
  [RegionAction.SendEmails]: "Send Emails",

  [RegionAction.StartRawDataReimport]: "Start Raw Data Reimport",
};

export interface RegionActionContext {
  ingestAction: RegionAction;
}

interface ActionRegionConfirmationFormProps {
  open: boolean;
  onConfirm: (context: RegionActionContext) => void;
  onCancel: () => void;
  action: RegionAction;
  actionName: string;
  regionCode: string;
  ingestInstance?: DirectIngestInstance | undefined;
}

const ActionRegionConfirmationForm: React.FC<
  ActionRegionConfirmationFormProps
> = ({
  open,
  onConfirm,
  onCancel,
  action,
  actionName,
  regionCode,
  ingestInstance,
}) => {
  const [form] = Form.useForm();
  const isProduction = window.RUNTIME_GCP_ENVIRONMENT === "production";
  const environmentName = isProduction ? "PROD" : "STAGING";
  const confirmationParts = [
    regionCode.toUpperCase(),
    action.toUpperCase(),
    environmentName,
  ];
  if (ingestInstance) {
    confirmationParts.push(ingestInstance);
  }
  const confirmationRegEx = confirmationParts.join("_");

  const GenericIngestActionConfirmationModal = (
    <Modal
      open={open}
      title={actionName || ""}
      okText="Ok"
      cancelText="Cancel"
      onCancel={onCancel}
      onOk={() => {
        form
          .validateFields()
          .then((values) => {
            form.resetFields();
            const ingestActionContext = {
              ingestAction: action,
            };
            onConfirm(ingestActionContext);
          })
          .catch((info) => {
            form.resetFields();
          });
      }}
    >
      Are you sure you want to
      <b> {actionName?.toLowerCase()} </b>
      for
      <b> {regionCode.toUpperCase()} </b>
      in
      <b> {environmentName}</b>
      {ingestInstance && <b> {ingestInstance}</b>}?
      <p>
        Type <b>{confirmationRegEx}</b> below to confirm.
      </p>
      <Form form={form} layout="vertical" name="form_in_modal">
        <Form.Item
          name="confirmation_code"
          rules={[
            {
              required: true,
              message: "Please input the confirmation code",
              pattern: RegExp(confirmationRegEx),
            },
          ]}
        >
          <Input />
        </Form.Item>
      </Form>
    </Modal>
  );

  return GenericIngestActionConfirmationModal;
};

export default ActionRegionConfirmationForm;
