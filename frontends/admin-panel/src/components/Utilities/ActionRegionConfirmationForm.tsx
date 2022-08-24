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

import { Button, Form, Input, Modal } from "antd";
import * as React from "react";
import { useState } from "react";
import { DirectIngestInstance } from "../IngestOperationsView/constants";

// TODO(#13406): Remove flag once raw data can be processed in secondary.
const disabledSecondaryIngestRerunRawDataSource = true;

export enum RegionAction {
  StartIngestRerun = "start_ingest_rerun",
  TriggerTaskScheduler = "trigger_task_scheduler",
  PauseIngestQueues = "pause",
  ResumeIngestQueues = "resume",

  PauseIngestInstance = "pause_instance",
  UnpauseIngestInstance = "unpause_instance",

  ExportToGCS = "export",
  ImportFromGCS = "import",

  GenerateEmails = "generate",
  SendEmails = "send",
}

export const regionActionNames = {
  [RegionAction.TriggerTaskScheduler]: "Trigger Task Scheduler",
  [RegionAction.StartIngestRerun]: "Start Ingest Rerun",
  [RegionAction.PauseIngestQueues]: "Pause Queues",
  [RegionAction.ResumeIngestQueues]: "Resume Queues",

  [RegionAction.PauseIngestInstance]: "Pause Instance",
  [RegionAction.UnpauseIngestInstance]: "Unpause Instance",

  [RegionAction.ExportToGCS]: "Export to GCS",
  [RegionAction.ImportFromGCS]: "Import from GCS",

  [RegionAction.GenerateEmails]: "Generate Emails",
  [RegionAction.SendEmails]: "Send Emails",
};

export interface RegionActionContext {
  ingestAction: RegionAction;
}

export interface StartIngestRerunContext extends RegionActionContext {
  ingestRerunRawDataSourceInstance: DirectIngestInstance;
}

interface ActionRegionConfirmationFormProps {
  visible: boolean;
  onConfirm: (context: RegionActionContext) => void;
  onCancel: () => void;
  action: RegionAction;
  actionName: string;
  regionCode: string;
  projectId?: string | undefined;
  ingestInstance?: DirectIngestInstance | undefined;
}

const ActionRegionConfirmationForm: React.FC<ActionRegionConfirmationFormProps> =
  ({
    visible,
    onConfirm,
    onCancel,
    action,
    actionName,
    regionCode,
    projectId,
    ingestInstance,
  }) => {
    const [form] = Form.useForm();
    const confirmationRegEx = ingestInstance
      ? regionCode
          .toUpperCase()
          .concat("_", action.toUpperCase(), "_", ingestInstance)
      : regionCode.toUpperCase().concat("_", action.toUpperCase());

    const [
      ingestRerunRawDataSourceInstance,
      setIngestRerunRawDataSourceInstance,
    ] = useState<DirectIngestInstance | undefined>(undefined);

    const GenericIngestActionConfirmationModal = (
      <>
        <Modal
          visible={visible}
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
          <p>
            Are you sure you want to
            <b> {actionName?.toLowerCase()} </b>
            for
            <b>{ingestInstance ? ` ${ingestInstance}` : ""}</b>
            {ingestInstance ? " ingest instance in " : " "}
            <b>{regionCode.toUpperCase()}</b>?
          </p>
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
      </>
    );

    const StartIngestRerunConfirmationModal = (
      <>
        <Modal
          visible={visible}
          title={actionName || ""}
          okText="OK"
          okButtonProps={{
            disabled: ingestRerunRawDataSourceInstance === undefined,
          }}
          cancelText="Cancel"
          onCancel={() => {
            setIngestRerunRawDataSourceInstance(undefined);
            onCancel();
          }}
          onOk={() => {
            form
              .validateFields()
              .then((values) => {
                form.resetFields();
                if (ingestRerunRawDataSourceInstance === undefined) {
                  throw new Error(
                    "Must have a defined ingestRerunRawDataSourceInstance before starting ingest rerun."
                  );
                }
                const rerunContext = {
                  ingestAction: action,
                  ingestRerunRawDataSourceInstance,
                };
                onConfirm(rerunContext);
              })
              .catch((info) => {
                form.resetFields();
              });
          }}
        >
          <b> Please select the source of the raw data for this rerun: </b>
          <ul>
            <li>
              If PRIMARY, then the rerun will just regenerate ingest view
              results using raw data already processed in PRIMARY.
            </li>
            <li>
              If SECONDARY, then the rerun will first import/process raw data in
              SECONDARY, then regenerate ingest view results.
            </li>
          </ul>
          <i>
            For now, the raw data source for secondary reruns can only be
            PRIMARY.
          </i>
          <div
            style={{
              display: "flex",
              alignItems: "center",
              justifyContent: "space-evenly",
            }}
          >
            <Button
              style={{ marginRight: 5 }}
              onClick={async () => {
                setIngestRerunRawDataSourceInstance(
                  DirectIngestInstance.PRIMARY
                );
              }}
            >
              PRIMARY
            </Button>
            <Button
              // TODO(#13406): Remove 'disabled' setting once raw data can be processed in secondary.
              disabled={disabledSecondaryIngestRerunRawDataSource}
              style={{ marginRight: 5 }}
              onClick={async () => {
                setIngestRerunRawDataSourceInstance(
                  DirectIngestInstance.SECONDARY
                );
              }}
            >
              SECONDARY
            </Button>
          </div>
          <br />
          <br />
          <b> Ingest Rerun Summary </b>
          <br />
          The rerun will have the following configurations:
          <ul>
            <li>
              <b>Project ID: </b>
              {projectId ? projectId.toLowerCase() : ""}
            </li>
            <li>
              <b>State Code: </b>
              {regionCode.toUpperCase()}
            </li>
            <li>
              <b>Rerun Instance: </b>
              {ingestInstance ? ` ${ingestInstance}` : ""}
            </li>
            <li>
              <b
                style={{
                  color:
                    ingestRerunRawDataSourceInstance === undefined
                      ? "red"
                      : "green",
                  justifyContent: "space-between",
                }}
              >
                Raw Data Source Instance:&nbsp;
              </b>
              {ingestRerunRawDataSourceInstance}
            </li>
          </ul>
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
      </>
    );

    return action === RegionAction.StartIngestRerun
      ? StartIngestRerunConfirmationModal
      : GenericIngestActionConfirmationModal;
  };

export default ActionRegionConfirmationForm;
