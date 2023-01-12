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

import { Alert, Button, Form, Input, Modal } from "antd";
import { rem } from "polished";
import * as React from "react";
import { useState } from "react";
import { secondaryRawDataImportEnabledInState } from "../../AdminPanelAPI/IngestOperations";
import {
  DirectIngestInstance,
  GCP_STORAGE_BASE_URL,
} from "../IngestOperationsView/constants";
import { fetchCurrentIngestInstanceStatus } from "./IngestInstanceUtilities";

export enum RegionAction {
  StartIngestRerun = "start_ingest_rerun",
  TriggerTaskScheduler = "trigger_task_scheduler",
  PauseIngestQueues = "pause",
  ResumeIngestQueues = "resume",

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
  ingestInstance?: DirectIngestInstance | undefined;
}

interface CodeBlockProps {
  children: React.ReactNode;
}

const EmbeddedCode = ({ children }: CodeBlockProps): JSX.Element => (
  <code
    style={{
      backgroundColor: "#ffffff00",
      display: "inline",
      fontSize: rem("11px"),
    }}
  >
    {children}
  </code>
);

const ActionRegionConfirmationForm: React.FC<ActionRegionConfirmationFormProps> =
  ({
    visible,
    onConfirm,
    onCancel,
    action,
    actionName,
    regionCode,
    ingestInstance,
  }) => {
    const [form] = Form.useForm();
    const confirmationRegEx = ingestInstance
      ? regionCode
          .toUpperCase()
          .concat("_", action.toUpperCase(), "_", ingestInstance)
      : regionCode.toUpperCase().concat("_", action.toUpperCase());
    const isProduction = window.RUNTIME_GCP_ENVIRONMENT === "production";
    const projectId = isProduction ? "recidiviz-123" : "recidiviz-staging";
    const secondaryBucketURL = `${GCP_STORAGE_BASE_URL}${projectId}-direct-ingest-state-${regionCode
      .toLowerCase()
      .replace("_", "-")}-secondary`;
    const [
      ingestRerunRawDataSourceInstance,
      setIngestRerunRawDataSourceInstance,
    ] = useState<DirectIngestInstance | undefined>(undefined);
    // TODO(#13406): Remove flag once raw data can be processed in all states in secondary.
    const [secondaryRawDataImportEnabled, setSecondaryRawDataImportEnabled] =
      useState<boolean>(false);
    const [
      currentSecondaryIngestInstanceStatus,
      setSecondaryIngestInstanceStatus,
    ] = React.useState<string | null>(null);

    const canStartRerun =
      currentSecondaryIngestInstanceStatus === "NO_RERUN_IN_PROGRESS";

    const getData = React.useCallback(async () => {
      if (regionCode) {
        const response = await Promise.all([
          fetchCurrentIngestInstanceStatus(
            regionCode,
            DirectIngestInstance.SECONDARY
          ),
          secondaryRawDataImportEnabledInState(regionCode),
        ]);
        setSecondaryIngestInstanceStatus(response[0]);
        setSecondaryRawDataImportEnabled(await response[1].json());
      }
    }, [regionCode]);

    React.useEffect(() => {
      getData();
    }, [getData]);

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

    const ingestRerunForm = (
      <div>
        <b> Please select the source of the raw data for this rerun: </b>
        <ul>
          <li>
            If PRIMARY, then the rerun will just regenerate ingest view results
            using raw data already processed in PRIMARY.
          </li>
          <li>
            If SECONDARY, then you will need to first copy all raw data files
            into the SECONDARY ingest bucket that should be used for this rerun.
            Then the rerun will import that raw data to the
            <code>us_xx_raw_data_secondary</code> dataset in BigQuery and
            generate ingest view results based on that data.
          </li>
        </ul>
        {!secondaryRawDataImportEnabled ? (
          <i>
            For now, the raw data source for secondary reruns can only be
            PRIMARY.
          </i>
        ) : null}
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
              setIngestRerunRawDataSourceInstance(DirectIngestInstance.PRIMARY);
            }}
          >
            PRIMARY
          </Button>
          <Button
            // TODO(#13406): Remove 'disabled' setting once raw data can be processed in secondary.
            disabled={!secondaryRawDataImportEnabled}
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
        <h2> Ingest Rerun Summary </h2>
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
          <br />
          {ingestRerunRawDataSourceInstance === "SECONDARY" ? (
            <div>
              <Alert
                message={
                  <>
                    <b style={{ color: "red" }}>BEFORE KICKING OFF THE RERUN</b>
                  </>
                }
                type="warning"
                description={
                  <>
                    <b style={{ color: "red" }}>
                      You must first copy the raw data files you would like to
                      ingest in this rerun to the SECONDARY ingest bucket.
                    </b>
                    <ul>
                      <li>
                        <p>
                          This raw data will become the source of truth in{" "}
                          <EmbeddedCode>
                            {regionCode.toLowerCase()}_raw_data
                          </EmbeddedCode>
                          once the results of this rerun are flashed to primary.
                        </p>
                      </li>
                      <li>
                        <p>
                          In order to copy the raw files, you will likely want
                          to take advantage of the following scripts:
                          <ul>
                            <li>
                              <EmbeddedCode>
                                copy_raw_state_files_between_projects
                              </EmbeddedCode>
                            </li>
                            <li>
                              <EmbeddedCode>
                                move_raw_state_files_from_storage
                              </EmbeddedCode>
                            </li>
                          </ul>
                          These will copy and move raw files from the desired
                          storage bucket to the secondary ingest bucket.
                        </p>
                      </li>
                      <li>
                        <b>
                          Confirm that the raw files you would like to re-ingest
                          are present in the{" "}
                          <a href={secondaryBucketURL}>
                            secondary ingest bucket
                          </a>
                          .
                        </b>
                      </li>
                    </ul>
                  </>
                }
                showIcon
              />
            </div>
          ) : null}
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
      </div>
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
          <div>
            <h2>Current Ingest Instance Statuses</h2>
            <i>
              In order to start a new ingest rerun, the SECONDARY instance
              status needs to be NO_RERUN_IN_PROGRESS.
            </i>
            <br />
            <br />
            <p style={{ color: canStartRerun ? "green" : "red" }}>
              The SECONDARY instance status is&nbsp;
              {currentSecondaryIngestInstanceStatus}.&nbsp;
              {canStartRerun
                ? "Can proceed with rerun!"
                : "Cannot proceed with rerun."}
            </p>
          </div>
          {canStartRerun ? ingestRerunForm : undefined}
        </Modal>
      </>
    );

    return action === RegionAction.StartIngestRerun
      ? StartIngestRerunConfirmationModal
      : GenericIngestActionConfirmationModal;
  };

export default ActionRegionConfirmationForm;
