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
  Button,
  message,
  Modal,
  PageHeader,
  StepProps,
  Steps,
} from "antd";
import * as React from "react";
import { useHistory } from "react-router-dom";
import {
  acquireBQExportLock,
  exportDatabaseToGCS,
  fetchIngestStateCodes,
  importDatabaseFromGCS,
  pauseDirectIngestInstance,
  releaseBQExportLock,
  updateIngestQueuesState,
} from "../AdminPanelAPI";
import useFetchedData from "../hooks";
import {
  DirectIngestInstance,
  QueueState,
  StateCodeInfo,
} from "./IngestOperationsView/constants";
import NewTabLink from "./NewTabLink";
import StateSelector from "./Utilities/StateSelector";

interface StyledStepProps extends StepProps {
  nextButtonTitle?: string;
  onNextButtonClick?: () => Promise<void>;
}

interface CodeBlockProps {
  children: React.ReactNode;
  enabled: boolean;
}

const CodeBlock = ({ children, enabled }: CodeBlockProps): JSX.Element => (
  <code
    style={{
      display: "block",
      whiteSpace: "pre-wrap",
      padding: 10,
      borderRadius: 10,
      backgroundColor: enabled ? "#fafafa" : "#d9d9d9",
      color: enabled ? "rgba(0, 0, 0, 0.85)" : "rgba(0, 0, 0, 0.45)",
    }}
  >
    {children}
  </code>
);

const FlashDatabaseChecklist = (): JSX.Element => {
  const isProduction = window.RUNTIME_GCP_ENVIRONMENT === "production";
  const projectId = isProduction ? "recidiviz-123" : "recidiviz-staging";
  const operationsPageURL = `https://go/${
    isProduction ? "prod" : "dev"
  }-state-data-operations`;

  const [currentStep, setCurrentStep] = React.useState(0);
  const [stateCode, setStateCode] = React.useState<string | null>(null);
  const [modalVisible, setModalVisible] = React.useState(true);

  const history = useHistory();

  const incrementCurrentStep = async () => setCurrentStep(currentStep + 1);
  const runAndCheckStatus = async (
    fn: () => Promise<Response>
  ): Promise<boolean> => {
    const r = await fn();
    if (r.status >= 400) {
      const text = await r.text();
      message.error(`Error: ${text}`);
      return false;
    }
    return true;
  };

  const StyledStep = ({
    nextButtonTitle = "Mark Done",
    onNextButtonClick,
    description,
    ...rest
  }: StyledStepProps): JSX.Element => {
    const [loading, setLoading] = React.useState(false);

    const jointDescription = (
      <>
        {description}
        <Button
          type="primary"
          onClick={async () => {
            setLoading(true);
            if (onNextButtonClick) {
              await onNextButtonClick();
            } else {
              await incrementCurrentStep();
            }
            setLoading(false);
          }}
          loading={loading}
          style={rest.status === "process" ? undefined : { display: "none" }}
        >
          {nextButtonTitle}
        </Button>
        {onNextButtonClick && (
          <Button
            ghost
            type="primary"
            onClick={async () => {
              setLoading(true);
              await incrementCurrentStep();
              setLoading(false);
            }}
            loading={loading}
            style={
              rest.status === "process"
                ? { marginLeft: 5 }
                : { display: "none" }
            }
          >
            Mark Done
          </Button>
        )}
      </>
    );

    return (
      <Steps.Step
        style={{ paddingBottom: 5 }}
        description={jointDescription}
        {...rest}
      />
    );
  };

  const contents =
    stateCode === null ? (
      <Alert
        message="Select a state"
        description="Once you pick a state, this form will display the set of instructions required to flash a secondary database to primary."
        type="info"
        showIcon
      />
    ) : (
      <Steps progressDot current={currentStep} direction="vertical">
        <StyledStep
          title="Pause Queues"
          description={
            <p>Pause all of the ingest-related queues for {stateCode}.</p>
          }
          nextButtonTitle="Pause Queues"
          onNextButtonClick={async () => {
            const request = async () =>
              updateIngestQueuesState(stateCode, QueueState.PAUSED);
            const succeeded = await runAndCheckStatus(request);
            if (succeeded) {
              await incrementCurrentStep();
            }
          }}
        />
        <StyledStep
          title="Acquire PRIMARY Ingest Lock"
          description={
            <p>
              Acquire the ingest lock for {stateCode}&#39;s primary ingest
              instance. This prevents other operations from updating ingest
              databases until the lock is released.
            </p>
          }
          nextButtonTitle="Acquire Lock"
          onNextButtonClick={async () => {
            const request = async () =>
              acquireBQExportLock(stateCode, DirectIngestInstance.PRIMARY);
            const succeeded = await runAndCheckStatus(request);
            if (succeeded) {
              await incrementCurrentStep();
            }
          }}
        />
        <StyledStep
          title="Acquire SECONDARY Ingest Lock"
          description={
            <p>
              Acquire the ingest lock for {stateCode}&#39;s secondary ingest
              instance. This prevents other operations from updating ingest
              databases until the lock is released.
            </p>
          }
          nextButtonTitle="Acquire Lock"
          onNextButtonClick={async () => {
            const request = async () =>
              acquireBQExportLock(stateCode, DirectIngestInstance.SECONDARY);
            const succeeded = await runAndCheckStatus(request);
            if (succeeded) {
              await incrementCurrentStep();
            }
          }}
        />
        <StyledStep
          title="Export secondary instance data to GCS"
          description={
            <p>
              Export a SQL dump of all data in the {stateCode.toLowerCase()}
              _secondary database to cloud storage bucket{" "}
              <code>{projectId}-cloud-sql-exports</code>. <br />
              You can check your progress in the{" "}
              <NewTabLink href={operationsPageURL}>
                Operations section
              </NewTabLink>{" "}
              of the STATE SQL instance page. If this request times out, but the
              operation succeeds, just select &#39;Mark Done&#39;.
            </p>
          }
          nextButtonTitle="Export Data"
          onNextButtonClick={async () => {
            const request = async () =>
              exportDatabaseToGCS(stateCode, DirectIngestInstance.SECONDARY);
            const succeeded = await runAndCheckStatus(request);
            if (succeeded) {
              await incrementCurrentStep();
            }
          }}
        />
        <StyledStep
          title="Drop data from primary database"
          description={
            <>
              <p>
                Drop all data from the{" "}
                <code>{stateCode.toLowerCase()}_primary</code> database. To do
                so, run this script locally run inside a pipenv shell:
              </p>
              <p>
                <CodeBlock enabled={currentStep === 4}>
                  python -m recidiviz.tools.migrations.purge_state_db \<br />
                  {"    "}--state-code {stateCode} \<br />
                  {"    "}--ingest-instance PRIMARY \<br />
                  {"    "}--project-id {projectId} \<br />
                  {"    "}--purge-schema
                </CodeBlock>
              </p>
            </>
          }
        />
        <StyledStep
          title="Move primary files to storage"
          description={
            <>
              <p>
                Move all primary instance ingest view files to deprecated
                storage:
              </p>
              <p>
                <CodeBlock enabled={currentStep === 5}>
                  python -m
                  recidiviz.tools.ingest.operations.move_storage_files_to_deprecated
                  \<br />
                  {"    "}--file-type ingest_view \<br />
                  {"    "}--region {stateCode.toLowerCase()} \<br />
                  {"    "}--ingest-instance PRIMARY \<br />
                  {"    "}--project-id {projectId} \<br />
                  {"    "}--dry-run False
                </CodeBlock>
              </p>
              <p>and then run:</p>
              <p>
                <CodeBlock enabled={currentStep === 5}>
                  gsutil rm gs://{projectId}-direct-ingest-state-storage/
                  {stateCode.toLowerCase()}/ingest_view/*/*/*/split_files/*
                </CodeBlock>
              </p>
            </>
          }
        />
        <StyledStep
          title="Deprecate primary instance operation database rows"
          description={
            <>
              <p>
                Drop all <code>{stateCode.toLowerCase()}_primary</code> from
                operations database.
              </p>
              <ol style={{ paddingLeft: 20 }}>
                <li>
                  SSH into <code>prod-data-client</code> and log into the
                  operations database via{" "}
                  <code>{isProduction ? "prod" : "dev"}-operations-psql</code>.
                </li>
                <li>
                  Drop all rows for{" "}
                  <code>{stateCode.toLowerCase()}_primary</code> from{" "}
                  <code>direct_ingest_ingest_file_metadata</code>:
                  <CodeBlock enabled={currentStep === 6}>
                    UPDATE direct_ingest_ingest_file_metadata <br />
                    SET is_invalidated = TRUE <br />
                    WHERE region_code = &#39;{stateCode}&#39; <br />
                    {"  "}AND ingest_database_name = &#39;
                    {stateCode.toLowerCase()}
                    _primary&#39;;
                  </CodeBlock>
                </li>
              </ol>
            </>
          }
        />
        <StyledStep
          title="Import data from secondary"
          description={
            <p>
              Load exported data from{" "}
              <code>{stateCode.toLowerCase()}_secondary</code> into{" "}
              <code>{stateCode.toLowerCase()}_primary</code>.
            </p>
          }
          nextButtonTitle="Import Data"
          onNextButtonClick={async () => {
            const request = async () =>
              importDatabaseFromGCS(
                stateCode,
                DirectIngestInstance.PRIMARY,
                DirectIngestInstance.SECONDARY
              );
            const succeeded = await runAndCheckStatus(request);
            if (succeeded) {
              await incrementCurrentStep();
            }
          }}
        />
        <StyledStep
          title="Transition secondary instance operations information to primary"
          description={
            <>
              <p>
                Update all rows in operations database that had database{" "}
                <code>{stateCode.toLowerCase()}_secondary</code> with updated
                database name <code>{stateCode.toLowerCase()}_primary</code>.
                <br />
                You can check your progress in the{" "}
                <NewTabLink href={operationsPageURL}>
                  Operations section
                </NewTabLink>{" "}
                of the STATE SQL instance page. If this request times out, but
                the operation succeeds, just select &#39;Mark Done&#39;.
              </p>
              <ol style={{ paddingLeft: 20 }}>
                <li>
                  SSH into <code>prod-data-client</code> and run{" "}
                  <code>{isProduction ? "prod" : "dev"}-operations-psql</code>.
                </li>
                <li>
                  Run the following SQL query to update the tables:
                  <CodeBlock enabled={currentStep === 8}>
                    UPDATE direct_ingest_ingest_file_metadata <br />
                    SET ingest_database_name = &#39;{stateCode.toLowerCase()}
                    _primary&#39; <br />
                    WHERE ingest_database_name = &#39;
                    {stateCode.toLowerCase()}_secondary&#39; <br />
                    {"  "}AND region_code = &#39;{stateCode}&#39;;
                  </CodeBlock>
                </li>
              </ol>
            </>
          }
        />
        <StyledStep
          title="Update ingest view files"
          description={
            <>
              <p>
                Move all ingest_view files from secondary storage to primary
                storage.
              </p>
              <p>
                <CodeBlock enabled={currentStep === 9}>
                  python -m
                  recidiviz.tools.ingest.operations.move_ingest_views_from_secondary_to_primary
                  \<br />
                  {"    "}--region {stateCode.toLowerCase()} \<br />
                  {"    "}--project-id {projectId} \<br />
                  {"    "}--dry-run False
                </CodeBlock>
              </p>
            </>
          }
        />
        <StyledStep
          title="Release PRIMARY Ingest Lock"
          description={
            <p>
              Release the ingest lock for {stateCode}&#39;s primary instance.
            </p>
          }
          nextButtonTitle="Release Lock"
          onNextButtonClick={async () => {
            const request = async () =>
              releaseBQExportLock(stateCode, DirectIngestInstance.PRIMARY);
            const succeeded = await runAndCheckStatus(request);
            if (succeeded) {
              await incrementCurrentStep();
            }
          }}
        />
        <StyledStep
          title="Release SECONDARY Ingest Lock"
          description={
            <p>
              Release the ingest lock for {stateCode}&#39;s secondary instance.
            </p>
          }
          nextButtonTitle="Release Lock"
          onNextButtonClick={async () => {
            const request = async () =>
              releaseBQExportLock(stateCode, DirectIngestInstance.SECONDARY);
            const succeeded = await runAndCheckStatus(request);
            if (succeeded) {
              await incrementCurrentStep();
            }
          }}
        />
        <StyledStep
          title="Pause secondary ingest"
          description={
            <p>Mark secondary ingest as paused in the operations db.</p>
          }
          nextButtonTitle="Mark Paused"
          onNextButtonClick={async () => {
            const request = async () =>
              pauseDirectIngestInstance(
                stateCode,
                DirectIngestInstance.SECONDARY
              );
            const succeeded = await runAndCheckStatus(request);
            if (succeeded) {
              await incrementCurrentStep();
            }
          }}
        />
        <StyledStep
          title="Clear secondary database"
          description={
            <>
              <p>
                Drop all data from the{" "}
                <code>{stateCode.toLowerCase()}_secondary</code> database. To do
                so, run this script locally inside a pipenv shell:
              </p>
              <p>
                <CodeBlock enabled={currentStep === 13}>
                  python -m recidiviz.tools.migrations.purge_state_db \<br />
                  {"    "}--state-code {stateCode} \<br />
                  {"    "}--ingest-instance SECONDARY \<br />
                  {"    "}--project-id {projectId} \<br />
                </CodeBlock>
              </p>
            </>
          }
        />
        <StyledStep
          title="Unpause queues"
          description={
            <p>
              Now that the database flashing is complete, unpause the queues.
            </p>
          }
          nextButtonTitle="Unpause Queues"
          onNextButtonClick={async () => {
            const request = async () =>
              updateIngestQueuesState(stateCode, QueueState.RUNNING);
            const succeeded = await runAndCheckStatus(request);
            if (succeeded) {
              await incrementCurrentStep();
            }
          }}
        />
      </Steps>
    );

  const { loading, data } = useFetchedData<StateCodeInfo[]>(
    fetchIngestStateCodes
  );

  return (
    <>
      <PageHeader
        title="Flash Primary Database"
        extra={
          <StateSelector
            onChange={setStateCode}
            initialValue={null}
            loading={loading}
            data={data}
          />
        }
      />
      <Modal
        title="Confirm Role"
        visible={modalVisible}
        maskClosable={false}
        closable={false}
        keyboard={false}
        onOk={() => setModalVisible(false)}
        onCancel={() => history.push("/admin")}
      >
        If you are not a full-time Recidiviz engineer, please navigate away from
        this page. By clicking OK, you attest that you are a full-time engineer
        who should be accessing this page.
      </Modal>
      {contents}
    </>
  );
};

export default FlashDatabaseChecklist;
