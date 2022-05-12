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
  Spin,
  StepProps,
  Steps,
} from "antd";
import * as React from "react";
import { useHistory } from "react-router-dom";
import {
  acquireBQExportLock,
  deleteDatabaseImportGCSFiles,
  exportDatabaseToGCS,
  fetchIngestStateCodes,
  getInstanceMaterializationBQBool,
  importDatabaseFromGCS,
  markInstanceIngestViewDataInvalidated,
  moveIngestViewResultsBetweenInstances,
  moveIngestViewResultsToBackup,
  pauseDirectIngestInstance,
  releaseBQExportLock,
  transferIngestViewMetadataToNewInstance,
  ungateMaterializationInstance,
  updateIngestQueuesState,
} from "../AdminPanelAPI";
import {
  DirectIngestInstance,
  QueueState,
  StateCodeInfo,
} from "./IngestOperationsView/constants";
import NewTabLink from "./NewTabLink";
import StateSelector from "./Utilities/StateSelector";

interface StyledStepProps extends StepProps {
  // Title of button that actually performs an action. If not present,
  // only a 'Mark done' button will be present for a given step.
  actionButtonTitle?: string;
  // Action that will be performed when the action button is clicked.
  onActionButtonClick?: () => Promise<Response>;
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

  const [currentStep, setCurrentStep] = React.useState(0);
  const [stateInfo, setStateInfo] = React.useState<StateCodeInfo | null>(null);
  const [modalVisible, setModalVisible] = React.useState(true);
  const [
    isPrimaryInstanceBQMaterializationEnabled,
    setPrimaryIsBQMaterializationEnabled,
  ] = React.useState(false);
  const [
    isSecondaryInstanceBQMaterializationEnabled,
    setSecondaryIsBQMaterializationEnabled,
  ] = React.useState(false);
  // TODO(#11424): Delete loading logic and code related to BQ materialization flags
  // once BQ materialization has shipped to all states.
  const [isLoading, setIsLoading] = React.useState(true);

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

  const getInstanceBQMaterializationFlags = async (stateCode: string) => {
    const response = await getInstanceMaterializationBQBool(stateCode);
    const json = await response.json();
    setPrimaryIsBQMaterializationEnabled(json.primary);
    setSecondaryIsBQMaterializationEnabled(json.secondary);
    setIsLoading(false);
  };

  const setNewState = async (info: StateCodeInfo) => {
    setCurrentStep(0);
    setIsLoading(true);
    setStateInfo(info);
    getInstanceBQMaterializationFlags(info.code);
  };

  const StyledStep = ({
    actionButtonTitle,
    onActionButtonClick,
    description,
    ...rest
  }: StyledStepProps): JSX.Element => {
    const [loading, setLoading] = React.useState(false);

    const jointDescription = (
      <>
        {description}
        {onActionButtonClick && (
          <Button
            type="primary"
            onClick={async () => {
              setLoading(true);
              const succeeded = await runAndCheckStatus(onActionButtonClick);
              if (succeeded) {
                await incrementCurrentStep();
              }
              setLoading(false);
            }}
            loading={loading}
            style={
              rest.status === "process"
                ? { marginRight: 5 }
                : { display: "none" }
            }
          >
            {actionButtonTitle}
          </Button>
        )}
        <Button
          type={onActionButtonClick ? undefined : "primary"}
          onClick={async () => {
            setLoading(true);
            await incrementCurrentStep();
            setLoading(false);
          }}
          loading={loading}
          style={rest.status === "process" ? undefined : { display: "none" }}
        >
          Mark Done
        </Button>
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

  interface StateFlashingChecklistProps {
    stateCode: string;
  }

  const StateFlashingChecklist = ({
    stateCode,
  }: StateFlashingChecklistProps): JSX.Element => {
    const secondaryIngestViewResultsDataset = `${stateCode.toLowerCase()}_ingest_view_results_secondary`;
    const primaryIngestViewResultsDataset = `${stateCode.toLowerCase()}_ingest_view_results_primary`;
    const operationsPageURL = `https://go/${
      isProduction ? "prod" : "dev"
    }-state-data-operations`;

    return isLoading ? (
      <Spin />
    ) : (
      <Steps progressDot current={currentStep} direction="vertical">
        <StyledStep
          title="Pause Queues"
          description={
            <p>Pause all of the ingest-related queues for {stateCode}.</p>
          }
          actionButtonTitle="Pause Queues"
          onActionButtonClick={async () =>
            updateIngestQueuesState(stateCode, QueueState.PAUSED)
          }
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
          actionButtonTitle="Acquire Lock"
          onActionButtonClick={async () =>
            acquireBQExportLock(stateCode, DirectIngestInstance.PRIMARY)
          }
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
          actionButtonTitle="Acquire Lock"
          onActionButtonClick={async () =>
            acquireBQExportLock(stateCode, DirectIngestInstance.SECONDARY)
          }
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
          actionButtonTitle="Export Data"
          onActionButtonClick={async () =>
            exportDatabaseToGCS(stateCode, DirectIngestInstance.SECONDARY)
          }
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
        {isPrimaryInstanceBQMaterializationEnabled ? (
          <StyledStep
            title="Backup primary ingest view results"
            description={
              <>
                <p>
                  Move all primary instance ingest view results to a backup
                  dataset in BQ.
                </p>
              </>
            }
            actionButtonTitle="Move to Backup"
            onActionButtonClick={async () =>
              moveIngestViewResultsToBackup(
                stateCode,
                DirectIngestInstance.PRIMARY
              )
            }
          />
        ) : (
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
              </>
            }
          />
        )}

        {isPrimaryInstanceBQMaterializationEnabled ? (
          <StyledStep
            title="Deprecate primary instance operation database rows"
            description={
              <p>
                Mark all <code>PRIMARY</code> instance rows in the{" "}
                <code>direct_ingest_view_materialization_metadata</code>{" "}
                operations database table as invalidated.
              </p>
            }
            actionButtonTitle="Invalidate primary rows"
            onActionButtonClick={async () =>
              markInstanceIngestViewDataInvalidated(
                stateCode,
                DirectIngestInstance.PRIMARY
              )
            }
          />
        ) : (
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
                    Run{" "}
                    <code>
                      ./recidiviz/tools/postgres/access_cloudsql_instance.sh
                    </code>{" "}
                    in pipenv shell. Select <code>{projectId}</code>,{" "}
                    <code>operations_v2</code>, then{" "}
                    <code>operations_v2_db_user</code>.
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
        )}

        <StyledStep
          title="Import data from secondary"
          description={
            <p>
              Update all rows in operations database that had database{" "}
              <code>{stateCode.toLowerCase()}_secondary</code> with updated
              database name <code>{stateCode.toLowerCase()}_primary</code>.
              <br />
              You can check your progress in the{" "}
              <NewTabLink href={operationsPageURL}>
                Operations section
              </NewTabLink>{" "}
              of the STATE SQL instance page. If this request times out, but the
              operation succeeds, just select &#39;Mark Done&#39;.
            </p>
          }
          actionButtonTitle="Import Data"
          onActionButtonClick={async () =>
            importDatabaseFromGCS(
              stateCode,
              DirectIngestInstance.PRIMARY,
              DirectIngestInstance.SECONDARY
            )
          }
        />

        {isSecondaryInstanceBQMaterializationEnabled ? (
          <StyledStep
            title="Move secondary ingest view metadata to primary"
            description={
              <p>
                Update all rows in the{" "}
                <code>direct_ingest_view_materialization_metadata</code>{" "}
                operations database that had instance <code>PRIMARY</code> with
                updated instance <code>SECONDARY</code>.
              </p>
            }
            actionButtonTitle="Move Secondary Metadata"
            onActionButtonClick={async () =>
              transferIngestViewMetadataToNewInstance(
                stateCode,
                DirectIngestInstance.SECONDARY,
                DirectIngestInstance.PRIMARY
              )
            }
          />
        ) : (
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
                    Run{" "}
                    <code>
                      ./recidiviz/tools/postgres/access_cloudsql_instance.sh
                    </code>{" "}
                    in pipenv shell . Select <code>{projectId}</code>,{" "}
                    <code>operations_v2</code>, then{" "}
                    <code>operations_v2_db_user</code>.
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
        )}

        {isSecondaryInstanceBQMaterializationEnabled ? (
          <StyledStep
            title="Move secondary ingest view data to primary"
            description={
              <p>
                Move all ingest view results from BQ dataset{" "}
                <code>{secondaryIngestViewResultsDataset}</code> to BQ dataset{" "}
                <code>{primaryIngestViewResultsDataset}</code>
              </p>
            }
            actionButtonTitle="Move Secondary Data"
            onActionButtonClick={async () =>
              moveIngestViewResultsBetweenInstances(
                stateCode,
                DirectIngestInstance.SECONDARY,
                DirectIngestInstance.PRIMARY
              )
            }
          />
        ) : (
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
        )}

        <StyledStep
          title="Release PRIMARY Ingest Lock"
          description={
            <p>
              Release the ingest lock for {stateCode}&#39;s primary instance.
            </p>
          }
          actionButtonTitle="Release Lock"
          onActionButtonClick={async () =>
            releaseBQExportLock(stateCode, DirectIngestInstance.PRIMARY)
          }
        />
        <StyledStep
          title="Release SECONDARY Ingest Lock"
          description={
            <p>
              Release the ingest lock for {stateCode}&#39;s secondary instance.
            </p>
          }
          actionButtonTitle="Release Lock"
          onActionButtonClick={async () =>
            releaseBQExportLock(stateCode, DirectIngestInstance.SECONDARY)
          }
        />
        <StyledStep
          title="Pause secondary ingest"
          description={
            <p>Mark secondary ingest as paused in the operations db.</p>
          }
          actionButtonTitle="Mark Paused"
          onActionButtonClick={async () =>
            pauseDirectIngestInstance(stateCode, DirectIngestInstance.SECONDARY)
          }
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
                  {"    "}--project-id {projectId}
                </CodeBlock>
              </p>
            </>
          }
        />
        <StyledStep
          title="Clean up imported SQL files"
          description={
            <p>
              Delete files containing the SQL that was imported into the{" "}
              <code>{stateCode.toLowerCase()}_primary</code> database.
            </p>
          }
          actionButtonTitle="Delete"
          onActionButtonClick={async () =>
            deleteDatabaseImportGCSFiles(
              stateCode,
              DirectIngestInstance.SECONDARY
            )
          }
        />

        {!isPrimaryInstanceBQMaterializationEnabled &&
        isSecondaryInstanceBQMaterializationEnabled ? (
          <StyledStep
            title="Ungate BQ materialization in primary"
            description={
              <p>
                Primary materialization type is <code>FILE</code> but Secondary
                materialization type is <code>BQ</code>, make Primary{" "}
                <code>BQ</code>.
              </p>
            }
            actionButtonTitle="Ungate Primary"
            onActionButtonClick={async () =>
              ungateMaterializationInstance(
                stateCode,
                DirectIngestInstance.PRIMARY
              )
            }
          />
        ) : null}

        <StyledStep
          title="Unpause queues"
          description={
            <p>
              Now that the database flashing is complete, unpause the queues.
            </p>
          }
          actionButtonTitle="Unpause Queues"
          onActionButtonClick={async () =>
            updateIngestQueuesState(stateCode, QueueState.RUNNING)
          }
        />
        <StyledStep
          title="Full Historical Refresh"
          description={
            <>
              <p>
                Trigger a BigQuery refresh and run all historical pipelines by
                running this script locally inside a pipenv shell:
              </p>
              <p>
                <CodeBlock enabled={currentStep === 16}>
                  python -m recidiviz.tools.deploy.trigger_post_deploy_tasks
                  --project-id {projectId} --trigger-historical-dag 1
                </CodeBlock>
              </p>
              <p>
                Visit{" "}
                <a
                  href={`http://go/airflow-${
                    isProduction ? "prod" : "staging"
                  }`}
                >
                  go/airflow-{isProduction ? "prod" : "staging"}
                </a>{" "}
                and wait for the historical pipelines to finish before
                continuing. Note that the historical pipelines may not start for
                ~10 minutes while the BigQuery refresh is still in progress.
              </p>
            </>
          }
        />
        {/* TODO(#9010): This step won't be necessary once the historical and incremental DAG have a more unified structure */}
        <StyledStep
          title="Trigger Incremental Pipelines"
          description={
            <p>
              Run all incremental pipelines by visiting{" "}
              <a
                href={`http://go/airflow-${isProduction ? "prod" : "staging"}`}
              >
                go/airflow-{isProduction ? "prod" : "staging"}
              </a>
              , and clicking on the &quot;Trigger DAG&quot; button for{" "}
              <code>{projectId}_incremental_pipeline_calculations_dag</code>. It
              looks like a play button and should be the left-most button under
              the &quot;Links&quot; section.
            </p>
          }
        />
      </Steps>
    );
  };

  const contents =
    stateInfo === null ? (
      <Alert
        message="Select a state"
        description="Once you pick a state, this form will display the set of instructions required to flash a secondary database to primary."
        type="info"
        showIcon
      />
    ) : (
      <StateFlashingChecklist stateCode={stateInfo.code} />
    );

  return (
    <>
      <PageHeader
        title="Flash Primary Database"
        extra={
          <StateSelector
            fetchStateList={fetchIngestStateCodes}
            onChange={setNewState}
            initialValue={null}
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
