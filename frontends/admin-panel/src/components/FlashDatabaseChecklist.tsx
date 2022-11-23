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
  changeIngestInstanceStatus,
  deleteDatabaseImportGCSFiles,
  exportDatabaseToGCS,
  fetchIngestStateCodes,
  importDatabaseFromGCS,
  markInstanceIngestViewDataInvalidated,
  moveIngestViewResultsBetweenInstances,
  moveIngestViewResultsToBackup,
  releaseBQExportLock,
  transferIngestViewMetadataToNewInstance,
  updateIngestQueuesState,
} from "../AdminPanelAPI";
import { deleteContentsInSecondaryIngestViewDataset } from "../AdminPanelAPI/IngestOperations";
import {
  DirectIngestInstance,
  QueueState,
  StateCodeInfo,
} from "./IngestOperationsView/constants";
import NewTabLink from "./NewTabLink";
import {
  fetchCurrentIngestInstanceStatus,
  fetchCurrentRawDataSourceInstance,
} from "./Utilities/IngestInstanceUtilities";
import StateSelector from "./Utilities/StateSelector";

interface StyledStepProps extends StepProps {
  // Title of button that actually performs an action. If not present,
  // only a 'Mark done' button will be present for a given step.
  actionButtonTitle?: string;
  // Action that will be performed when the action button is clicked.
  onActionButtonClick?: () => Promise<Response>;

  // Action that will be performed when the mark done button is clicked.
  onMarkDoneClick?: () => Promise<void>;

  // Whether the action button on a step should be enabled. The Mark Done button
  // is always enabled.
  actionButtonEnabled?: boolean;
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

const FlashChecklistStepSection = {
  /* Ordered list of sections in the flash checklist.
  NOTE: The relative order of these steps is important.
  IF YOU ADD A NEW STEP SECTION,
  you MUST add it in the relative order to other sections. */
  PAUSE_OPERATIONS: 1,
  START_FLASH: 2,
  PRIMARY_INGEST_VIEW_DEPRECATION: 3,
  FLASH_INGEST_VIEW_TO_PRIMARY: 4,
  SECONDARY_INGEST_VIEW_CLEANUP: 5,
  FINALIZE_FLASH: 6,
  RESUME_OPERATIONS: 7,
  TRIGGER_PIPELINES: 8,
  DONE: 9,
};

const CancelFlashChecklistStepSection = {
  /* Ordered list of sections in the flash cancellation checklist.
  NOTE: The relative order of these steps is important.
  IF YOU ADD A NEW STEP SECTION,
  you MUST add it in the relative order to other sections. */
  PAUSE_OPERATIONS: 1,
  START_CANCELLATION: 2,
  SECONDARY_INGEST_VIEW_CLEANUP: 3,
  FINALIZE_CANCELLATION: 4,
  RESUME_OPERATIONS: 5,
  DONE: 6,
};

interface ChecklistSectionHeaderProps {
  children: React.ReactNode;
  currentStepSection: number;
  stepSection: number;
}

const ChecklistSectionHeader = ({
  children,
  currentStepSection,
  stepSection,
}: ChecklistSectionHeaderProps): JSX.Element => (
  <h1>
    <b style={{ display: "inline-flex" }}>
      {currentStepSection > stepSection ? "COMPLETED-" : ""}
      {children}
    </b>
  </h1>
);

interface ChecklistSectionProps {
  children: React.ReactNode;
  headerContents: React.ReactNode;
  currentStep: number;
  currentStepSection: number;
  stepSection: number;
}

const ChecklistSection = ({
  children,
  headerContents,
  currentStep,
  currentStepSection,
  stepSection,
}: ChecklistSectionProps): JSX.Element => (
  <div
    style={{
      opacity: currentStepSection === stepSection ? 1 : 0.25,
      pointerEvents: currentStepSection === stepSection ? "initial" : "none",
    }}
  >
    <>
      <ChecklistSectionHeader
        currentStepSection={currentStepSection}
        stepSection={stepSection}
      >
        {headerContents}
      </ChecklistSectionHeader>
      <Steps
        progressDot
        current={currentStepSection === stepSection ? currentStep : 0}
        direction="vertical"
      >
        {children}
      </Steps>
    </>
  </div>
);

const FlashDatabaseChecklist = (): JSX.Element => {
  const isProduction = window.RUNTIME_GCP_ENVIRONMENT === "production";
  const projectId = isProduction ? "recidiviz-123" : "recidiviz-staging";

  const [currentStep, setCurrentStep] = React.useState(0);
  const [currentStepSection, setCurrentStepSection] = React.useState(0);
  const [stateInfo, setStateInfo] = React.useState<StateCodeInfo | null>(null);
  const [currentPrimaryIngestInstanceStatus, setPrimaryIngestInstanceStatus] =
    React.useState<string | null>(null);
  const [
    currentSecondaryIngestInstanceStatus,
    setSecondaryIngestInstanceStatus,
  ] = React.useState<string | null>(null);
  const [proceedWithFlash, setProceedWithFlash] =
    React.useState<boolean | null>(null);
  const [
    currentSecondaryRawDataSourceInstance,
    setSecondaryRawDataSourceInstance,
  ] = React.useState<DirectIngestInstance | null>(null);
  const [modalVisible, setModalVisible] = React.useState(true);
  const history = useHistory();

  const isFlashInProgress =
    currentPrimaryIngestInstanceStatus === "FLASH_IN_PROGRESS" &&
    currentSecondaryIngestInstanceStatus === "FLASH_IN_PROGRESS";
  const isFlashCancellationInProgress =
    currentSecondaryIngestInstanceStatus === "FLASH_CANCELLATION_IN_PROGRESS";
  const isFlashCanceled =
    currentSecondaryIngestInstanceStatus === "FLASH_CANCELED";
  const isReadyToFlash =
    currentSecondaryIngestInstanceStatus === "READY_TO_FLASH";
  const isFlashCompleted =
    currentSecondaryIngestInstanceStatus === "FLASH_COMPLETED";
  const isNoRerunInProgress =
    currentSecondaryIngestInstanceStatus === "NO_RERUN_IN_PROGRESS";

  const incrementCurrentStep = async () => setCurrentStep(currentStep + 1);

  const getData = React.useCallback(async () => {
    if (stateInfo) {
      const [primaryStatus, secondaryStatus, secondaryRawDataSourceInstance] =
        await Promise.all([
          fetchCurrentIngestInstanceStatus(
            stateInfo.code,
            DirectIngestInstance.PRIMARY
          ),
          fetchCurrentIngestInstanceStatus(
            stateInfo.code,
            DirectIngestInstance.SECONDARY
          ),
          fetchCurrentRawDataSourceInstance(
            stateInfo.code,
            DirectIngestInstance.SECONDARY
          ),
        ]);
      setPrimaryIngestInstanceStatus(primaryStatus);
      setSecondaryIngestInstanceStatus(secondaryStatus);
      setSecondaryRawDataSourceInstance(secondaryRawDataSourceInstance);
    }
  }, [stateInfo]);

  React.useEffect(() => {
    getData();
  }, [getData]);

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

  const setNewState = async (info: StateCodeInfo) => {
    setCurrentStep(0);
    setStateInfo(info);
    setProceedWithFlash(null);
    setSecondaryRawDataSourceInstance(null);
  };

  const moveToNextChecklistSection = async (newSection: number) => {
    setCurrentStepSection(newSection);
    // Reset current step once starting a new section.
    setCurrentStep(0);
  };

  const setStatusInPrimaryAndSecondaryTo = async (
    stateCode: string,
    status: string
  ): Promise<Response> => {
    const [primaryResponse, secondaryResponse] = await Promise.all([
      changeIngestInstanceStatus(
        stateCode,
        DirectIngestInstance.PRIMARY,
        status
      ),
      changeIngestInstanceStatus(
        stateCode,
        DirectIngestInstance.SECONDARY,
        status
      ),
    ]);
    return primaryResponse.status !== 200 ? primaryResponse : secondaryResponse;
  };

  const StyledStep = ({
    actionButtonTitle,
    onActionButtonClick,
    onMarkDoneClick,
    description,
    actionButtonEnabled,
    ...rest
  }: StyledStepProps): JSX.Element => {
    const [loading, setLoading] = React.useState(false);

    const jointDescription = (
      <>
        {description}
        {onActionButtonClick && (
          <Button
            type="primary"
            disabled={!actionButtonEnabled}
            onClick={async () => {
              setLoading(true);
              const succeeded = await runAndCheckStatus(onActionButtonClick);
              if (succeeded) {
                await getData();
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
          disabled={false}
          onClick={async () => {
            setLoading(true);
            await getData();
            await incrementCurrentStep();
            setLoading(false);
            if (onMarkDoneClick) {
              onMarkDoneClick();
            }
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

  const FlashDecisionComponent = ({
    onSelectProceed,
    onSelectCancel,
  }: {
    onSelectProceed: () => void;
    onSelectCancel: () => void;
  }): JSX.Element => {
    return (
      <div>
        The SECONDARY results are ready to be flashed to PRIMARY. Would you like
        to:
        <ul>
          <li>
            <b>Proceed with flash to PRIMARY.</b> Results in SECONDARY have been
            validated and can be copied to PRIMARY.
          </li>
          <li>
            <b>Cancel flash to PRIMARY.</b> Delete and clean up results in
            SECONDARY and do not copy over to PRIMARY.
          </li>
        </ul>
        <Button type="primary" onClick={onSelectProceed}>
          Proceed with Flash
        </Button>
        <Button onClick={onSelectCancel}>Cancel Flash</Button>
      </div>
    );
  };

  const StateCancelFlashChecklist = ({
    stateCode,
  }: StateFlashingChecklistProps): JSX.Element => {
    const secondaryIngestViewResultsDataset = `${stateCode.toLowerCase()}_ingest_view_results_secondary`;
    return (
      <div>
        <h1>Canceling Flash of Rerun Results from SECONDARY to PRIMARY </h1>
        <h3 style={{ color: "green" }}>
          Raw data source:{" "}
          {currentSecondaryRawDataSourceInstance === null
            ? "None"
            : currentSecondaryRawDataSourceInstance}
        </h3>
        <ChecklistSection
          currentStep={currentStep}
          currentStepSection={currentStepSection}
          stepSection={CancelFlashChecklistStepSection.PAUSE_OPERATIONS}
          headerContents="Pause Operations"
        >
          <StyledStep
            title="Pause Queues"
            description={
              <p>Pause all of the ingest-related queues for {stateCode}.</p>
            }
            actionButtonTitle="Pause Queues"
            actionButtonEnabled={proceedWithFlash === false}
            onActionButtonClick={async () =>
              updateIngestQueuesState(stateCode, QueueState.PAUSED)
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
            actionButtonEnabled={proceedWithFlash === false}
            actionButtonTitle="Acquire Lock"
            onActionButtonClick={async () =>
              acquireBQExportLock(stateCode, DirectIngestInstance.SECONDARY)
            }
            onMarkDoneClick={async () =>
              moveToNextChecklistSection(
                CancelFlashChecklistStepSection.START_CANCELLATION
              )
            }
          />
        </ChecklistSection>
        <ChecklistSection
          currentStep={currentStep}
          currentStepSection={currentStepSection}
          stepSection={CancelFlashChecklistStepSection.START_CANCELLATION}
          headerContents={<p>Start Flash Cancellation</p>}
        >
          <StyledStep
            title="Set status to FLASH_CANCELLATION_IN_PROGRESS"
            description={
              <p>
                Set ingest status to FLASH_CANCELLATION_IN_PROGRESS in SECONDARY
                in &nbsp;
                {stateCode}.
              </p>
            }
            actionButtonTitle="Update Ingest Instance Status"
            actionButtonEnabled={proceedWithFlash === false}
            onActionButtonClick={async () =>
              changeIngestInstanceStatus(
                stateCode,
                DirectIngestInstance.SECONDARY,
                "FLASH_CANCELLATION_IN_PROGRESS"
              )
            }
            onMarkDoneClick={async () =>
              moveToNextChecklistSection(
                CancelFlashChecklistStepSection.SECONDARY_INGEST_VIEW_CLEANUP
              )
            }
          />
        </ChecklistSection>
        <ChecklistSection
          currentStep={currentStep}
          currentStepSection={currentStepSection}
          stepSection={
            CancelFlashChecklistStepSection.SECONDARY_INGEST_VIEW_CLEANUP
          }
          headerContents={
            <p>
              Clean up Ingest View Data and Associated Metadata in{" "}
              <code>SECONDARY</code>
            </p>
          }
        >
          <StyledStep
            title="Clear secondary database"
            actionButtonEnabled={isFlashCancellationInProgress}
            description={
              <>
                <p>
                  Drop all data from the{" "}
                  <code>{stateCode.toLowerCase()}_secondary</code> database. To
                  do so, run this script locally inside a pipenv shell:
                </p>
                <p>
                  <CodeBlock
                    enabled={
                      currentStepSection ===
                      CancelFlashChecklistStepSection.SECONDARY_INGEST_VIEW_CLEANUP
                    }
                  >
                    python -m recidiviz.tools.migrations.purge_state_db \
                    <br />
                    {"    "}--state-code {stateCode} \<br />
                    {"    "}--ingest-instance SECONDARY \<br />
                    {"    "}--project-id {projectId}
                  </CodeBlock>
                </p>
              </>
            }
          />
          <StyledStep
            title="Deprecate secondary instance operation database rows"
            description={
              <p>
                Mark all <code>SECONDARY</code> instance rows in the{" "}
                <code>direct_ingest_view_materialization_metadata</code>{" "}
                operations database table as invalidated.
              </p>
            }
            actionButtonEnabled={isFlashCancellationInProgress}
            actionButtonTitle="Invalidate secondary rows"
            onActionButtonClick={async () =>
              markInstanceIngestViewDataInvalidated(
                stateCode,
                DirectIngestInstance.SECONDARY
              )
            }
          />
          <StyledStep
            title="Clean up SECONDARY ingest view results"
            description={
              <p>
                Delete the contents of the{" "}
                <code>{secondaryIngestViewResultsDataset}</code> dataset.
              </p>
            }
            actionButtonEnabled={isFlashCancellationInProgress}
            actionButtonTitle="Clean up SECONDARY ingest view results"
            onActionButtonClick={async () =>
              deleteContentsInSecondaryIngestViewDataset(stateCode)
            }
            onMarkDoneClick={async () =>
              moveToNextChecklistSection(
                CancelFlashChecklistStepSection.FINALIZE_CANCELLATION
              )
            }
          />
        </ChecklistSection>
        <ChecklistSection
          currentStep={currentStep}
          currentStepSection={currentStepSection}
          stepSection={CancelFlashChecklistStepSection.FINALIZE_CANCELLATION}
          headerContents={<p>Finalize Flash Cancellation</p>}
        >
          <StyledStep
            title="Set SECONDARY status to FLASH_CANCELED"
            description={
              <p>
                Set ingest status to FLASH_CANCELED in SECONDARY in &nbsp;
                {stateCode}.
              </p>
            }
            actionButtonEnabled={isFlashCancellationInProgress}
            actionButtonTitle="Update Ingest Instance Status"
            onActionButtonClick={async () =>
              changeIngestInstanceStatus(
                stateCode,
                DirectIngestInstance.SECONDARY,
                "FLASH_CANCELED"
              )
            }
          />
          <StyledStep
            title="Set status to NO_RERUN_IN_PROGRESS"
            description={
              <p>
                Set ingest status to NO_RERUN_IN_PROGRESS in SECONDARY in &nbsp;
                {stateCode}.
              </p>
            }
            actionButtonEnabled={isFlashCanceled}
            actionButtonTitle="Update Ingest Instance Status"
            onActionButtonClick={async () =>
              changeIngestInstanceStatus(
                stateCode,
                DirectIngestInstance.SECONDARY,
                "NO_RERUN_IN_PROGRESS"
              )
            }
            onMarkDoneClick={async () =>
              moveToNextChecklistSection(
                CancelFlashChecklistStepSection.RESUME_OPERATIONS
              )
            }
          />
        </ChecklistSection>
        <ChecklistSection
          currentStep={currentStep}
          currentStepSection={currentStepSection}
          stepSection={CancelFlashChecklistStepSection.RESUME_OPERATIONS}
          headerContents={<p>Resume Operations</p>}
        >
          <StyledStep
            title="Release SECONDARY Ingest Lock"
            description={
              <p>
                Release the ingest lock for {stateCode}&#39;s secondary
                instance.
              </p>
            }
            actionButtonEnabled={isNoRerunInProgress}
            actionButtonTitle="Release Lock"
            onActionButtonClick={async () =>
              releaseBQExportLock(stateCode, DirectIngestInstance.SECONDARY)
            }
          />
          <StyledStep
            title="Unpause queues"
            description={
              <p>
                Now that the database flashing is complete, unpause the queues.
              </p>
            }
            actionButtonTitle="Unpause Queues"
            actionButtonEnabled={isNoRerunInProgress}
            onActionButtonClick={async () =>
              updateIngestQueuesState(stateCode, QueueState.RUNNING)
            }
            onMarkDoneClick={async () =>
              moveToNextChecklistSection(CancelFlashChecklistStepSection.DONE)
            }
          />
        </ChecklistSection>
        <ChecklistSection
          currentStep={currentStep}
          currentStepSection={currentStepSection}
          stepSection={CancelFlashChecklistStepSection.DONE}
          headerContents={
            <p style={{ color: "green" }}>Flash cancellation is complete!</p>
          }
        >
          <p>DONE</p>
        </ChecklistSection>
      </div>
    );
  };

  const StateProceedWithFlashChecklist = ({
    stateCode,
  }: StateFlashingChecklistProps): JSX.Element => {
    const secondaryIngestViewResultsDataset = `${stateCode.toLowerCase()}_ingest_view_results_secondary`;
    const primaryIngestViewResultsDataset = `${stateCode.toLowerCase()}_ingest_view_results_primary`;
    const operationsPageURL = `https://go/${
      isProduction ? "prod" : "dev"
    }-state-data-operations`;

    return (
      <div>
        <h1>
          Proceeding with Flash of Rerun Results from SECONDARY to PRIMARY
        </h1>
        <h3 style={{ color: "green" }}>
          Raw data source:{" "}
          {currentSecondaryRawDataSourceInstance === null
            ? "None"
            : currentSecondaryRawDataSourceInstance}
        </h3>
        <br />
        <ChecklistSection
          currentStep={currentStep}
          currentStepSection={currentStepSection}
          stepSection={FlashChecklistStepSection.PAUSE_OPERATIONS}
          headerContents={<p>Pause Operations</p>}
        >
          <StyledStep
            title="Pause Queues"
            description={
              <p>Pause all of the ingest-related queues for {stateCode}.</p>
            }
            actionButtonTitle="Pause Queues"
            actionButtonEnabled={isReadyToFlash}
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
            actionButtonEnabled={isReadyToFlash}
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
            actionButtonEnabled={isReadyToFlash}
            actionButtonTitle="Acquire Lock"
            onActionButtonClick={async () =>
              acquireBQExportLock(stateCode, DirectIngestInstance.SECONDARY)
            }
            onMarkDoneClick={async () =>
              moveToNextChecklistSection(FlashChecklistStepSection.START_FLASH)
            }
          />
        </ChecklistSection>
        <ChecklistSection
          currentStep={currentStep}
          currentStepSection={currentStepSection}
          stepSection={FlashChecklistStepSection.START_FLASH}
          headerContents={<p>Start Flash</p>}
        >
          <StyledStep
            title="Set status to FLASH_IN_PROGRESS"
            description={
              <p>
                Set ingest status to FLASH_IN_PROGRESS in PRIMARY and SECONDARY
                in &nbsp;
                {stateCode}.
              </p>
            }
            actionButtonTitle="Update Ingest Instance Status"
            actionButtonEnabled={isReadyToFlash}
            onActionButtonClick={async () =>
              setStatusInPrimaryAndSecondaryTo(stateCode, "FLASH_IN_PROGRESS")
            }
            onMarkDoneClick={async () =>
              moveToNextChecklistSection(
                FlashChecklistStepSection.PRIMARY_INGEST_VIEW_DEPRECATION
              )
            }
          />
        </ChecklistSection>
        <ChecklistSection
          currentStep={currentStep}
          currentStepSection={currentStepSection}
          stepSection={
            FlashChecklistStepSection.PRIMARY_INGEST_VIEW_DEPRECATION
          }
          headerContents={
            <p>
              Deprecate Ingest Views and Associated Metadata in{" "}
              <code>PRIMARY</code>
            </p>
          }
        >
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
                  <CodeBlock
                    enabled={
                      currentStepSection ===
                      FlashChecklistStepSection.PRIMARY_INGEST_VIEW_DEPRECATION
                    }
                  >
                    python -m recidiviz.tools.migrations.purge_state_db \
                    <br />
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
            title="Backup primary ingest view results"
            description={
              <>
                <p>
                  Move all primary instance ingest view results to a backup
                  dataset in BQ.
                </p>
              </>
            }
            actionButtonEnabled={isFlashInProgress}
            actionButtonTitle="Move to Backup"
            onActionButtonClick={async () =>
              moveIngestViewResultsToBackup(
                stateCode,
                DirectIngestInstance.PRIMARY
              )
            }
          />
          <StyledStep
            title="Deprecate primary instance operation database rows"
            description={
              <p>
                Mark all <code>PRIMARY</code> instance rows in the{" "}
                <code>direct_ingest_view_materialization_metadata</code>{" "}
                operations database table as invalidated.
              </p>
            }
            actionButtonEnabled={isFlashInProgress}
            actionButtonTitle="Invalidate primary rows"
            onActionButtonClick={async () =>
              markInstanceIngestViewDataInvalidated(
                stateCode,
                DirectIngestInstance.PRIMARY
              )
            }
            onMarkDoneClick={async () =>
              moveToNextChecklistSection(
                FlashChecklistStepSection.FLASH_INGEST_VIEW_TO_PRIMARY
              )
            }
          />
        </ChecklistSection>
        <ChecklistSection
          currentStep={currentStep}
          currentStepSection={currentStepSection}
          stepSection={FlashChecklistStepSection.FLASH_INGEST_VIEW_TO_PRIMARY}
          headerContents={
            <p>
              Flash Ingest View Results to <code>PRIMARY</code>
            </p>
          }
        >
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
                of the STATE SQL instance page. If this request times out, but
                the operation succeeds, just select &#39;Mark Done&#39;.
              </p>
            }
            actionButtonEnabled={isFlashInProgress}
            actionButtonTitle="Export Data"
            onActionButtonClick={async () =>
              exportDatabaseToGCS(stateCode, DirectIngestInstance.SECONDARY)
            }
          />
          <StyledStep
            title="Import data from secondary"
            description={
              <p>
                Import the SQL dump from the{" "}
                <code>{stateCode.toLowerCase()}_secondary</code> Postgres
                database into the <code>{stateCode.toLowerCase()}_primary</code>{" "}
                Postgres database. You can check your progress in the{" "}
                <NewTabLink href={operationsPageURL}>
                  Operations section
                </NewTabLink>{" "}
                of the STATE SQL instance page. If this request times out, but
                the operation succeeds, just select &#39;Mark Done&#39;.
              </p>
            }
            actionButtonEnabled={isFlashInProgress}
            actionButtonTitle="Import Data"
            onActionButtonClick={async () =>
              importDatabaseFromGCS(
                stateCode,
                DirectIngestInstance.PRIMARY,
                DirectIngestInstance.SECONDARY
              )
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
            actionButtonEnabled={isFlashInProgress}
            actionButtonTitle="Delete"
            onActionButtonClick={async () =>
              deleteDatabaseImportGCSFiles(
                stateCode,
                DirectIngestInstance.SECONDARY
              )
            }
          />
          <StyledStep
            title="Move ingest view metadata from SECONDARY instance to PRIMARY"
            description={
              <p>
                Update all rows in the{" "}
                <code>direct_ingest_view_materialization_metadata</code>{" "}
                operations database that had instance <code>SECONDARY</code>{" "}
                with updated instance <code>PRIMARY</code>.
              </p>
            }
            actionButtonEnabled={isFlashInProgress}
            actionButtonTitle="Move Secondary Metadata"
            onActionButtonClick={async () =>
              transferIngestViewMetadataToNewInstance(
                stateCode,
                DirectIngestInstance.SECONDARY,
                DirectIngestInstance.PRIMARY
              )
            }
          />
          <StyledStep
            title="Move secondary ingest view data to primary"
            description={
              <p>
                Move all ingest view results from BQ dataset{" "}
                <code>{secondaryIngestViewResultsDataset}</code> to BQ dataset{" "}
                <code>{primaryIngestViewResultsDataset}</code>
              </p>
            }
            actionButtonEnabled={isFlashInProgress}
            actionButtonTitle="Move Secondary Data"
            onActionButtonClick={async () =>
              moveIngestViewResultsBetweenInstances(
                stateCode,
                DirectIngestInstance.SECONDARY,
                DirectIngestInstance.PRIMARY
              )
            }
            onMarkDoneClick={async () =>
              moveToNextChecklistSection(
                FlashChecklistStepSection.SECONDARY_INGEST_VIEW_CLEANUP
              )
            }
          />
        </ChecklistSection>
        <ChecklistSection
          currentStep={currentStep}
          currentStepSection={currentStepSection}
          stepSection={FlashChecklistStepSection.SECONDARY_INGEST_VIEW_CLEANUP}
          headerContents={
            <p>
              Clean up Ingest View Data and Associated Metadata in{" "}
              <code>SECONDARY</code>
            </p>
          }
        >
          <StyledStep
            title="Clear secondary database"
            description={
              <>
                <p>
                  Drop all data from the{" "}
                  <code>{stateCode.toLowerCase()}_secondary</code> database. To
                  do so, run this script locally inside a pipenv shell:
                </p>
                <p>
                  <CodeBlock
                    enabled={
                      currentStepSection ===
                      FlashChecklistStepSection.SECONDARY_INGEST_VIEW_CLEANUP
                    }
                  >
                    python -m recidiviz.tools.migrations.purge_state_db \
                    <br />
                    {"    "}--state-code {stateCode} \<br />
                    {"    "}--ingest-instance SECONDARY \<br />
                    {"    "}--project-id {projectId}
                  </CodeBlock>
                </p>
              </>
            }
            onMarkDoneClick={async () =>
              moveToNextChecklistSection(
                FlashChecklistStepSection.FINALIZE_FLASH
              )
            }
          />
        </ChecklistSection>
        <ChecklistSection
          currentStep={currentStep}
          currentStepSection={currentStepSection}
          stepSection={FlashChecklistStepSection.FINALIZE_FLASH}
          headerContents={<p>Finalize Flash</p>}
        >
          <StyledStep
            title="Set status to FLASH_COMPLETED"
            description={
              <p>
                Set ingest status to FLASH_COMPLETED in PRIMARY and SECONDARY in
                &nbsp;
                {stateCode}.
              </p>
            }
            actionButtonEnabled={isFlashInProgress}
            actionButtonTitle="Update Ingest Instance Status"
            onActionButtonClick={async () =>
              setStatusInPrimaryAndSecondaryTo(stateCode, "FLASH_COMPLETED")
            }
          />
          <StyledStep
            title="Set status to NO_RERUN_IN_PROGRESS"
            description={
              <p>
                Set ingest status to NO_RERUN_IN_PROGRESS in SECONDARY in &nbsp;
                {stateCode}.
              </p>
            }
            actionButtonEnabled={isFlashCompleted}
            actionButtonTitle="Update Ingest Instance Status"
            onActionButtonClick={async () =>
              changeIngestInstanceStatus(
                stateCode,
                DirectIngestInstance.SECONDARY,
                "NO_RERUN_IN_PROGRESS"
              )
            }
            onMarkDoneClick={async () =>
              moveToNextChecklistSection(
                FlashChecklistStepSection.RESUME_OPERATIONS
              )
            }
          />
        </ChecklistSection>
        <ChecklistSection
          currentStep={currentStep}
          currentStepSection={currentStepSection}
          stepSection={FlashChecklistStepSection.RESUME_OPERATIONS}
          headerContents={<p>Resume Operations</p>}
        >
          <StyledStep
            title="Release PRIMARY Ingest Lock"
            description={
              <p>
                Release the ingest lock for {stateCode}&#39;s primary instance.
              </p>
            }
            actionButtonEnabled={isFlashInProgress}
            actionButtonTitle="Release Lock"
            onActionButtonClick={async () =>
              releaseBQExportLock(stateCode, DirectIngestInstance.PRIMARY)
            }
          />
          <StyledStep
            title="Release SECONDARY Ingest Lock"
            description={
              <p>
                Release the ingest lock for {stateCode}&#39;s secondary
                instance.
              </p>
            }
            actionButtonEnabled={isFlashInProgress}
            actionButtonTitle="Release Lock"
            onActionButtonClick={async () =>
              releaseBQExportLock(stateCode, DirectIngestInstance.SECONDARY)
            }
          />
          <StyledStep
            title="Unpause queues"
            description={
              <p>
                Now that the database flashing is complete, unpause the queues.
              </p>
            }
            actionButtonTitle="Unpause Queues"
            actionButtonEnabled={isNoRerunInProgress}
            onActionButtonClick={async () =>
              updateIngestQueuesState(stateCode, QueueState.RUNNING)
            }
            onMarkDoneClick={async () =>
              moveToNextChecklistSection(
                FlashChecklistStepSection.TRIGGER_PIPELINES
              )
            }
          />
        </ChecklistSection>
        <ChecklistSection
          currentStep={currentStep}
          currentStepSection={currentStepSection}
          stepSection={FlashChecklistStepSection.TRIGGER_PIPELINES}
          headerContents={<p>Trigger Pipelines</p>}
        >
          <StyledStep
            title="Full Historical Refresh"
            description={
              <>
                <p>
                  Trigger a BigQuery refresh and run the historical DAG by
                  running this script locally inside a pipenv shell:
                </p>
                <p>
                  <CodeBlock
                    enabled={
                      currentStepSection ===
                      FlashChecklistStepSection.TRIGGER_PIPELINES
                    }
                  >
                    python -m recidiviz.tools.deploy.trigger_post_deploy_tasks
                    --project-id {projectId}
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
                  and wait for the historical DAG to finish before continuing.
                  Note that the historical DAG may not start for ~10 minutes
                  while the BigQuery refresh is still in progress.
                </p>
              </>
            }
          />
          {/* TODO(#9010): This step won't be necessary once the historical and incremental DAG have a more unified structure */}
          <StyledStep
            title="Trigger Incremental Pipelines"
            description={
              <p>
                Run the incremental DAG by visiting{" "}
                <a
                  href={`http://go/airflow-${
                    isProduction ? "prod" : "staging"
                  }`}
                >
                  go/airflow-{isProduction ? "prod" : "staging"}
                </a>
                , and clicking on the &quot;Trigger DAG&quot; button for{" "}
                <code>{projectId}_incremental_pipeline_calculations_dag</code>.
                It looks like a play button and should be the left-most button
                under the &quot;Links&quot; section.
              </p>
            }
            onMarkDoneClick={async () =>
              moveToNextChecklistSection(FlashChecklistStepSection.DONE)
            }
          />
        </ChecklistSection>
        <ChecklistSection
          currentStep={currentStep}
          currentStepSection={currentStepSection}
          stepSection={FlashChecklistStepSection.DONE}
          headerContents={<p style={{ color: "green" }}>Flash is complete!</p>}
        >
          <p>DONE</p>
        </ChecklistSection>
      </div>
    );
  };

  let activeComponent;
  if (stateInfo === null) {
    activeComponent = (
      <Alert
        message="Select a state"
        description="Once you pick a state, this form will display the set of instructions required to flash a secondary database to primary."
        type="info"
        showIcon
      />
    );
  } else if (currentPrimaryIngestInstanceStatus === undefined) {
    activeComponent = <Spin />;
  } else if (
    !isReadyToFlash &&
    !isFlashInProgress &&
    !isFlashCancellationInProgress &&
    proceedWithFlash === null &&
    // This check makes it so we don't show the "can't flash" component
    // when you set the status to FLASH_COMPLETE in the middle of the checklist.
    currentStep === 0
  ) {
    /* If we have loaded a status but it does not indicate that we can proceed with flashing, show an alert on top of the checklist */
    /* Regardless of status, we can cancel a flash to primary at any time */
    const cannotFlashDescription = `Primary: ${currentPrimaryIngestInstanceStatus}. Secondary: ${currentSecondaryIngestInstanceStatus}.`;
    activeComponent = (
      <div>
        <Alert
          message="Cannot proceed with flash to primary. Secondary instance needs to have the status 'READY_TO_FLASH'."
          description={cannotFlashDescription}
          type="info"
          showIcon
        />
        <br />
        {currentSecondaryRawDataSourceInstance === null ? (
          <h3>
            <b style={{ color: "red" }}>
              Could not locate the raw data source instance of the rerun in
              secondary.
            </b>{" "}
            Are you sure there is a rerun in progress in secondary?
          </h3>
        ) : (
          <h3 style={{ color: "green" }}>
            Regardless of ingest instance status, you may proceed with cleaning
            up the secondary instance and canceling the flash:{" "}
            <Button
              type="primary"
              onClick={async () => {
                setProceedWithFlash(false);
                moveToNextChecklistSection(
                  CancelFlashChecklistStepSection.PAUSE_OPERATIONS
                );
              }}
            >
              CLEAN UP SECONDARY + CANCEL FLASH
            </Button>
          </h3>
        )}
      </div>
    );
  } else if (proceedWithFlash === null && isReadyToFlash) {
    activeComponent = (
      /* This is the only time that someone can choose whether to cancel or
      move forward with a flash. */
      <FlashDecisionComponent
        onSelectProceed={async () => {
          setProceedWithFlash(true);
          moveToNextChecklistSection(
            FlashChecklistStepSection.PAUSE_OPERATIONS
          );
        }}
        onSelectCancel={async () => {
          setProceedWithFlash(false);
          moveToNextChecklistSection(
            CancelFlashChecklistStepSection.PAUSE_OPERATIONS
          );
        }}
      />
    );
  } else if (proceedWithFlash || isFlashInProgress) {
    activeComponent = (
      /* If decision has been made to cancel a flash from SECONDARY to PRIMARY */
      <StateProceedWithFlashChecklist stateCode={stateInfo.code} />
    );
  } else if (!proceedWithFlash || isFlashCancellationInProgress) {
    activeComponent = (
      /* This covers when a decision has been made to
      proceed with a flash from SECONDARY to PRIMARY */
      <StateCancelFlashChecklist stateCode={stateInfo.code} />
    );
  }

  return (
    <>
      <PageHeader
        title="Flash Primary Database"
        extra={
          <StateSelector
            fetchStateList={fetchIngestStateCodes}
            onChange={(state) => {
              setNewState(state);
            }}
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
      {activeComponent}
    </>
  );
};

export default FlashDatabaseChecklist;
