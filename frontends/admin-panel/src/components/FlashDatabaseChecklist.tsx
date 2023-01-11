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
import {
  deleteContentsOfRawDataTables,
  deleteContentsInSecondaryIngestViewDataset,
  markInstanceRawDataInvalidated,
  copyRawDataBetweenInstances,
  copyRawDataToBackup,
  transferRawDataMetadataToNewInstance,
  getIngestRawFileProcessingStatus,
} from "../AdminPanelAPI/IngestOperations";
import {
  DirectIngestInstance,
  IngestRawFileProcessingStatus,
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

  // Section to move to if this step succeeds.
  nextSection?: number;

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
  /* Only present when rerun raw data source instance is SECONDARY */
  PRIMARY_RAW_DATA_DEPRECATION: 3,
  PRIMARY_INGEST_VIEW_DEPRECATION: 4,
  /* Only present when rerun raw data source instance is SECONDARY */
  FLASH_RAW_DATA_TO_PRIMARY: 5,
  FLASH_INGEST_VIEW_TO_PRIMARY: 6,
  /* Only present when rerun raw data source instance is SECONDARY */
  SECONDARY_RAW_DATA_CLEANUP: 7,
  SECONDARY_INGEST_VIEW_CLEANUP: 8,
  FINALIZE_FLASH: 9,
  RESUME_OPERATIONS: 10,
  TRIGGER_PIPELINES: 11,
  DONE: 12,
};

const CancelFlashChecklistStepSection = {
  /* Ordered list of sections in the flash cancellation checklist.
  NOTE: The relative order of these steps is important.
  IF YOU ADD A NEW STEP SECTION,
  you MUST add it in the relative order to other sections. */
  PAUSE_OPERATIONS: 1,
  START_CANCELLATION: 2,
  /* Only present when rerun raw data source instance is SECONDARY */
  SECONDARY_RAW_DATA_CLEANUP: 3,
  SECONDARY_INGEST_VIEW_CLEANUP: 4,
  FINALIZE_CANCELLATION: 5,
  RESUME_OPERATIONS: 6,
  DONE: 7,
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
    primaryIngestRawFileProcessingStatus,
    setPrimaryIngestRawFileProcessingStatus,
  ] = React.useState<IngestRawFileProcessingStatus[] | null>(null);
  const [
    secondaryIngestRawFileProcessingStatus,
    setSecondaryIngestRawFileProcessingStatus,
  ] = React.useState<IngestRawFileProcessingStatus[] | null>(null);
  const [
    currentSecondaryRawDataSourceInstance,
    setSecondaryRawDataSourceInstance,
  ] = React.useState<DirectIngestInstance | null>(null);
  const [modalVisible, setModalVisible] = React.useState(true);

  const history = useHistory();
  // Uses useRef so abort controller not re-initialized every render cycle.
  const abortControllerRef =
    React.useRef<AbortController | undefined>(undefined);
  const [dataLoading, setDataLoading] = React.useState<boolean>(true);

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
  const isSecondaryRawDataImport =
    currentSecondaryRawDataSourceInstance === DirectIngestInstance.SECONDARY;

  const incrementCurrentStep = async () => setCurrentStep(currentStep + 1);

  // Promise.allSettled() return both a status and a (potential) return
  // value. Because the return values vary depending on whether the status
  // indicates a success, this function only returns the associated value if
  // the associated status is "fulfilled", otherwise it returns null.
  function getValueIfResolved<Value>(
    result: PromiseSettledResult<Value>
  ): Value | null {
    return result.status === "fulfilled" ? result.value : null;
  }

  const getStatusData = React.useCallback(async () => {
    if (stateInfo) {
      try {
        const statusResults = await Promise.allSettled([
          fetchCurrentIngestInstanceStatus(
            stateInfo.code,
            DirectIngestInstance.PRIMARY
          ),
          fetchCurrentIngestInstanceStatus(
            stateInfo.code,
            DirectIngestInstance.SECONDARY
          ),
        ]);
        setPrimaryIngestInstanceStatus(getValueIfResolved(statusResults[0]));
        setSecondaryIngestInstanceStatus(getValueIfResolved(statusResults[1]));
      } catch (err) {
        message.error(`An error occured: ${err}`);
      }
    }
  }, [stateInfo]);

  const getData = React.useCallback(async () => {
    if (stateInfo) {
      setDataLoading(true);
      if (abortControllerRef.current) {
        abortControllerRef.current.abort();
        abortControllerRef.current = undefined;
      }

      // Ingest instance status information is fetched upon
      // inital page load after a state is set, and each
      // after each step is completed.
      getStatusData();

      // Raw data source instance and ingest bucket processing statuses,
      // are only fetched upon initial page load after a state is set.
      try {
        abortControllerRef.current = new AbortController();
        const results = await Promise.allSettled([
          fetchCurrentRawDataSourceInstance(
            stateInfo.code,
            DirectIngestInstance.SECONDARY
          ),
          getIngestRawFileProcessingStatus(
            stateInfo.code,
            DirectIngestInstance.PRIMARY,
            abortControllerRef.current
          ),
          getIngestRawFileProcessingStatus(
            stateInfo.code,
            DirectIngestInstance.SECONDARY,
            abortControllerRef.current
          ),
        ]);
        setSecondaryRawDataSourceInstance(getValueIfResolved(results[0]));
        setPrimaryIngestRawFileProcessingStatus(
          await getValueIfResolved(results[1])?.json()
        );
        setSecondaryIngestRawFileProcessingStatus(
          await getValueIfResolved(results[2])?.json()
        );
      } catch (err) {
        message.error(`An error occured: ${err}`);
      }
      setDataLoading(false);
    }
  }, [stateInfo, getStatusData]);

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
    setPrimaryIngestRawFileProcessingStatus(null);
    setSecondaryIngestRawFileProcessingStatus(null);
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
    nextSection,
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
                await getStatusData();
                await incrementCurrentStep();
                if (nextSection !== undefined) {
                  await moveToNextChecklistSection(nextSection);
                }
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
            await getStatusData();
            await incrementCurrentStep();
            setLoading(false);
            if (nextSection !== undefined) {
              await moveToNextChecklistSection(nextSection);
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
            nextSection={CancelFlashChecklistStepSection.START_CANCELLATION}
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
            nextSection={
              isSecondaryRawDataImport
                ? CancelFlashChecklistStepSection.SECONDARY_RAW_DATA_CLEANUP
                : CancelFlashChecklistStepSection.SECONDARY_INGEST_VIEW_CLEANUP
            }
          />
        </ChecklistSection>
        {isSecondaryRawDataImport ? (
          <ChecklistSection
            currentStep={currentStep}
            currentStepSection={currentStepSection}
            stepSection={
              CancelFlashChecklistStepSection.SECONDARY_RAW_DATA_CLEANUP
            }
            headerContents={
              <p>
                Clean Up Raw Data and Associated Metadata in{" "}
                <code>SECONDARY</code>
              </p>
            }
          >
            <StyledStep
              title="Clean up SECONDARY raw data on BQ"
              description={
                <p>
                  Delete the contents of the tables in{" "}
                  <code>{stateCode.toLowerCase()}_raw_data_secondary</code>{" "}
                  (without deleting the tables themselves)
                </p>
              }
              actionButtonEnabled={isFlashCancellationInProgress}
              actionButtonTitle="Clean up SECONDARY raw data"
              onActionButtonClick={async () =>
                deleteContentsOfRawDataTables(
                  stateCode,
                  DirectIngestInstance.SECONDARY
                )
              }
            />
            <StyledStep
              title="Clear Out SECONDARY Ingest GCS Bucket"
              description={
                <p>
                  Move any remaining unprocessed raw files in{" "}
                  <code>
                    {projectId}-direct-ingest-state-
                    {stateCode.toLowerCase().replaceAll("_", "-")}-secondary
                  </code>{" "}
                  to deprecated.
                  <CodeBlock
                    // TODO(#17068): Update to python script, once it exists.
                    enabled={
                      currentStepSection ===
                      CancelFlashChecklistStepSection.SECONDARY_RAW_DATA_CLEANUP
                    }
                  >
                    gsutil -m mv &#39;gs://{projectId}
                    -direct-ingest-state-
                    {stateCode.toLowerCase().replaceAll("_", "-")}
                    -secondary/*_raw_*&#39; gs://
                    {projectId}
                    -direct-ingest-state-storage-secondary/
                    {stateCode.toLowerCase()}/deprecated/deprecated_on_
                    {new Date().toLocaleDateString().replaceAll("/", "_")}
                  </CodeBlock>
                </p>
              }
            />
            <StyledStep
              title="Move SECONDARY storage raw files to deprecated"
              description={
                <p>
                  Use the command below within the <code>pipenv shell</code> to
                  move SECONDARY storage raw files to deprecated (confirm output
                  with <code>--dry-run True</code> before proceeding)
                  <CodeBlock
                    enabled={
                      currentStepSection ===
                      FlashChecklistStepSection.SECONDARY_RAW_DATA_CLEANUP
                    }
                  >
                    python -m
                    recidiviz.tools.ingest.operations.move_storage_raw_files_to_deprecated
                    --project-id {projectId} --region {stateCode.toLowerCase()}{" "}
                    --ingest-instance SECONDARY --skip-prompts True --dry-run
                    True
                  </CodeBlock>
                </p>
              }
              nextSection={
                CancelFlashChecklistStepSection.SECONDARY_INGEST_VIEW_CLEANUP
              }
            />
            <StyledStep
              title="Deprecate SECONDARY raw data rows in operations DB"
              description={
                <p>
                  Mark all <code>SECONDARY</code> instance rows in the{" "}
                  <code>direct_ingest_raw_file_metadata</code> operations
                  database table as invalidated.
                </p>
              }
              actionButtonEnabled={isFlashCancellationInProgress}
              actionButtonTitle="Invalidate secondary rows"
              onActionButtonClick={async () =>
                markInstanceRawDataInvalidated(
                  stateCode,
                  DirectIngestInstance.SECONDARY
                )
              }
            />
          </ChecklistSection>
        ) : null}
        <ChecklistSection
          currentStep={currentStep}
          currentStepSection={currentStepSection}
          stepSection={
            CancelFlashChecklistStepSection.SECONDARY_INGEST_VIEW_CLEANUP
          }
          headerContents={
            <p>
              Clean Up Ingest View Data and Associated Metadata in{" "}
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
            nextSection={CancelFlashChecklistStepSection.FINALIZE_CANCELLATION}
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
            nextSection={CancelFlashChecklistStepSection.RESUME_OPERATIONS}
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
            nextSection={CancelFlashChecklistStepSection.DONE}
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
    const secondaryRawDataDataset = `${stateCode.toLowerCase()}_raw_data_secondary`;
    const primaryRawDataDataset = `${stateCode.toLowerCase()}_raw_data`;
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
            nextSection={FlashChecklistStepSection.START_FLASH}
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
            nextSection={
              isSecondaryRawDataImport
                ? FlashChecklistStepSection.PRIMARY_RAW_DATA_DEPRECATION
                : FlashChecklistStepSection.PRIMARY_INGEST_VIEW_DEPRECATION
            }
          />
        </ChecklistSection>
        {isSecondaryRawDataImport ? (
          <ChecklistSection
            currentStep={currentStep}
            currentStepSection={currentStepSection}
            stepSection={FlashChecklistStepSection.PRIMARY_RAW_DATA_DEPRECATION}
            headerContents={
              <p>
                Deprecate Raw Data and Associated Metadata in{" "}
                <code>PRIMARY</code>
              </p>
            }
          >
            <StyledStep
              title="Backup PRIMARY raw data"
              description={
                <>
                  <p>
                    Move all primary instance raw data to a backup dataset in
                    BQ.
                  </p>
                </>
              }
              actionButtonEnabled={isFlashInProgress}
              actionButtonTitle="Move PRIMARY Raw Data to Backup"
              onActionButtonClick={async () =>
                copyRawDataToBackup(stateCode, DirectIngestInstance.PRIMARY)
              }
            />
            <StyledStep
              title="Move PRIMARY storage raw files to deprecated"
              description={
                <p>
                  Use the command below within the <code>pipenv shell</code> to
                  move PRIMARY storage raw files to deprecated (confirm output
                  with <code>--dry-run True</code> before proceeding)
                  <CodeBlock
                    enabled={
                      currentStepSection ===
                      FlashChecklistStepSection.PRIMARY_RAW_DATA_DEPRECATION
                    }
                  >
                    python -m
                    recidiviz.tools.ingest.operations.move_storage_raw_files_to_deprecated
                    \ --project-id {projectId} \ --region{" "}
                    {stateCode.toLowerCase()} \ --ingest-instance PRIMARY \
                    --dry-run True
                  </CodeBlock>
                </p>
              }
            />
            <StyledStep
              title="Deprecate PRIMARY raw data rows in operations DB"
              description={
                <p>
                  Mark all <code>PRIMARY</code> instance rows in the{" "}
                  <code>direct_ingest_raw_file_metadata</code> operations
                  database table as invalidated.
                </p>
              }
              actionButtonEnabled={isFlashInProgress}
              actionButtonTitle="Deprecate primary rows"
              onActionButtonClick={async () =>
                markInstanceRawDataInvalidated(
                  stateCode,
                  DirectIngestInstance.PRIMARY
                )
              }
              nextSection={
                FlashChecklistStepSection.PRIMARY_INGEST_VIEW_DEPRECATION
              }
            />
          </ChecklistSection>
        ) : null}
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
            nextSection={
              isSecondaryRawDataImport
                ? FlashChecklistStepSection.FLASH_RAW_DATA_TO_PRIMARY
                : FlashChecklistStepSection.FLASH_INGEST_VIEW_TO_PRIMARY
            }
          />
        </ChecklistSection>
        {isSecondaryRawDataImport ? (
          <ChecklistSection
            currentStep={currentStep}
            currentStepSection={currentStepSection}
            stepSection={FlashChecklistStepSection.FLASH_RAW_DATA_TO_PRIMARY}
            headerContents={
              <p>
                Flash Raw Data to <code>PRIMARY</code>
              </p>
            }
          >
            <StyledStep
              title="Move raw data metadata from SECONDARY instance to PRIMARY"
              description={
                <p>
                  Update all rows in the{" "}
                  <code>direct_ingest_raw_file_metadata</code> operations
                  database that had instance <code>SECONDARY</code> with updated
                  instance <code>PRIMARY</code>.
                </p>
              }
              actionButtonEnabled={isFlashInProgress}
              actionButtonTitle="Move Secondary Raw Data Metadata"
              onActionButtonClick={async () =>
                transferRawDataMetadataToNewInstance(
                  stateCode,
                  DirectIngestInstance.SECONDARY,
                  DirectIngestInstance.PRIMARY
                )
              }
            />
            <StyledStep
              title="Copy SECONDARY raw data to PRIMARY on BQ"
              description={
                <p>
                  Copy all raw data from BQ dataset{" "}
                  <code>{secondaryRawDataDataset}</code> to BQ dataset{" "}
                  <code>{primaryRawDataDataset}</code>
                </p>
              }
              actionButtonEnabled={isFlashInProgress}
              actionButtonTitle="Copy Secondary Raw Data"
              onActionButtonClick={async () =>
                copyRawDataBetweenInstances(
                  stateCode,
                  DirectIngestInstance.SECONDARY,
                  DirectIngestInstance.PRIMARY
                )
              }
            />
            <StyledStep
              title="Move SECONDARY storage raw data to PRIMARY"
              description={
                <div>
                  <p>Run the following command from the terminal: </p>
                  <CodeBlock
                    enabled={
                      currentStepSection ===
                      FlashChecklistStepSection.FLASH_RAW_DATA_TO_PRIMARY
                    }
                  >
                    gsutil -m mv &#39;gs://{projectId}
                    -direct-ingest-state-storage-secondary/
                    {stateCode.toLowerCase()}/raw/*&#39; gs://
                    {projectId}
                    -direct-ingest-state-storage/
                    {stateCode.toLowerCase()}/raw/
                  </CodeBlock>
                </div>
              }
              nextSection={
                FlashChecklistStepSection.FLASH_INGEST_VIEW_TO_PRIMARY
              }
            />
          </ChecklistSection>
        ) : null}
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
            actionButtonTitle="Move Secondary Ingest View Metadata"
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
            nextSection={
              isSecondaryRawDataImport
                ? FlashChecklistStepSection.SECONDARY_RAW_DATA_CLEANUP
                : FlashChecklistStepSection.SECONDARY_INGEST_VIEW_CLEANUP
            }
          />
        </ChecklistSection>
        {isSecondaryRawDataImport ? (
          <ChecklistSection
            currentStep={currentStep}
            currentStepSection={currentStepSection}
            stepSection={FlashChecklistStepSection.SECONDARY_RAW_DATA_CLEANUP}
            headerContents={
              <p>
                Clean Up Raw Data and Associated Metadata in{" "}
                <code>SECONDARY</code>
              </p>
            }
          >
            <StyledStep
              title="Clean up SECONDARY raw data on BQ"
              description={
                <p>
                  Delete the contents of the tables in{" "}
                  <code>{stateCode.toLowerCase()}_raw_data_secondary</code>{" "}
                  (without deleting the tables themselves)
                </p>
              }
              actionButtonEnabled={isFlashInProgress}
              actionButtonTitle="Clean up SECONDARY raw data"
              onActionButtonClick={async () =>
                deleteContentsOfRawDataTables(
                  stateCode,
                  DirectIngestInstance.SECONDARY
                )
              }
              nextSection={
                FlashChecklistStepSection.SECONDARY_INGEST_VIEW_CLEANUP
              }
            />
          </ChecklistSection>
        ) : null}
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
            nextSection={FlashChecklistStepSection.FINALIZE_FLASH}
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
            nextSection={FlashChecklistStepSection.RESUME_OPERATIONS}
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
            actionButtonEnabled={isNoRerunInProgress}
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
            nextSection={FlashChecklistStepSection.TRIGGER_PIPELINES}
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

  const unprocessedFilesInPrimaryIngestBucket =
    primaryIngestRawFileProcessingStatus !== null
      ? primaryIngestRawFileProcessingStatus.filter(
          (info) => info.numberFilesInBucket !== 0
        )
      : [];
  const unprocessedFilesInSecondaryIngestBucket =
    secondaryIngestRawFileProcessingStatus !== null
      ? secondaryIngestRawFileProcessingStatus.filter(
          (info) => info.numberFilesInBucket !== 0
        )
      : [];
  const emptyIngestBuckets =
    unprocessedFilesInPrimaryIngestBucket.length === 0 &&
    unprocessedFilesInSecondaryIngestBucket.length === 0;
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
  } else if (dataLoading || currentPrimaryIngestInstanceStatus === undefined) {
    activeComponent = <Spin />;
  } else if (
    currentSecondaryRawDataSourceInstance === DirectIngestInstance.SECONDARY &&
    !emptyIngestBuckets
  ) {
    const formattedStateCode = stateInfo.code
      .toLowerCase()
      .replaceAll("_", "-");
    const primaryBucketURL = `https://console.cloud.google.com/storage/browser/${projectId}-direct-ingest-state-${formattedStateCode}`;
    const secondaryBucketURL = `https://console.cloud.google.com/storage/browser/${projectId}-direct-ingest-state-${formattedStateCode}-secondary`;
    activeComponent = (
      <div>
        Cannot proceed with flash of SECONDARY raw data to PRIMARY, because the
        PRIMARY and/or SECONDARY ingest buckets are not empty. Below are the
        file tags present in the ingest buckets.
        <br />
        <h3>
          PRIMARY INGEST BUCKET: (<a href={primaryBucketURL}>link</a>)
        </h3>
        {unprocessedFilesInPrimaryIngestBucket.length === 0 ? (
          <p>EMPTY</p>
        ) : (
          <ul>
            {unprocessedFilesInPrimaryIngestBucket.map((o) => (
              <li>{o.fileTag}</li>
            ))}
          </ul>
        )}
        <h3>
          SECONDARY INGEST BUCKET (<a href={secondaryBucketURL}>link</a>)
        </h3>
        {unprocessedFilesInSecondaryIngestBucket.length === 0 ? (
          <p>EMPTY</p>
        ) : (
          <ul>
            {unprocessedFilesInSecondaryIngestBucket.map((o) => (
              <li>{o.fileTag}</li>
            ))}
          </ul>
        )}
      </div>
    );
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
      /* This covers when a decision has been made to
      proceed with a flash from SECONDARY to PRIMARY */

      <StateProceedWithFlashChecklist stateCode={stateInfo.code} />
    );
  } else if (!proceedWithFlash || isFlashCancellationInProgress) {
    activeComponent = (
      /* If decision has been made to cancel a flash from SECONDARY to PRIMARY */
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
