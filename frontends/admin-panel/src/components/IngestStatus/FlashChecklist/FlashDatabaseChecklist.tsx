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
  Card,
  Divider,
  message,
  Modal,
  PageHeader,
  Spin,
  StepProps,
  Steps,
} from "antd";
import * as React from "react";
import { useHistory } from "react-router";

import {
  changeIngestInstanceStatus,
  fetchIngestStateCodes,
  updateIngestQueuesState,
} from "../../../AdminPanelAPI";
import {
  copyRawDataBetweenInstances,
  copyRawDataToBackup,
  deleteContentsOfRawDataTables,
  deleteTablesInPruningDatasets,
  getIngestRawFileProcessingStatus,
  invalidateIngestPipelineRuns,
  markInstanceRawDataInvalidated,
  purgeIngestQueues,
  transferRawDataMetadataToNewInstance,
  triggerCalculationDAGForState,
} from "../../../AdminPanelAPI/IngestOperations";
import { StateCodeInfo } from "../../general/constants";
import { fetchCurrentIngestInstanceStatus } from "../../Utilities/IngestInstanceUtilities";
import StateSelector from "../../Utilities/StateSelector";
import {
  DirectIngestInstance,
  IngestRawFileProcessingStatus,
  QueueState,
} from "../constants";

interface StyledStepContent {
  // Text to be displayed for this step
  description: JSX.Element;

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

interface ContentStepProps extends StepProps {
  content: JSX.Element;
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
  PAUSE_OPERATIONS: 0,
  START_FLASH: 1,
  /* Only present when rerun raw data source instance is SECONDARY */
  PRIMARY_RAW_DATA_DEPRECATION: 2,
  /* Only present when rerun raw data source instance is SECONDARY */
  FLASH_RAW_DATA_TO_PRIMARY: 3,
  /* Only present when rerun raw data source instance is SECONDARY */
  SECONDARY_RAW_DATA_CLEANUP: 4,
  FINALIZE_FLASH: 5,
  RESUME_OPERATIONS: 6,
  TRIGGER_PIPELINES: 7,
  DONE: 8,
};

const CancelRerunChecklistStepSection = {
  /* Ordered list of sections in the rerun cancellation checklist.
  NOTE: The relative order of these steps is important.
  IF YOU ADD A NEW STEP SECTION,
  you MUST add it in the relative order to other sections. */
  PAUSE_OPERATIONS: 0,
  START_CANCELLATION: 1,
  /* Only present when rerun raw data source instance is SECONDARY */
  SECONDARY_RAW_DATA_CLEANUP: 2,
  FINALIZE_CANCELLATION: 3,
  RESUME_OPERATIONS: 4,
  DONE: 5,
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
  children?: React.ReactNode;
  items: ContentStepProps[];
  headerContents: React.ReactNode;
  currentStep: number;
  currentStepSection: number;
  stepSection: number;
}

const ChecklistSection = ({
  children,
  items,
  headerContents,
  currentStep,
  currentStepSection,
  stepSection,
}: ChecklistSectionProps): JSX.Element => {
  const currentStepsSection =
    currentStepSection === stepSection ? currentStep : 0;

  return (
    <div
      style={{
        opacity: currentStepSection === stepSection ? 1 : 0.25,
        pointerEvents: currentStepSection === stepSection ? "initial" : "none",
      }}
    >
      <ChecklistSectionHeader
        currentStepSection={currentStepSection}
        stepSection={stepSection}
      >
        {headerContents}
      </ChecklistSectionHeader>
      <div style={{ display: "flex" }}>
        <Card style={{ float: "left", width: "30%" }}>
          <Steps
            progressDot
            current={currentStepsSection}
            direction="vertical"
            size="small"
            items={items.map((item: ContentStepProps) => ({
              ...item,
              title: <div style={{ textWrap: "wrap" }}>{item.title}</div>,
            }))}
          >
            {children}
          </Steps>
        </Card>
        <Card style={{ width: "100%", marginLeft: "1%" }}>
          {items.length > 0 ? items[currentStepsSection]?.content : undefined}
        </Card>
      </div>
    </div>
  );
};

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
  const [proceedWithFlash, setProceedWithFlash] = React.useState<
    boolean | null
  >(null);
  const [
    primaryIngestRawFileProcessingStatus,
    setPrimaryIngestRawFileProcessingStatus,
  ] = React.useState<IngestRawFileProcessingStatus[] | null>(null);
  const [
    secondaryIngestRawFileProcessingStatus,
    setSecondaryIngestRawFileProcessingStatus,
  ] = React.useState<IngestRawFileProcessingStatus[] | null>(null);
  const [modalVisible, setModalVisible] = React.useState(true);

  const history = useHistory();
  // Uses useRef so abort controller not re-initialized every render cycle.
  const abortControllerRef = React.useRef<AbortController | undefined>(
    undefined
  );
  const [dataLoading, setDataLoading] = React.useState<boolean>(true);

  const isFlashInProgress =
    currentPrimaryIngestInstanceStatus === "FLASH_IN_PROGRESS" &&
    currentSecondaryIngestInstanceStatus === "FLASH_IN_PROGRESS";

  const isReimportCancellationInProgress =
    currentSecondaryIngestInstanceStatus ===
    "RAW_DATA_REIMPORT_CANCELLATION_IN_PROGRESS";

  const isReimportCanceled =
    currentSecondaryIngestInstanceStatus === "RAW_DATA_REIMPORT_CANCELED";

  const isReadyToFlash =
    currentSecondaryIngestInstanceStatus === "READY_TO_FLASH";
  const isFlashCompleted =
    currentSecondaryIngestInstanceStatus === "FLASH_COMPLETED";

  const isNoReimportInProgress =
    currentSecondaryIngestInstanceStatus === "NO_RAW_DATA_REIMPORT_IN_PROGRESS";

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
        setPrimaryIngestRawFileProcessingStatus(
          await getValueIfResolved(results[0])?.json()
        );
        setSecondaryIngestRawFileProcessingStatus(
          await getValueIfResolved(results[1])?.json()
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
    setCurrentStepSection(0);
    setStateInfo(info);
    setProceedWithFlash(null);
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

  // eslint-disable-next-line react/no-unstable-nested-components
  const StyledStepContent = ({
    actionButtonTitle,
    onActionButtonClick,
    nextSection,
    description,
    actionButtonEnabled,
  }: StyledStepContent): JSX.Element => {
    const [loading, setLoading] = React.useState(false);

    return (
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
            style={{ marginRight: 5 }}
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
        >
          Mark Done
        </Button>
      </>
    );
  };

  interface StateFlashingChecklistProps {
    stateCode: string;
  }

  // eslint-disable-next-line react/no-unstable-nested-components
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
            <b>Cancel secondary rerun.</b> Delete and clean up results in
            SECONDARY and do not copy over to PRIMARY.
          </li>
        </ul>
        <Button type="primary" onClick={onSelectProceed}>
          Proceed with Flash
        </Button>
        <Button onClick={onSelectCancel}>Cancel Rerun</Button>
      </div>
    );
  };

  // eslint-disable-next-line react/no-unstable-nested-components
  const StateCancelRerunChecklist = ({
    stateCode,
  }: StateFlashingChecklistProps): JSX.Element => {
    // --- step 1: pause operations ---------------------------------------------------

    const pauseOperationsSteps = [
      {
        title: "Pause Queues",
        content: (
          <StyledStepContent
            description={
              <p>Pause all of the ingest-related queues for {stateCode}.</p>
            }
            actionButtonTitle="Pause Queues"
            actionButtonEnabled={proceedWithFlash === false}
            onActionButtonClick={async () =>
              updateIngestQueuesState(stateCode, QueueState.PAUSED)
            }
            nextSection={CancelRerunChecklistStepSection.START_CANCELLATION}
          />
        ),
        style: { paddingBottom: 5 },
      },
    ];

    // --- step 2: start re-import cancelation -----------------------------------------

    const startReimportCancelationSteps = [
      {
        title: "Set status to RAW_DATA_REIMPORT_CANCELLATION_IN_PROGRESS",
        content: (
          <StyledStepContent
            description={
              <p>
                Set ingest status to RAW_DATA_REIMPORT_CANCELLATION_IN_PROGRESS
                in SECONDARY in &nbsp;
                {stateCode}.
              </p>
            }
            actionButtonTitle="Update Ingest Instance Status"
            actionButtonEnabled={proceedWithFlash === false}
            onActionButtonClick={async () =>
              changeIngestInstanceStatus(
                stateCode,
                DirectIngestInstance.SECONDARY,
                "RAW_DATA_REIMPORT_CANCELLATION_IN_PROGRESS"
              )
            }
            nextSection={
              CancelRerunChecklistStepSection.SECONDARY_RAW_DATA_CLEANUP
            }
          />
        ),
      },
    ];

    // --- step 3: clean up metadata ---------------------------------------------------

    const cleanUpMetadataSteps = [
      {
        title: "Clean up SECONDARY raw data on BQ",
        content: (
          <StyledStepContent
            description={
              <p>
                Delete the contents of the tables in{" "}
                <code>{stateCode.toLowerCase()}_raw_data_secondary</code>{" "}
                (without deleting the tables themselves)
              </p>
            }
            actionButtonEnabled={isReimportCancellationInProgress}
            actionButtonTitle="Clean up SECONDARY raw data"
            onActionButtonClick={async () =>
              deleteContentsOfRawDataTables(
                stateCode,
                DirectIngestInstance.SECONDARY
              )
            }
          />
        ),
      },
      {
        title: "Clean up PRUNING raw data tables in SECONDARY on BQ",
        content: (
          <StyledStepContent
            description={
              <p>
                Delete any outstanding tables in{" "}
                <code>
                  pruning_{stateCode.toLowerCase()}_new_raw_data_secondary
                </code>{" "}
                and{" "}
                <code>
                  pruning_{stateCode.toLowerCase()}
                  _raw_data_diff_results_secondary
                </code>
              </p>
            }
            actionButtonEnabled={isReimportCancellationInProgress}
            actionButtonTitle="Clean up SECONDARY raw data"
            onActionButtonClick={async () =>
              deleteTablesInPruningDatasets(
                stateCode,
                DirectIngestInstance.SECONDARY
              )
            }
          />
        ),
      },
      {
        title: "Clear Out SECONDARY Ingest GCS Bucket",
        content: (
          <StyledStepContent
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
                    CancelRerunChecklistStepSection.SECONDARY_RAW_DATA_CLEANUP
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
        ),
      },
      {
        title: "Move SECONDARY storage raw files to deprecated",
        content: (
          <StyledStepContent
            description={
              <p>
                Use the command below within the <code>pipenv shell</code> to
                move SECONDARY storage raw files to deprecated
                <CodeBlock
                  enabled={
                    currentStepSection ===
                    CancelRerunChecklistStepSection.SECONDARY_RAW_DATA_CLEANUP
                  }
                >
                  python -m
                  recidiviz.tools.ingest.operations.move_storage_raw_files_to_deprecated
                  --project-id {projectId} --region {stateCode.toLowerCase()}{" "}
                  --ingest-instance SECONDARY --skip-prompts True --dry-run
                  False
                </CodeBlock>
              </p>
            }
          />
        ),
      },
      {
        title: "Deprecate SECONDARY raw data rows in operations DB",
        content: (
          <StyledStepContent
            description={
              <p>
                Mark all <code>SECONDARY</code> instance rows in the{" "}
                <code>direct_ingest_raw_file_metadata</code> operations database
                table as invalidated.
              </p>
            }
            actionButtonEnabled={isReimportCancellationInProgress}
            actionButtonTitle="Invalidate secondary rows"
            onActionButtonClick={async () =>
              markInstanceRawDataInvalidated(
                stateCode,
                DirectIngestInstance.SECONDARY
              )
            }
            nextSection={CancelRerunChecklistStepSection.FINALIZE_CANCELLATION}
          />
        ),
      },
    ];

    // --- step 4: finalize cancelation ------------------------------------------------

    const finalizeCancelationSteps = [
      {
        title: "Set SECONDARY status to RAW_DATA_REIMPORT_CANCELED",
        content: (
          <StyledStepContent
            description={
              <p>
                Set ingest status to RAW_DATA_REIMPORT_CANCELED in SECONDARY in
                &nbsp;
                {stateCode}.
              </p>
            }
            actionButtonEnabled={isReimportCancellationInProgress}
            actionButtonTitle="Update Ingest Instance Status"
            onActionButtonClick={async () =>
              changeIngestInstanceStatus(
                stateCode,
                DirectIngestInstance.SECONDARY,
                "RAW_DATA_REIMPORT_CANCELED"
              )
            }
          />
        ),
      },
      {
        title: "Set status to NO_RAW_DATA_REIMPORT_IN_PROGRESS",
        content: (
          <StyledStepContent
            description={
              <p>
                Set ingest status to NO_RAW_DATA_REIMPORT_IN_PROGRESS in
                SECONDARY in &nbsp;
                {stateCode}.
              </p>
            }
            actionButtonEnabled={isReimportCanceled}
            actionButtonTitle="Update Ingest Instance Status"
            onActionButtonClick={async () =>
              changeIngestInstanceStatus(
                stateCode,
                DirectIngestInstance.SECONDARY,
                "NO_RAW_DATA_REIMPORT_IN_PROGRESS"
              )
            }
            nextSection={CancelRerunChecklistStepSection.RESUME_OPERATIONS}
          />
        ),
      },
    ];

    // --- step 5: resume operations ---------------------------------------------------

    const resumeOperationsSteps = [
      {
        title: "Unpause queues",
        content: (
          <StyledStepContent
            description={
              <p>
                Now that the database cleanup is complete, unpause the queues.
              </p>
            }
            actionButtonTitle="Unpause Queues"
            actionButtonEnabled={isNoReimportInProgress}
            onActionButtonClick={async () =>
              updateIngestQueuesState(stateCode, QueueState.RUNNING)
            }
            nextSection={CancelRerunChecklistStepSection.DONE}
          />
        ),
      },
    ];

    return (
      <div>
        <h1>Canceling SECONDARY Raw Data Reimport</h1>
        <h3 style={{ color: "green" }}>Current Ingest Instance Statuses:</h3>
        <ul style={{ color: "green" }}>
          <li>PRIMARY INSTANCE: {currentPrimaryIngestInstanceStatus}</li>
          <li>SECONDARY INSTANCE: {currentSecondaryIngestInstanceStatus}</li>
        </ul>
        <ChecklistSection
          currentStep={currentStep}
          currentStepSection={currentStepSection}
          stepSection={CancelRerunChecklistStepSection.PAUSE_OPERATIONS}
          headerContents="Pause Operations"
          items={pauseOperationsSteps}
        />
        <ChecklistSection
          currentStep={currentStep}
          currentStepSection={currentStepSection}
          stepSection={CancelRerunChecklistStepSection.START_CANCELLATION}
          headerContents={<p>Start Reimport Cancellation</p>}
          items={startReimportCancelationSteps}
        />
        <ChecklistSection
          currentStep={currentStep}
          currentStepSection={currentStepSection}
          stepSection={
            CancelRerunChecklistStepSection.SECONDARY_RAW_DATA_CLEANUP
          }
          headerContents={
            <p>
              Clean Up Raw Data and Associated Metadata in{" "}
              <code>SECONDARY</code>
            </p>
          }
          items={cleanUpMetadataSteps}
        />
        <ChecklistSection
          currentStep={currentStep}
          currentStepSection={currentStepSection}
          stepSection={CancelRerunChecklistStepSection.FINALIZE_CANCELLATION}
          headerContents={<p>Finalize Reimport Cancellation</p>}
          items={finalizeCancelationSteps}
        />
        <ChecklistSection
          currentStep={currentStep}
          currentStepSection={currentStepSection}
          stepSection={CancelRerunChecklistStepSection.RESUME_OPERATIONS}
          headerContents={<p>Resume Operations</p>}
          items={resumeOperationsSteps}
        />
        <ChecklistSection
          currentStep={currentStep}
          currentStepSection={currentStepSection}
          stepSection={CancelRerunChecklistStepSection.DONE}
          headerContents={
            <p style={{ color: "green" }}>Rerun cancellation is complete!</p>
          }
          items={[]}
        >
          <p>DONE</p>
        </ChecklistSection>
      </div>
    );
  };

  // eslint-disable-next-line react/no-unstable-nested-components
  const StateProceedWithFlashChecklist = ({
    stateCode,
  }: StateFlashingChecklistProps): JSX.Element => {
    const secondaryRawDataDataset = `${stateCode.toLowerCase()}_raw_data_secondary`;
    const primaryRawDataDataset = `${stateCode.toLowerCase()}_raw_data`;

    // --- step 1: pause operations ----------------------------------------------------
    const pauseOperationsSteps = [
      {
        title: "Pause Queues",
        content: (
          <StyledStepContent
            description={
              <p>Pause all of the ingest-related queues for {stateCode}.</p>
            }
            actionButtonTitle="Pause Queues"
            actionButtonEnabled={isReadyToFlash}
            onActionButtonClick={async () =>
              updateIngestQueuesState(stateCode, QueueState.PAUSED)
            }
          />
        ),
      },
      {
        title: "Purge All Ingest Related Queues",
        content: (
          <StyledStepContent
            description={
              <p>Clear out all ingest-related queues in both instances.</p>
            }
            actionButtonTitle="Clear Queue"
            actionButtonEnabled={isReadyToFlash}
            onActionButtonClick={async () => purgeIngestQueues(stateCode)}
            nextSection={FlashChecklistStepSection.START_FLASH}
          />
        ),
      },
    ];
    // --- step 2: start flash ---------------------------------------------------------
    const startFlashSteps = [
      {
        title: "Set status to FLASH_IN_PROGRESS",
        content: (
          <StyledStepContent
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
            nextSection={FlashChecklistStepSection.PRIMARY_RAW_DATA_DEPRECATION}
          />
        ),
      },
    ];
    // --- step 3: deprecate data and metadata -----------------------------------------
    const deprecateDataAndMetadataSteps = [
      {
        title: "Backup PRIMARY raw data",
        content: (
          <StyledStepContent
            description={
              <p>
                Move all primary instance raw data to a backup dataset in BQ.
              </p>
            }
            actionButtonEnabled={isFlashInProgress}
            actionButtonTitle="Move PRIMARY Raw Data to Backup"
            onActionButtonClick={async () =>
              copyRawDataToBackup(stateCode, DirectIngestInstance.PRIMARY)
            }
          />
        ),
      },
      {
        title: "Clean up PRUNING raw data tables in PRIMARY on BQ",
        content: (
          <StyledStepContent
            description={
              <p>
                Delete any outstanding tables in{" "}
                <code>pruning_{stateCode.toLowerCase()}_new_raw_data</code> and{" "}
                <code>
                  pruning_{stateCode.toLowerCase()}
                  _raw_data_diff_results
                </code>
              </p>
            }
            actionButtonEnabled={isReimportCancellationInProgress}
            actionButtonTitle="Clean up PRIMARY raw data"
            onActionButtonClick={async () =>
              deleteTablesInPruningDatasets(
                stateCode,
                DirectIngestInstance.PRIMARY
              )
            }
          />
        ),
      },
      {
        title: "Deprecate ingest pipeline runs for PRIMARY",
        content: (
          <StyledStepContent
            description={
              <p>
                Mark all <code>PRIMARY</code> ingest pipeline rows in the{" "}
                <code>direct_ingest_dataflow_job</code> operations database
                table as invalidated.
              </p>
            }
            actionButtonEnabled={isFlashInProgress}
            actionButtonTitle="Invalidate primary rows"
            onActionButtonClick={async () =>
              invalidateIngestPipelineRuns(
                stateCode,
                DirectIngestInstance.PRIMARY
              )
            }
          />
        ),
      },
      {
        title: "Deprecate PRIMARY raw data rows in operations DB",
        content: (
          <StyledStepContent
            description={
              <p>
                Mark all <code>PRIMARY</code> instance rows in the{" "}
                <code>direct_ingest_raw_file_metadata</code> operations database
                table as invalidated.
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
            nextSection={FlashChecklistStepSection.FLASH_RAW_DATA_TO_PRIMARY}
          />
        ),
      },
    ];
    // --- step 4: execute flash -------------------------------------------------------
    const executeFlashSteps = [
      {
        title: "Move raw data metadata from SECONDARY instance to PRIMARY",
        content: (
          <StyledStepContent
            description={
              <p>
                Update all rows in the{" "}
                <code>direct_ingest_raw_file_metadata</code> operations database
                that had instance <code>SECONDARY</code> with updated instance{" "}
                <code>PRIMARY</code>.
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
        ),
      },
      {
        title: "Copy SECONDARY raw data to PRIMARY on BQ",
        content: (
          <StyledStepContent
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
        ),
      },
      {
        title: "Move SECONDARY storage raw files to deprecated",
        content: (
          <StyledStepContent
            description={
              <p>
                Use the command below within the <code>pipenv shell</code> to
                move SECONDARY storage raw files to deprecated
                <CodeBlock
                  enabled={
                    currentStepSection ===
                    FlashChecklistStepSection.FLASH_RAW_DATA_TO_PRIMARY
                  }
                >
                  python -m
                  recidiviz.tools.ingest.operations.move_storage_raw_files_to_deprecated
                  --project-id {projectId} --region {stateCode.toLowerCase()}{" "}
                  --ingest-instance SECONDARY --skip-prompts True --dry-run
                  False
                </CodeBlock>
              </p>
            }
            nextSection={FlashChecklistStepSection.SECONDARY_RAW_DATA_CLEANUP}
          />
        ),
      },
    ];
    // --- step 5: clean up raw data ---------------------------------------------------
    const cleanUpSteps = [
      {
        title: "Clean up SECONDARY raw data on BQ",
        content: (
          <StyledStepContent
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
            nextSection={FlashChecklistStepSection.FINALIZE_FLASH}
          />
        ),
      },
    ];
    // --- step 6: finalize flash ------------------------------------------------------
    const finalizeFlashSteps = [
      {
        title: "Set status to FLASH_COMPLETED",
        content: (
          <StyledStepContent
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
        ),
      },
      {
        title: "Set status to NO_RAW_DATA_REIMPORT_IN_PROGRESS",
        content: (
          <StyledStepContent
            description={
              <p>
                Set ingest status to NO_RAW_DATA_REIMPORT_IN_PROGRESS in
                SECONDARY in &nbsp;
                {stateCode}.
              </p>
            }
            actionButtonEnabled={isFlashCompleted}
            actionButtonTitle="Update Ingest Instance Status"
            onActionButtonClick={async () =>
              changeIngestInstanceStatus(
                stateCode,
                DirectIngestInstance.SECONDARY,
                "NO_RAW_DATA_REIMPORT_IN_PROGRESS"
              )
            }
            nextSection={FlashChecklistStepSection.RESUME_OPERATIONS}
          />
        ),
      },
    ];
    // --- step 7: resume operations ---------------------------------------------------
    const resumeOperationsSteps = [
      {
        title: "Unpause queues",
        content: (
          <StyledStepContent
            description={
              <p>
                Now that the database flashing is complete, unpause the queues.
              </p>
            }
            actionButtonTitle="Unpause Queues"
            actionButtonEnabled={isNoReimportInProgress}
            onActionButtonClick={async () =>
              updateIngestQueuesState(stateCode, QueueState.RUNNING)
            }
            nextSection={FlashChecklistStepSection.TRIGGER_PIPELINES}
          />
        ),
      },
    ];
    // --- step 8: trigger pipelines ---------------------------------------------------
    const triggerPipelinesSteps = [
      {
        title: "Full Historical Refresh",
        content: (
          <StyledStepContent
            description={
              <p>
                Trigger a BigQuery refresh and run the Calculation DAG for{" "}
                {stateCode} in <code>PRIMARY</code>.
              </p>
            }
            actionButtonTitle="Start Calculation DAG Run"
            actionButtonEnabled
            onActionButtonClick={async () =>
              triggerCalculationDAGForState(stateCode)
            }
            nextSection={FlashChecklistStepSection.DONE}
          />
        ),
      },
    ];

    return (
      <div>
        <Divider />
        <h1>
          Proceeding with Flash of Rerun Results from SECONDARY to PRIMARY
        </h1>
        <h3 style={{ color: "green" }}>Current Ingest Instance Statuses:</h3>
        <ul style={{ color: "green" }}>
          <li>PRIMARY INSTANCE: {currentPrimaryIngestInstanceStatus}</li>
          <li>SECONDARY INSTANCE: {currentSecondaryIngestInstanceStatus}</li>
        </ul>
        <Divider />
        <ChecklistSection
          currentStep={currentStep}
          currentStepSection={currentStepSection}
          stepSection={FlashChecklistStepSection.PAUSE_OPERATIONS}
          headerContents={<p>Pause Operations</p>}
          items={pauseOperationsSteps}
        />
        <ChecklistSection
          currentStep={currentStep}
          currentStepSection={currentStepSection}
          stepSection={FlashChecklistStepSection.START_FLASH}
          headerContents={<p>Start Flash</p>}
          items={startFlashSteps}
        />
        <ChecklistSection
          currentStep={currentStep}
          currentStepSection={currentStepSection}
          stepSection={FlashChecklistStepSection.PRIMARY_RAW_DATA_DEPRECATION}
          headerContents={
            <p>
              Deprecate Raw Data and Associated Metadata in <code>PRIMARY</code>
            </p>
          }
          items={deprecateDataAndMetadataSteps}
        />
        <ChecklistSection
          currentStep={currentStep}
          currentStepSection={currentStepSection}
          stepSection={FlashChecklistStepSection.FLASH_RAW_DATA_TO_PRIMARY}
          headerContents={
            <p>
              Flash Raw Data to <code>PRIMARY</code>
            </p>
          }
          items={executeFlashSteps}
        />
        <ChecklistSection
          currentStep={currentStep}
          currentStepSection={currentStepSection}
          stepSection={FlashChecklistStepSection.SECONDARY_RAW_DATA_CLEANUP}
          headerContents={
            <p>
              Clean Up Raw Data in <code>SECONDARY</code>
            </p>
          }
          items={cleanUpSteps}
        />
        <ChecklistSection
          currentStep={currentStep}
          currentStepSection={currentStepSection}
          stepSection={FlashChecklistStepSection.FINALIZE_FLASH}
          headerContents={<p>Finalize Flash</p>}
          items={finalizeFlashSteps}
        />
        <ChecklistSection
          currentStep={currentStep}
          currentStepSection={currentStepSection}
          stepSection={FlashChecklistStepSection.RESUME_OPERATIONS}
          headerContents={<p>Resume Operations</p>}
          items={resumeOperationsSteps}
        />
        <ChecklistSection
          currentStep={currentStep}
          currentStepSection={currentStepSection}
          stepSection={FlashChecklistStepSection.TRIGGER_PIPELINES}
          headerContents={<p>Trigger Pipelines</p>}
          items={triggerPipelinesSteps}
        />
        <ChecklistSection
          currentStep={currentStep}
          currentStepSection={currentStepSection}
          stepSection={FlashChecklistStepSection.DONE}
          headerContents={<p style={{ color: "green" }}>Flash is complete!</p>}
          items={[]}
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
  const emptyIngestBuckets = true;
  // unprocessedFilesInPrimaryIngestBucket.length === 0 &&
  //   unprocessedFilesInSecondaryIngestBucket.length === 0;
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
  } else if (!emptyIngestBuckets && proceedWithFlash === null) {
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
        <h3 style={{ color: "green" }}>
          Regardless of ingest bucket status, you may proceed with cleaning up
          the secondary instance and canceling the rerun in SECONDARY:{" "}
          <Button
            type="primary"
            onClick={async () => {
              setProceedWithFlash(false);
              moveToNextChecklistSection(
                CancelRerunChecklistStepSection.PAUSE_OPERATIONS
              );
            }}
          >
            CLEAN UP SECONDARY + CANCEL RAW DATA REIMPORT
          </Button>
        </h3>
      </div>
    );
  } else if (
    !isReadyToFlash &&
    !isFlashInProgress &&
    !isReimportCancellationInProgress &&
    proceedWithFlash === null &&
    // This check makes it so we don't show the "can't flash" component
    // when you set the status to FLASH_COMPLETE in the middle of the checklist.
    currentStep === 0
  ) {
    /* If we have loaded a status but it does not indicate that we can proceed with flashing, show an alert on top of the checklist */
    /* Regardless of status, we can cancel a secondary rerun any time */
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
        <h3 style={{ color: "green" }}>
          Regardless of ingest instance status, you may proceed with cleaning up
          the secondary instance and canceling the rerun:{" "}
          <Button
            type="primary"
            onClick={async () => {
              setProceedWithFlash(false);
              moveToNextChecklistSection(
                CancelRerunChecklistStepSection.PAUSE_OPERATIONS
              );
            }}
          >
            CLEAN UP SECONDARY + CANCEL RAW DATA REIMPORT
          </Button>
        </h3>
      </div>
    );
  } else if (proceedWithFlash === null && isReadyToFlash) {
    activeComponent = (
      /* This is the only time that someone can choose whether to cancel a rerun or
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
            CancelRerunChecklistStepSection.PAUSE_OPERATIONS
          );
        }}
      />
    );
  } else if (proceedWithFlash || isFlashInProgress) {
    /* In the case of a refresh in the middle of a flash, proceedWithFlash
    will get reset. Set the value back to true, since a flash is already in
    progress. */
    if (proceedWithFlash === null) {
      setProceedWithFlash(true);
    }
    activeComponent = (
      /* This covers when a decision has been made to
      proceed with a flash from SECONDARY to PRIMARY */
      <StateProceedWithFlashChecklist stateCode={stateInfo.code} />
    );
  } else if (!proceedWithFlash || isReimportCancellationInProgress) {
    activeComponent = (
      /* If decision has been made to cancel a rerun in SECONDARY */
      <StateCancelRerunChecklist stateCode={stateInfo.code} />
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
