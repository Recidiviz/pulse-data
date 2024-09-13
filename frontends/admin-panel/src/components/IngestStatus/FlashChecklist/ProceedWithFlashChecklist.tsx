// Recidiviz - a data platform for criminal justice reform
// Copyright (C) 2024 Recidiviz, Inc.
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

import { Divider } from "antd";
import { observer } from "mobx-react-lite";

import {
  changeIngestInstanceStatus,
  copyRawDataBetweenInstances,
  copyRawDataToBackup,
  deleteContentsOfRawDataTables,
  deleteTablesInPruningDatasets,
  invalidateIngestPipelineRuns,
  markInstanceRawDataInvalidated,
  purgeIngestQueues,
  transferRawDataMetadataToNewInstance,
  triggerCalculationDAGForState,
  updateIngestQueuesState,
} from "../../../AdminPanelAPI/IngestOperations";
import { DirectIngestInstance, QueueState } from "../constants";
import { useFlashChecklistStore } from "./FlashChecklistStore";
import StyledStepContent, {
  ChecklistSection,
  CodeBlock,
} from "./FlashComponents";

interface StateFlashingChecklistProps {
  stateCode: string;
}

export const FlashChecklistStepSection = {
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

const StateProceedWithFlashChecklist = ({
  stateCode,
}: StateFlashingChecklistProps): JSX.Element => {
  const {
    currentStep,
    currentRawDataInstanceStatus: currentIngestStatus,
    currentStepSection,
    projectId,
  } = useFlashChecklistStore();

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
          actionButtonEnabled={currentIngestStatus.isReadyToFlash()}
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
          actionButtonEnabled={currentIngestStatus.isReadyToFlash()}
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
              Set ingest status to FLASH_IN_PROGRESS in PRIMARY and SECONDARY in
              &nbsp;
              {stateCode}.
            </p>
          }
          actionButtonTitle="Update Ingest Instance Status"
          actionButtonEnabled={currentIngestStatus.isReadyToFlash()}
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
            <p>Move all primary instance raw data to a backup dataset in BQ.</p>
          }
          actionButtonEnabled={currentIngestStatus.isFlashInProgress()}
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
          actionButtonEnabled={currentIngestStatus.isReimportCancellationInProgress()}
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
              <code>direct_ingest_dataflow_job</code> operations database table
              as invalidated.
            </p>
          }
          actionButtonEnabled={currentIngestStatus.isFlashInProgress()}
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
          actionButtonEnabled={currentIngestStatus.isFlashInProgress()}
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
          actionButtonEnabled={currentIngestStatus.isFlashInProgress()}
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
          actionButtonEnabled={currentIngestStatus.isFlashInProgress()}
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
              Use the command below within the <code>pipenv shell</code> to move
              SECONDARY storage raw files to deprecated
              <CodeBlock
                enabled={
                  currentStepSection ===
                  FlashChecklistStepSection.FLASH_RAW_DATA_TO_PRIMARY
                }
              >
                python -m
                recidiviz.tools.ingest.operations.move_storage_raw_files_to_deprecated
                --project-id {projectId} --region {stateCode.toLowerCase()}{" "}
                --ingest-instance SECONDARY --skip-prompts True --dry-run False
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
              <code>{stateCode.toLowerCase()}_raw_data_secondary</code> (without
              deleting the tables themselves)
            </p>
          }
          actionButtonEnabled={currentIngestStatus.isFlashInProgress()}
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
          actionButtonEnabled={currentIngestStatus.isFlashInProgress()}
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
              Set ingest status to NO_RAW_DATA_REIMPORT_IN_PROGRESS in SECONDARY
              in &nbsp;
              {stateCode}.
            </p>
          }
          actionButtonEnabled={currentIngestStatus.isFlashCompleted()}
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
          actionButtonEnabled={currentIngestStatus.isNoReimportInProgress()}
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
      <h1>Proceeding with Flash of Rerun Results from SECONDARY to PRIMARY</h1>
      <h3 style={{ color: "green" }}>Current Ingest Instance Statuses:</h3>
      <ul style={{ color: "green" }}>
        <li>PRIMARY INSTANCE: {currentIngestStatus.primary}</li>
        <li>SECONDARY INSTANCE: {currentIngestStatus.secondary}</li>
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

const setStatusInPrimaryAndSecondaryTo = async (
  stateCode: string,
  status: string
): Promise<Response> => {
  const [primaryResponse, secondaryResponse] = await Promise.all([
    changeIngestInstanceStatus(stateCode, DirectIngestInstance.PRIMARY, status),
    changeIngestInstanceStatus(
      stateCode,
      DirectIngestInstance.SECONDARY,
      status
    ),
  ]);
  return primaryResponse.status !== 200 ? primaryResponse : secondaryResponse;
};

export default observer(StateProceedWithFlashChecklist);
