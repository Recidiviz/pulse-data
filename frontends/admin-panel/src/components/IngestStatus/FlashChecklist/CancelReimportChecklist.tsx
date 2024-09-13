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

import { observer } from "mobx-react-lite";

import {
  changeIngestInstanceStatus,
  updateIngestQueuesState,
} from "../../../AdminPanelAPI";
import {
  deleteContentsOfRawDataTables,
  deleteTablesInPruningDatasets,
  markInstanceRawDataInvalidated,
} from "../../../AdminPanelAPI/IngestOperations";
import { DirectIngestInstance, QueueState } from "../constants";
import { useFlashChecklistStore } from "./FlashChecklistStore";
import StyledStepContent, {
  ChecklistSection,
  CodeBlock,
} from "./FlashComponents";

interface StateCancelReimportChecklistProps {
  stateCode: string;
}

export const CancelReimportChecklistStepSection = {
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

const StateCancelReimportChecklist = ({
  stateCode,
}: StateCancelReimportChecklistProps): JSX.Element => {
  const {
    currentStep,
    proceedWithFlash,
    currentRawDataInstanceStatus: currentIngestStatus,
    currentStepSection,
    projectId,
  } = useFlashChecklistStore();

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
          nextSection={CancelReimportChecklistStepSection.START_CANCELLATION}
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
              Set ingest status to RAW_DATA_REIMPORT_CANCELLATION_IN_PROGRESS in
              SECONDARY in &nbsp;
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
            CancelReimportChecklistStepSection.SECONDARY_RAW_DATA_CLEANUP
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
              <code>{stateCode.toLowerCase()}_raw_data_secondary</code> (without
              deleting the tables themselves)
            </p>
          }
          actionButtonEnabled={currentIngestStatus.isReimportCancellationInProgress()}
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
          actionButtonEnabled={currentIngestStatus.isReimportCancellationInProgress()}
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
                  CancelReimportChecklistStepSection.SECONDARY_RAW_DATA_CLEANUP
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
              Use the command below within the <code>pipenv shell</code> to move
              SECONDARY storage raw files to deprecated
              <CodeBlock
                enabled={
                  currentStepSection ===
                  CancelReimportChecklistStepSection.SECONDARY_RAW_DATA_CLEANUP
                }
              >
                python -m
                recidiviz.tools.ingest.operations.move_storage_raw_files_to_deprecated
                --project-id {projectId} --region {stateCode.toLowerCase()}{" "}
                --ingest-instance SECONDARY --skip-prompts True --dry-run False
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
          actionButtonEnabled={currentIngestStatus.isReimportCancellationInProgress()}
          actionButtonTitle="Invalidate secondary rows"
          onActionButtonClick={async () =>
            markInstanceRawDataInvalidated(
              stateCode,
              DirectIngestInstance.SECONDARY
            )
          }
          nextSection={CancelReimportChecklistStepSection.FINALIZE_CANCELLATION}
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
          actionButtonEnabled={currentIngestStatus.isReimportCancellationInProgress()}
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
              Set ingest status to NO_RAW_DATA_REIMPORT_IN_PROGRESS in SECONDARY
              in &nbsp;
              {stateCode}.
            </p>
          }
          actionButtonEnabled={currentIngestStatus.isReimportCanceled()}
          actionButtonTitle="Update Ingest Instance Status"
          onActionButtonClick={async () =>
            changeIngestInstanceStatus(
              stateCode,
              DirectIngestInstance.SECONDARY,
              "NO_RAW_DATA_REIMPORT_IN_PROGRESS"
            )
          }
          nextSection={CancelReimportChecklistStepSection.RESUME_OPERATIONS}
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
          actionButtonEnabled={currentIngestStatus.isNoReimportInProgress()}
          onActionButtonClick={async () =>
            updateIngestQueuesState(stateCode, QueueState.RUNNING)
          }
          nextSection={CancelReimportChecklistStepSection.DONE}
        />
      ),
    },
  ];

  return (
    <div>
      <h1>Canceling SECONDARY Raw Data Reimport</h1>
      <h3 style={{ color: "green" }}>Current Ingest Instance Statuses:</h3>
      <ul style={{ color: "green" }}>
        <li>PRIMARY INSTANCE: {currentIngestStatus.primary}</li>
        <li>SECONDARY INSTANCE: {currentIngestStatus.secondary}</li>
      </ul>
      <ChecklistSection
        currentStep={currentStep}
        currentStepSection={currentStepSection}
        stepSection={CancelReimportChecklistStepSection.PAUSE_OPERATIONS}
        headerContents="Pause Operations"
        items={pauseOperationsSteps}
      />
      <ChecklistSection
        currentStep={currentStep}
        currentStepSection={currentStepSection}
        stepSection={CancelReimportChecklistStepSection.START_CANCELLATION}
        headerContents={<p>Start Reimport Cancellation</p>}
        items={startReimportCancelationSteps}
      />
      <ChecklistSection
        currentStep={currentStep}
        currentStepSection={currentStepSection}
        stepSection={
          CancelReimportChecklistStepSection.SECONDARY_RAW_DATA_CLEANUP
        }
        headerContents={
          <p>
            Clean Up Raw Data and Associated Metadata in <code>SECONDARY</code>
          </p>
        }
        items={cleanUpMetadataSteps}
      />
      <ChecklistSection
        currentStep={currentStep}
        currentStepSection={currentStepSection}
        stepSection={CancelReimportChecklistStepSection.FINALIZE_CANCELLATION}
        headerContents={<p>Finalize Reimport Cancellation</p>}
        items={finalizeCancelationSteps}
      />
      <ChecklistSection
        currentStep={currentStep}
        currentStepSection={currentStepSection}
        stepSection={CancelReimportChecklistStepSection.RESUME_OPERATIONS}
        headerContents={<p>Resume Operations</p>}
        items={resumeOperationsSteps}
      />
      <ChecklistSection
        currentStep={currentStep}
        currentStepSection={currentStepSection}
        stepSection={CancelReimportChecklistStepSection.DONE}
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

export default observer(StateCancelReimportChecklist);
