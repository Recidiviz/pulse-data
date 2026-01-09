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

import { Divider, Spin } from "antd";
import { observer } from "mobx-react-lite";

import {
  acquireResourceLocksForStateAndInstance,
  deleteContentsOfRawDataTables,
  deleteTablesInPruningDatasets,
  markInstanceRawDataInvalidated,
  releaseResourceLocksForStateById,
} from "../../../AdminPanelAPI/IngestOperations";
import { DirectIngestInstance } from "../constants";
import { useFlashChecklistStore } from "./FlashChecklistContext";
import { FlashingChecklistType } from "./FlashChecklistStore";
import StyledStepContent, {
  ChecklistSection,
  CodeBlock,
} from "./FlashComponents";
import {
  cancelReimportLockDescription,
  flashingLockSecondsTtl,
} from "./FlashUtils";

export const CancelReimportChecklistStepSection = {
  /* Ordered list of sections in the rerun cancellation checklist.
  NOTE: The relative order of these steps is important.
  IF YOU ADD A NEW STEP SECTION,
  you MUST add it in the relative order to other sections. */
  ACQUIRE_RESOURCE_LOCKS: 0,
  SECONDARY_RAW_DATA_CLEANUP: 1,
  RELEASE_RESOURCE_LOCKS: 2,
  DONE: 3,
};

const CancelReimportChecklist = (): JSX.Element => {
  const {
    isFlashInProgress,
    currentLockStatus,
    activeChecklist,
    stateInfo,
    currentStep,
    currentStepSection,
    projectId,
  } = useFlashChecklistStore();

  if (!stateInfo) {
    return <Spin />;
  }

  const stateCode = stateInfo.code;

  // --- step 1: acquire resource locks ------------------------------------------------

  const acquireResourceLocksSteps = [
    {
      title: "Acquire Resource Locks",
      content: (
        <StyledStepContent
          description={<p>Acquire all resource locks for: {stateCode}.</p>}
          actionButtonTitle="Acquire Resource Locks"
          actionButtonEnabled={
            activeChecklist === FlashingChecklistType.CANCEL_REIMPORT &&
            currentLockStatus.allSecondaryLocksFree()
          }
          onActionButtonClick={async () =>
            acquireResourceLocksForStateAndInstance(
              stateCode,
              DirectIngestInstance.SECONDARY,
              cancelReimportLockDescription,
              flashingLockSecondsTtl
            )
          }
          nextSection={
            CancelReimportChecklistStepSection.SECONDARY_RAW_DATA_CLEANUP
          }
        />
      ),
      style: { paddingBottom: 5 },
    },
  ];

  // --- step 2: clean up metadata -----------------------------------------------------

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
          actionButtonEnabled={currentLockStatus.allSecondaryLocksHeldByAdHoc()}
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
                {stateCode.toLowerCase()}_new_pruned_raw_data_secondary
              </code>{" "}
              and{" "}
              <code>
                {stateCode.toLowerCase()}
                _raw_data_pruning_diff_results_secondary
              </code>
            </p>
          }
          actionButtonEnabled={currentLockStatus.allSecondaryLocksHeldByAdHoc()}
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
              Use the command below to move SECONDARY storage raw files to
              deprecated
              <CodeBlock
                enabled={
                  currentStepSection ===
                  CancelReimportChecklistStepSection.SECONDARY_RAW_DATA_CLEANUP
                }
              >
                uv run python -m
                recidiviz.tools.ingest.operations.deprecate_raw_data
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
              Mark all <code>SECONDARY</code> instance rows in the following
              operations database tables as invalidated:
              <ul>
                <li>
                  <code>direct_ingest_raw_big_query_file_metadata</code>
                </li>
                <li>
                  <code>direct_ingest_raw_gcs_file_metadata</code>
                </li>
                <li>
                  <code>direct_ingest_raw_data_pruning_metadata</code>
                </li>
                <li>
                  <code>direct_ingest_raw_file_import_run</code>
                </li>
                <li>
                  <code>direct_ingest_raw_file_import</code>
                </li>
              </ul>
            </p>
          }
          actionButtonEnabled={currentLockStatus.allSecondaryLocksHeldByAdHoc()}
          actionButtonTitle="Invalidate secondary rows"
          onActionButtonClick={async () =>
            markInstanceRawDataInvalidated(
              stateCode,
              DirectIngestInstance.SECONDARY
            )
          }
          nextSection={
            CancelReimportChecklistStepSection.RELEASE_RESOURCE_LOCKS
          }
        />
      ),
    },
  ];

  // --- step 3: release resource locks ------------------------------------------------

  const releaseResourceLocksSteps = [
    {
      title: "Release Resource Locks",
      content: (
        <StyledStepContent
          description={
            <p>
              Now that the database cleanup is complete, release all resource
              locks.
            </p>
          }
          actionButtonTitle="Release Resource Locks"
          actionButtonEnabled={
            currentStepSection ===
            CancelReimportChecklistStepSection.RELEASE_RESOURCE_LOCKS
          }
          onActionButtonClick={async () =>
            releaseResourceLocksForStateById(
              stateCode,
              DirectIngestInstance.SECONDARY,
              currentLockStatus.secondaryLocks.map((lock) => lock.lockId)
            )
          }
          nextSection={CancelReimportChecklistStepSection.DONE}
        />
      ),
    },
  ];

  // --- step 4: done!! --------------------------------------------------------------
  const completeSteps = [
    {
      title: "Cancel Reimport Complete",
      content: (
        <StyledStepContent
          description={<p>Reimport Cancelation is complete!</p>}
          returnButton
        />
      ),
    },
  ];

  return (
    <>
      <h2>{activeChecklist}</h2>
      <Divider />
      <h3>
        <em>Flashing Status</em>:{" "}
        {isFlashInProgress ? "IN PROGRESS" : "NOT FLASHING"}
      </h3>
      <h3>
        <em>SECONDARY Lock Status</em>:{" "}
        {currentLockStatus.secondaryHeaderDescription()}
      </h3>
      <ChecklistSection
        currentStep={currentStep}
        currentStepSection={currentStepSection}
        stepSection={CancelReimportChecklistStepSection.ACQUIRE_RESOURCE_LOCKS}
        headerContents="Acquire Resource Locks"
        items={acquireResourceLocksSteps}
      />
      <ChecklistSection
        currentStep={currentStep}
        currentStepSection={currentStepSection}
        stepSection={
          CancelReimportChecklistStepSection.SECONDARY_RAW_DATA_CLEANUP
        }
        headerContents="Clean Up Raw Data and Associated Metadata in  SECONDARY"
        items={cleanUpMetadataSteps}
      />
      <ChecklistSection
        currentStep={currentStep}
        currentStepSection={currentStepSection}
        stepSection={CancelReimportChecklistStepSection.RELEASE_RESOURCE_LOCKS}
        headerContents="Release Resource Locks"
        items={releaseResourceLocksSteps}
      />
      <ChecklistSection
        currentStep={currentStep}
        currentStepSection={currentStepSection}
        stepSection={CancelReimportChecklistStepSection.DONE}
        headerContents="Reimport cancellation is complete!"
        items={completeSteps}
      />
    </>
  );
};

export default observer(CancelReimportChecklist);
