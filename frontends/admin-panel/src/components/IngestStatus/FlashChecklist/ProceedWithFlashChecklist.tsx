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

import { Col, Layout, Row, Spin } from "antd";
import { observer } from "mobx-react-lite";

import {
  acquireResourceLocksForStateAndInstance,
  copyRawDataBetweenInstances,
  copyRawDataToBackup,
  deleteContentsOfRawDataTables,
  deleteTablesInPruningDatasets,
  invalidateIngestPipelineRuns,
  markInstanceRawDataInvalidated,
  releaseResourceLocksForStateById,
  transferRawDataMetadataToNewInstance,
  triggerCalculationDAG,
  updateIsFlashingInProgress,
} from "../../../AdminPanelAPI/IngestOperations";
import { DirectIngestInstance } from "../constants";
import { useFlashChecklistStore } from "./FlashChecklistContext";
import StyledStepContent, {
  ChecklistSection,
  CodeBlock,
} from "./FlashComponents";
import {
  flashingLockSecondsTtl,
  flashSecondaryLockDescription,
} from "./FlashUtils";

export const FlashChecklistStepSection = {
  /* Ordered list of sections in the flash checklist.
  NOTE: The relative order of these steps is important.
  IF YOU ADD A NEW STEP SECTION,
  you MUST add it in the relative order to other sections. */
  ACQUIRE_RESOURCE_LOCKS: 0,
  START_FLASH: 1,
  PRIMARY_RAW_DATA_DEPRECATION: 2,
  FLASH_RAW_DATA_TO_PRIMARY: 3,
  SECONDARY_RAW_DATA_CLEANUP: 4,
  END_FLASH: 5,
  RELEASE_RESOURCE_LOCKS: 6,
  TRIGGER_PIPELINES: 7,
  DONE: 8,
};

const ProceedWithFlashChecklist = (): JSX.Element => {
  const {
    stateInfo,
    currentStep,
    currentStepSection,
    projectId,
    isReadyToFlash,
    isFlashInProgress,
    currentLockStatus,
  } = useFlashChecklistStore();
  if (!stateInfo) {
    return <Spin />;
  }

  const stateCode = stateInfo.code;

  const secondaryRawDataDataset = `${stateCode.toLowerCase()}_raw_data_secondary`;
  const primaryRawDataDataset = `${stateCode.toLowerCase()}_raw_data`;

  // --- step 1: get resource locks ----------------------------------------------------
  const acquireResourceLocksStep = [
    {
      title: "Acquire PRIMARY Resource Locks",
      content: (
        <StyledStepContent
          description={<p>Acquire PRIMARY resource locks for {stateCode}.</p>}
          actionButtonTitle="Acquire PRIMARY Locks"
          actionButtonEnabled={
            isReadyToFlash() && currentLockStatus.allPrimaryLocksFree()
          }
          onActionButtonClick={async () =>
            acquireResourceLocksForStateAndInstance(
              stateCode,
              DirectIngestInstance.PRIMARY,
              flashSecondaryLockDescription,
              flashingLockSecondsTtl
            )
          }
        />
      ),
    },
    {
      title: "Acquire SECONDARY Resource Locks",
      content: (
        <StyledStepContent
          description={<p>Acquire SECONDARY resource locks for {stateCode}.</p>}
          actionButtonTitle="Acquire SECONDARY Locks"
          actionButtonEnabled={
            isReadyToFlash() && currentLockStatus.allSecondaryLocksFree()
          }
          onActionButtonClick={async () =>
            acquireResourceLocksForStateAndInstance(
              stateCode,
              DirectIngestInstance.SECONDARY,
              flashSecondaryLockDescription,
              flashingLockSecondsTtl
            )
          }
          nextSection={FlashChecklistStepSection.START_FLASH}
        />
      ),
    },
  ];
  //
  // --- step 2: start flash ---------------------------------------------------------
  const startFlashSteps = [
    {
      title: "Set is flashing in progress to true",
      content: (
        <StyledStepContent
          description={
            <p>Set is flashing in progress to true in {stateCode}</p>
          }
          actionButtonTitle="Set is flashing in progress"
          actionButtonEnabled={
            isReadyToFlash() && currentLockStatus.allLocksHeldByAdHoc()
          }
          onActionButtonClick={async () =>
            updateIsFlashingInProgress(stateCode, true)
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
              <code>{stateCode.toLowerCase()}_new_pruned_raw_data</code> and{" "}
              <code>
                {stateCode.toLowerCase()}
                _raw_data_pruning_diff_results
              </code>
            </p>
          }
          actionButtonEnabled={isFlashInProgress}
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
              Mark all <code>PRIMARY</code> instance rows in
              <code>direct_ingest_raw_big_query_file_metadata</code> as
              invalidated.
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
              Update all rows in the following tables that had instance
              <code> SECONDARY</code> with updated instance <code>PRIMARY</code>
              :
              <ul>
                <li>
                  <code>direct_ingest_raw_big_query_file_metadata</code>
                </li>
                <li>
                  <code>direct_ingest_raw_gcs_file_metadata</code>
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
              Use the command below within the <code>pipenv shell</code> to move
              SECONDARY storage raw files to deprecated
              <CodeBlock
                enabled={
                  currentStepSection ===
                  FlashChecklistStepSection.FLASH_RAW_DATA_TO_PRIMARY
                }
              >
                python -m recidiviz.tools.ingest.operations.deprecate_raw_data
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
          actionButtonEnabled={isFlashInProgress}
          actionButtonTitle="Clean up SECONDARY raw data"
          onActionButtonClick={async () =>
            deleteContentsOfRawDataTables(
              stateCode,
              DirectIngestInstance.SECONDARY
            )
          }
          nextSection={FlashChecklistStepSection.END_FLASH}
        />
      ),
    },
  ];
  // --- step 6: finalize flash ------------------------------------------------------
  const finalizeFlashSteps = [
    {
      title: "Set is flashing in progress false",
      content: (
        <StyledStepContent
          description={
            <p>Set is flashing in progress to false in {stateCode}</p>
          }
          actionButtonEnabled={isFlashInProgress}
          actionButtonTitle="Set flashing not in progress"
          onActionButtonClick={async () =>
            updateIsFlashingInProgress(stateCode, false)
          }
          nextSection={FlashChecklistStepSection.RELEASE_RESOURCE_LOCKS}
        />
      ),
    },
  ];
  // --- step 7: release resource locks ------------------------------------------------
  const releaseResourceLocksSteps = [
    {
      title: "Release SECONDARY Resource Locks",
      content: (
        <StyledStepContent
          description={
            <p>
              Now that the database cleanup is complete, release SECONDARY
              resource locks.
            </p>
          }
          actionButtonTitle="Release SECONDARY Locks"
          actionButtonEnabled={
            currentStepSection ===
            FlashChecklistStepSection.RELEASE_RESOURCE_LOCKS
          }
          onActionButtonClick={async () =>
            releaseResourceLocksForStateById(
              stateCode,
              DirectIngestInstance.SECONDARY,
              currentLockStatus.secondaryLocks.map((lock) => lock.lockId)
            )
          }
        />
      ),
    },
    {
      title: "Release PRIMARY Resource Locks",
      content: (
        <StyledStepContent
          description={
            <p>
              Now that the database cleanup is complete, release PRIMARY
              resource locks.
            </p>
          }
          actionButtonTitle="Release PRIMARY Locks"
          actionButtonEnabled={
            currentStepSection ===
            FlashChecklistStepSection.RELEASE_RESOURCE_LOCKS
          }
          onActionButtonClick={async () =>
            releaseResourceLocksForStateById(
              stateCode,
              DirectIngestInstance.PRIMARY,
              currentLockStatus.primaryLocks.map((lock) => lock.lockId)
            )
          }
          nextSection={FlashChecklistStepSection.TRIGGER_PIPELINES}
        />
      ),
    },
  ];
  // --- step 8: trigger pipelines ---------------------------------------------------
  const triggerPipelinesSteps = [
    {
      title: "Re-run Ingest & Calc in PRIMARY",
      content: (
        <StyledStepContent
          description={
            <p>
              Now that we have new raw data in PRIMARY, we should make sure that
              data is propagated through ingest, normalization & metrics to
              product exports and views for
              {stateCode} in <code>PRIMARY</code>.
            </p>
          }
          actionButtonTitle="Start Calculation DAG Run"
          actionButtonEnabled
          onActionButtonClick={async () => triggerCalculationDAG()}
          nextSection={FlashChecklistStepSection.DONE}
        />
      ),
    },
  ];

  // --- step 9: done!! --------------------------------------------------------------
  const completeSteps = [
    {
      title: "Flash Complete",
      content: (
        <StyledStepContent
          description={<p>Flashing Secondary to PRIMARY is complete!</p>}
          returnButton
        />
      ),
    },
  ];

  return (
    <Layout>
      <Layout.Header
        style={{
          position: "sticky",
          top: "0px",
          float: "left",
          height: "200px",
          zIndex: 1,
        }}
      >
        <div style={{ lineHeight: "1.5" }}>
          <h3>Proceeding with Flash from SECONDARY to PRIMARY</h3>
          <br />
          <Row gutter={{ xs: 1, sm: 2, md: 2, lg: 2 }} justify="space-evenly">
            <Col className="gutter-row" span={8}>
              <em>Is Flashing In Progress Status</em>:{" "}
              <span style={{ color: isFlashInProgress ? "green" : "red" }}>
                {isFlashInProgress ? "IN PROGRESS" : "NOT FLASHING"}
              </span>
            </Col>
            <Col className="gutter-row" span={8}>
              <em>PRIMARY Lock Status</em>:{" "}
              {currentLockStatus.primaryHeaderDescription()}
            </Col>
            <Col className="gutter-row" span={8}>
              <em>SECONDARY Lock Status</em>:{" "}
              {currentLockStatus.secondaryHeaderDescription()}
            </Col>
          </Row>
        </div>
      </Layout.Header>
      <Layout.Content style={{ zIndex: 0 }}>
        <ChecklistSection
          currentStep={currentStep}
          currentStepSection={currentStepSection}
          stepSection={FlashChecklistStepSection.ACQUIRE_RESOURCE_LOCKS}
          headerContents="Pause Operations"
          items={acquireResourceLocksStep}
        />
        <ChecklistSection
          currentStep={currentStep}
          currentStepSection={currentStepSection}
          stepSection={FlashChecklistStepSection.START_FLASH}
          headerContents="Start Flash"
          items={startFlashSteps}
        />
        <ChecklistSection
          currentStep={currentStep}
          currentStepSection={currentStepSection}
          stepSection={FlashChecklistStepSection.PRIMARY_RAW_DATA_DEPRECATION}
          headerContents="Deprecate Raw Data and Associated Metadata"
          items={deprecateDataAndMetadataSteps}
        />
        <ChecklistSection
          currentStep={currentStep}
          currentStepSection={currentStepSection}
          stepSection={FlashChecklistStepSection.FLASH_RAW_DATA_TO_PRIMARY}
          headerContents="Flash Raw Data to PRIMARY"
          items={executeFlashSteps}
        />
        <ChecklistSection
          currentStep={currentStep}
          currentStepSection={currentStepSection}
          stepSection={FlashChecklistStepSection.SECONDARY_RAW_DATA_CLEANUP}
          headerContents="Clean Up Raw Data in SECONDARY"
          items={cleanUpSteps}
        />
        <ChecklistSection
          currentStep={currentStep}
          currentStepSection={currentStepSection}
          stepSection={FlashChecklistStepSection.END_FLASH}
          headerContents="Finalize Flash"
          items={finalizeFlashSteps}
        />
        <ChecklistSection
          currentStep={currentStep}
          currentStepSection={currentStepSection}
          stepSection={FlashChecklistStepSection.RELEASE_RESOURCE_LOCKS}
          headerContents="Resume Operations"
          items={releaseResourceLocksSteps}
        />
        <ChecklistSection
          currentStep={currentStep}
          currentStepSection={currentStepSection}
          stepSection={FlashChecklistStepSection.TRIGGER_PIPELINES}
          headerContents="Trigger Pipelines"
          items={triggerPipelinesSteps}
        />
        <ChecklistSection
          currentStep={currentStep}
          currentStepSection={currentStepSection}
          stepSection={FlashChecklistStepSection.DONE}
          headerContents="Flash is complete"
          items={completeSteps}
        />
      </Layout.Content>
    </Layout>
  );
};

export default observer(ProceedWithFlashChecklist);
