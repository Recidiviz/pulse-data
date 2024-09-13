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
import { Spin } from "antd";
import { observer } from "mobx-react-lite";
import * as React from "react";

import { useLegacyFlashChecklistStore } from "./FlashChecklistStore";
import LegacyStateCancelReimportChecklist, {
  LegacyCancelReimportChecklistStepSection,
} from "./LegacyCancelReimportChecklist";
import {
  CannotFlashDecisionNonEmptyBucketComponent,
  CannotFlashDecisionWrongStatusComponent,
  FlashReadyDecisionComponent,
} from "./LegacyFlashComponents";
import LegacyStateProceedWithFlashChecklist, {
  LegacyFlashChecklistStepSection,
} from "./LegacyProceedWithFlashChecklist";

const LegacyFlashDatabaseChecklistActiveComponent = (): JSX.Element => {
  // store for data that is used by child components
  const flashStore = useLegacyFlashChecklistStore();

  const [dataLoading, setDataLoading] = React.useState<boolean>(true);

  const getData = React.useCallback(async () => {
    if (flashStore && flashStore.stateInfo) {
      setDataLoading(true);

      // Raw data source instance and ingest bucket processing statuses,
      // are only fetched upon initial page load after a state is set.
      await flashStore.fetchRawFileProcessingStatus();

      // Ingest instance status information is fetched upon
      // initial page load after a state is set, and each
      // after each step is completed.
      await flashStore.fetchLegacyInstanceStatusData();

      setDataLoading(false);
    }
  }, [flashStore]);

  React.useEffect(() => {
    getData();
  }, [getData]);

  if (
    dataLoading ||
    !flashStore ||
    !flashStore.stateInfo ||
    flashStore.legacyCurrentRawDataInstanceStatus.primary === undefined
  ) {
    return <Spin />;
  }
  if (
    !flashStore.currentRawFileProcessingStatus.emptyIngestBuckets &&
    flashStore.proceedWithFlash === undefined
  ) {
    /* this state means that we the ingest buckets are not yet cleaned so we cannot */
    /* yet proceed with flash! */
    return (
      <CannotFlashDecisionNonEmptyBucketComponent
        onSelectProceed={async () => {
          flashStore.setProceedWithFlash(false);
          await flashStore.moveToNextChecklistSection(
            LegacyCancelReimportChecklistStepSection.PAUSE_OPERATIONS
          );
        }}
      />
    );
  }
  if (
    !flashStore.legacyCurrentRawDataInstanceStatus.isReadyToFlash() &&
    !flashStore.legacyCurrentRawDataInstanceStatus.isFlashInProgress() &&
    !flashStore.legacyCurrentRawDataInstanceStatus.isReimportCancellationInProgress() &&
    flashStore.proceedWithFlash === undefined &&
    // This check makes it so we don't show the "can't flash" component
    // when you set the status to FLASH_COMPLETE in the middle of the checklist.
    flashStore.currentStep === 0
  ) {
    /* If we have loaded a status but it does not indicate that we can proceed with flashing, show an alert on top of the checklist */
    /* Regardless of status, we can cancel a secondary rerun any time */
    return (
      <CannotFlashDecisionWrongStatusComponent
        currentIngestStatus={flashStore.legacyCurrentRawDataInstanceStatus}
        onSelectProceed={async () => {
          flashStore.setProceedWithFlash(false);
          await flashStore.moveToNextChecklistSection(
            LegacyCancelReimportChecklistStepSection.PAUSE_OPERATIONS
          );
        }}
      />
    );
  }
  if (
    flashStore.proceedWithFlash === undefined &&
    flashStore.legacyCurrentRawDataInstanceStatus.isReadyToFlash()
  ) {
    return (
      /* This is the only time that someone can choose whether to cancel a rerun or
      move forward with a flash. */
      <FlashReadyDecisionComponent
        onSelectProceed={async () => {
          flashStore.setProceedWithFlash(true);
          await flashStore.moveToNextChecklistSection(
            LegacyFlashChecklistStepSection.PAUSE_OPERATIONS
          );
        }}
        onSelectCancel={async () => {
          flashStore.setProceedWithFlash(false);
          await flashStore.moveToNextChecklistSection(
            LegacyCancelReimportChecklistStepSection.PAUSE_OPERATIONS
          );
        }}
      />
    );
  }
  if (
    flashStore.proceedWithFlash ||
    flashStore.legacyCurrentRawDataInstanceStatus.isFlashInProgress()
  ) {
    /* In the case of a refresh in the middle of a flash, proceedWithFlash
    will get reset. Set the value back to true, since a flash is already in
    progress. */
    if (flashStore.proceedWithFlash === undefined) {
      flashStore.setProceedWithFlash(true);
    }
    return (
      /* This covers when a decision has been made to
      proceed with a flash from SECONDARY to PRIMARY */
      <LegacyStateProceedWithFlashChecklist />
    );
  }
  if (
    !flashStore.proceedWithFlash ||
    flashStore.legacyCurrentRawDataInstanceStatus.isReimportCancellationInProgress()
  ) {
    return (
      /* If decision has been made to cancel a rerun in SECONDARY */
      <LegacyStateCancelReimportChecklist />
    );
  }
  return <div>Error!</div>;
};

export default observer(LegacyFlashDatabaseChecklistActiveComponent);
