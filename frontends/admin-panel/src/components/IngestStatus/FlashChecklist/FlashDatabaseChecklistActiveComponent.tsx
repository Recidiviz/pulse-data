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
import { Alert, Spin } from "antd";
import { observer } from "mobx-react-lite";
import * as React from "react";

import { StateCodeInfo } from "../../general/constants";
import StateCancelReimportChecklist, {
  CancelReimportChecklistStepSection,
} from "./CancelReimportChecklist";
import { FlashChecklistContext } from "./FlashChecklistStore";
import {
  CannotFlashDecisionNonEmptyBucketComponent,
  CannotFlashDecisionWrongStatusComponent,
  FlashReadyDecisionComponent,
} from "./FlashComponents";
import StateProceedWithFlashChecklist, {
  FlashChecklistStepSection,
} from "./ProceedWithFlashChecklist";

interface FlashDatabaseChecklistActiveComponentProps {
  stateInfo: StateCodeInfo | undefined;
}

const FlashDatabaseChecklistActiveComponent = ({
  stateInfo,
}: FlashDatabaseChecklistActiveComponentProps): JSX.Element => {
  // store for data that is used by child components
  const flashStore = React.useContext(FlashChecklistContext);

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
      await flashStore.fetchInstanceStatusData();

      setDataLoading(false);
    }
  }, [flashStore]);

  React.useEffect(() => {
    getData();
  }, [getData]);

  if (stateInfo === undefined) {
    return (
      <Alert
        message="Select a state"
        description="Once you pick a state, this form will display the set of instructions required to flash a secondary database to primary."
        type="info"
        showIcon
      />
    );
  }
  if (
    dataLoading ||
    !flashStore ||
    flashStore.currentRawDataInstanceStatus.primary === undefined
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
            CancelReimportChecklistStepSection.PAUSE_OPERATIONS
          );
        }}
      />
    );
  }
  if (
    !flashStore.currentRawDataInstanceStatus.isReadyToFlash() &&
    !flashStore.currentRawDataInstanceStatus.isFlashInProgress() &&
    !flashStore.currentRawDataInstanceStatus.isReimportCancellationInProgress() &&
    flashStore.proceedWithFlash === undefined &&
    // This check makes it so we don't show the "can't flash" component
    // when you set the status to FLASH_COMPLETE in the middle of the checklist.
    flashStore.currentStep === 0
  ) {
    /* If we have loaded a status but it does not indicate that we can proceed with flashing, show an alert on top of the checklist */
    /* Regardless of status, we can cancel a secondary rerun any time */
    return (
      <CannotFlashDecisionWrongStatusComponent
        currentIngestStatus={flashStore.currentRawDataInstanceStatus}
        onSelectProceed={async () => {
          flashStore.setProceedWithFlash(false);
          await flashStore.moveToNextChecklistSection(
            CancelReimportChecklistStepSection.PAUSE_OPERATIONS
          );
        }}
      />
    );
  }
  if (
    flashStore.proceedWithFlash === undefined &&
    flashStore.currentRawDataInstanceStatus.isReadyToFlash()
  ) {
    return (
      /* This is the only time that someone can choose whether to cancel a rerun or
      move forward with a flash. */
      <FlashReadyDecisionComponent
        onSelectProceed={async () => {
          flashStore.setProceedWithFlash(true);
          await flashStore.moveToNextChecklistSection(
            FlashChecklistStepSection.PAUSE_OPERATIONS
          );
        }}
        onSelectCancel={async () => {
          flashStore.setProceedWithFlash(false);
          await flashStore.moveToNextChecklistSection(
            CancelReimportChecklistStepSection.PAUSE_OPERATIONS
          );
        }}
      />
    );
  }
  if (
    flashStore.proceedWithFlash ||
    flashStore.currentRawDataInstanceStatus.isFlashInProgress()
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
      <StateProceedWithFlashChecklist stateCode={stateInfo.code} />
    );
  }
  if (
    !flashStore.proceedWithFlash ||
    flashStore.currentRawDataInstanceStatus.isReimportCancellationInProgress()
  ) {
    return (
      /* If decision has been made to cancel a rerun in SECONDARY */
      <StateCancelReimportChecklist stateCode={stateInfo.code} />
    );
  }
  return <div>Error!</div>;
};

export default observer(FlashDatabaseChecklistActiveComponent);
