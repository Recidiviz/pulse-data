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

import CancelReimportChecklist, {
  CancelReimportChecklistStepSection,
} from "./CancelReimportChecklist";
import { useFlashChecklistStore } from "./FlashChecklistContext";
import { FlashingChecklistType } from "./FlashChecklistStore";
import {
  CannotFlashDecisionComponent,
  FlashReadyDecisionComponent,
} from "./FlashComponents";
import ProceedWithFlashChecklist from "./ProceedWithFlashChecklist";

const FlashDatabaseChecklistActiveComponent = (): JSX.Element => {
  // store for data that is used by child components
  const flashStore = useFlashChecklistStore();

  const getData = React.useCallback(async () => {
    if (
      flashStore &&
      flashStore.stateInfo &&
      flashStore.hydrationState.status === "needs hydration"
    ) {
      await flashStore.hydrate();
    }
  }, [flashStore]);

  React.useEffect(() => {
    getData();
  }, [getData, flashStore.hydrationState.status]);

  if (
    flashStore.hydrationState.status === "needs hydration" ||
    flashStore.hydrationState.status === "loading"
  ) {
    return <Spin />;
  }
  if (flashStore.hydrationState.status === "failed") {
    return <Alert message={flashStore.hydrationState.error} />;
  }
  if (flashStore.hydrationState.status !== "hydrated") {
    return (
      <Alert
        message={`Found an unexpected hydrationState: ${flashStore.hydrationState} `}
      />
    );
  }

  if (
    flashStore.activeChecklist ===
    FlashingChecklistType.FLASH_SECONDARY_TO_PRIMARY
  ) {
    return <ProceedWithFlashChecklist />;
  }

  if (flashStore.activeChecklist === FlashingChecklistType.CANCEL_REIMPORT) {
    return <CancelReimportChecklist />;
  }

  if (
    flashStore.activeChecklist !== undefined ||
    flashStore.isFlashInProgress
  ) {
    return (
      <div>
        An Error Occurred: we found an unexpected activeChecklist type or
        flashing was in progress without activeChecklist being set
      </div>
    );
  }

  if (!flashStore.isReadyToFlash()) {
    return (
      <CannotFlashDecisionComponent
        onSelectProceed={async () => {
          flashStore.setActiveChecklist(FlashingChecklistType.CANCEL_REIMPORT);
          await flashStore.moveToNextChecklistSection(
            CancelReimportChecklistStepSection.ACQUIRE_RESOURCE_LOCKS
          );
        }}
      />
    );
  }

  return (
    <FlashReadyDecisionComponent
      onSelectProceed={async () => {
        flashStore.setActiveChecklist(
          FlashingChecklistType.FLASH_SECONDARY_TO_PRIMARY
        );
        await flashStore.moveToNextChecklistSection(0);
      }}
      onSelectCancel={async () => {
        flashStore.setActiveChecklist(FlashingChecklistType.CANCEL_REIMPORT);
        await flashStore.moveToNextChecklistSection(
          CancelReimportChecklistStepSection.ACQUIRE_RESOURCE_LOCKS
        );
      }}
    />
  );
};

export default observer(FlashDatabaseChecklistActiveComponent);
