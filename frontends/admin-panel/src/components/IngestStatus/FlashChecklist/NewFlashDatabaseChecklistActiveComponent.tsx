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

import { useNewFlashChecklistStore } from "./FlashChecklistStore";
import {
  CannotFlashDecisionComponent,
  FlashReadyDecisionComponent,
} from "./FlashComponents";
import { NewFlashingChecklistType } from "./NewFlashChecklistStore";

const NewFlashDatabaseChecklistActiveComponent = (): JSX.Element => {
  // store for data that is used by child components
  const flashStore = useNewFlashChecklistStore();

  const getData = React.useCallback(async () => {
    if (flashStore && flashStore.stateInfo) {
      await flashStore.hydrate();
    }
  }, [flashStore]);

  React.useEffect(() => {
    getData();
  }, [getData]);

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
    NewFlashingChecklistType.FLASH_SECONDARY_TO_PRIMARY
  ) {
    // TODO(#33150) build the new flashing checklist here
    return <div> flashing checklist!! </div>;
  }

  if (flashStore.activeChecklist === NewFlashingChecklistType.CANCEL_REIMPORT) {
    // TODO(#33150) build the new cancel rerun checklist here
    return <div> cancel rerun checklist!! </div>;
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
          flashStore.setActiveChecklist(
            NewFlashingChecklistType.CANCEL_REIMPORT
          );
          // TODO(#33150) add cancel reimport step section here
          await flashStore.moveToNextChecklistSection(0);
        }}
      />
    );
  }

  return (
    <FlashReadyDecisionComponent
      onSelectProceed={async () => {
        flashStore.setActiveChecklist(
          NewFlashingChecklistType.FLASH_SECONDARY_TO_PRIMARY
        );
        // TODO(#33150) add flashing checklist step section here
        await flashStore.moveToNextChecklistSection(0);
      }}
      onSelectCancel={async () => {
        flashStore.setActiveChecklist(NewFlashingChecklistType.CANCEL_REIMPORT);
        // TODO(#33150) add cancel reimport step section here
        await flashStore.moveToNextChecklistSection(0);
      }}
    />
  );
};

export default observer(NewFlashDatabaseChecklistActiveComponent);
