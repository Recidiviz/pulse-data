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

import { makeAutoObservable } from "mobx";

import { getIngestRawFileProcessingStatus } from "../../../AdminPanelAPI";
import {
  getIsFlashingInProgress,
  getStaleSecondaryRawData,
} from "../../../AdminPanelAPI/IngestOperations";
import { Hydratable, HydrationState } from "../../../InsightsStore/types";
import { StateCodeInfo } from "../../general/constants";
import { gcpEnvironment } from "../../Utilities/EnvironmentUtilities";
import {
  DirectIngestInstance,
  IngestRawFileProcessingStatus,
} from "../constants";
import { getValueIfResolved } from "./FlashUtils";
import { NewCurrentRawFileProcessingStatus } from "./NewCurrentRawFileProcessingStatus";

export enum NewFlashingChecklistType {
  FLASH_SECONDARY_TO_PRIMARY = "Flash SECONDARY to PRIMARY",
  CANCEL_REIMPORT = "Canceling SECONDARY Raw Data Reimport",
}

export class NewFlashChecklistStore implements Hydratable {
  // --- state about the user's progress & interaction w/ flashing checklist -----------

  // the checklist type that is active (has been selected by the user)
  activeChecklist?: NewFlashingChecklistType;

  // current step section for the active flashing checklist
  currentStepSection: number;

  // current step for the current step section
  currentStep: number;

  // state info, as selected in StateSelector. will be undefined until the user selects
  // a state
  stateInfo?: StateCodeInfo;

  // --- state about the raw data infrastructure & data --------------------------------

  // hydration state of the properties below
  hydrationState: HydrationState;

  // the most recent row in direct_ingest_raw_data_flash_status table
  isFlashInProgress: boolean;

  // metadata about raw data file processing status for both primary and secondary
  currentRawFileProcessingStatus: NewCurrentRawFileProcessingStatus;

  // metadata about whether or not secondary is stale
  currentStaleSecondaryStatus: string[];

  // --- misc properties ---------------------------------------------------------------

  abortController?: AbortController;

  projectId: string;

  constructor(stateInfo: StateCodeInfo | undefined) {
    this.stateInfo = stateInfo;

    this.hydrationState = { status: "needs hydration" };

    this.currentRawFileProcessingStatus = new NewCurrentRawFileProcessingStatus(
      {
        primary: undefined,
        secondary: undefined,
      }
    );

    this.isFlashInProgress = false;
    this.currentStep = 0;
    this.currentStepSection = 0;
    this.currentStaleSecondaryStatus = [];

    this.projectId = gcpEnvironment.isProduction
      ? "recidiviz-123"
      : "recidiviz-staging";

    makeAutoObservable(this, { abortController: false }, { autoBind: true });
  }

  // setters

  setHydrationState(hydrationState: HydrationState) {
    this.hydrationState = hydrationState;
  }

  setStateInfo(stateInfo: StateCodeInfo) {
    this.stateInfo = stateInfo;
  }

  setActiveChecklist(activeChecklist: NewFlashingChecklistType | undefined) {
    this.activeChecklist = activeChecklist;
  }

  async setCurrentStep(currentStep: number) {
    this.currentStep = currentStep;
  }

  async incrementCurrentStep() {
    this.currentStep += 1;
  }

  async setCurrentStepSection(currentStepSection: number) {
    this.currentStepSection = currentStepSection;
  }

  async moveToNextChecklistSection(nextSection: number) {
    this.currentStepSection = nextSection;
    this.setCurrentStep(0);
  }

  setCurrentRawFileProcessingStatus(
    currentPrimaryIngestInstanceStatus:
      | IngestRawFileProcessingStatus[]
      | undefined,
    currentSecondaryIngestInstanceStatus:
      | IngestRawFileProcessingStatus[]
      | undefined
  ) {
    this.currentRawFileProcessingStatus = new NewCurrentRawFileProcessingStatus(
      {
        primary: currentPrimaryIngestInstanceStatus,
        secondary: currentSecondaryIngestInstanceStatus,
      }
    );
  }

  setIsFlashInProgress(isFlashInProgress: boolean) {
    this.isFlashInProgress = isFlashInProgress;
  }

  setCurrentStaleSecondaryStatus(currentStaleSecondaryStatus: string[]) {
    this.currentStaleSecondaryStatus = currentStaleSecondaryStatus;
  }

  async fetchIsFlashInProgress() {
    if (this.stateInfo) {
      const flashStatus = await getIsFlashingInProgress(this.stateInfo.code);
      this.setIsFlashInProgress(await flashStatus.json());
    }
  }

  async fetchStaleSecondaryStatus() {
    if (this.stateInfo) {
      const staleSecondary = await getStaleSecondaryRawData(
        this.stateInfo.code
      );
      this.setCurrentStaleSecondaryStatus(await staleSecondary.json());
    }
  }

  async fetchRawFileProcessingStatus() {
    if (this.stateInfo) {
      if (this.abortController) {
        this.abortController.abort();
        this.abortController = undefined;
      }
      this.abortController = new AbortController();
      const results = await Promise.allSettled([
        getIngestRawFileProcessingStatus(
          this.stateInfo.code,
          DirectIngestInstance.PRIMARY,
          this.abortController
        ),
        getIngestRawFileProcessingStatus(
          this.stateInfo.code,
          DirectIngestInstance.SECONDARY,
          this.abortController
        ),
      ]);
      this.setCurrentRawFileProcessingStatus(
        await getValueIfResolved(results[0])?.json(),
        await getValueIfResolved(results[1])?.json()
      );
    }
  }

  isReadyToFlash() {
    if (
      this.currentStaleSecondaryStatus === undefined ||
      this.isFlashInProgress === undefined
    )
      return false;

    return (
      !this.isFlashInProgress &&
      this.currentRawFileProcessingStatus.hasProcessedFilesInSecondary() &&
      !this.currentRawFileProcessingStatus.hasUnprocessedFilesInSecondary() &&
      this.currentStaleSecondaryStatus.length === 0
    );
  }

  async hydrate() {
    if (
      this.hydrationState.status === "hydrated" ||
      this.hydrationState.status === "loading"
    )
      return;
    try {
      this.setHydrationState({ status: "loading" });
      // Raw data source instance and ingest bucket processing statuses,
      // are only fetched upon initial page load after a state is set.
      await this.fetchRawFileProcessingStatus();

      // Fetch stale secondary status, or all non-invalidated files in primary that
      // have a discovery date after the earliest non-invalidated secondary discovery
      // date that are not present in secondary
      await this.fetchStaleSecondaryStatus();

      // Ingest instance status information is fetched upon
      // initial page load after a state is set, and each
      // after each step is completed.
      await this.fetchIsFlashInProgress();
      if (this.isFlashInProgress) {
        this.setActiveChecklist(
          NewFlashingChecklistType.FLASH_SECONDARY_TO_PRIMARY
        );
      }

      this.setHydrationState({ status: "hydrated" });
    } catch (e) {
      this.setHydrationState({ status: "failed", error: e as Error });
    }
  }
}
