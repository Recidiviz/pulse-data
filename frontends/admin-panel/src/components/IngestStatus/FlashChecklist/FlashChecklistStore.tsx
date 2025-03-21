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
  getRawDataInstanceLockStatuses,
  getStaleSecondaryRawData,
  isRawDataImportDagEnabled,
} from "../../../AdminPanelAPI/IngestOperations";
import { Hydratable, HydrationState } from "../../../InsightsStore/types";
import { StateCodeInfo } from "../../general/constants";
import { gcpEnvironment } from "../../Utilities/EnvironmentUtilities";
import {
  DirectIngestInstance,
  IngestRawFileProcessingStatus,
  RawDataDagEnabled,
} from "../constants";
import { RegionResourceLockStatus } from "../RegionResourceLockStatus";
import { CurrentRawFileProcessingStatus } from "./CurrentRawFileProcessingStatus";
import { getValueIfResolved } from "./FlashUtils";
import { StaleSecondaryStatus } from "./StaleSecondaryStatus";

export enum FlashingChecklistType {
  FLASH_SECONDARY_TO_PRIMARY = "Flash SECONDARY to PRIMARY",
  CANCEL_REIMPORT = "Canceling SECONDARY Raw Data Reimport",
}

export class FlashChecklistStore implements Hydratable {
  // --- state about the user's progress & interaction w/ flashing checklist -----------

  // the checklist type that is active (has been selected by the user)
  activeChecklist?: FlashingChecklistType;

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
  currentRawFileProcessingStatus: CurrentRawFileProcessingStatus;

  // metadata about whether or not secondary is stale
  currentStaleSecondaryStatus: StaleSecondaryStatus;

  // metadata about raw data resource locks
  currentLockStatus: RegionResourceLockStatus;

  // metadata about raw data import dag gating
  rawDataImportDagEnabled: RawDataDagEnabled;

  // --- misc properties ---------------------------------------------------------------

  abortController?: AbortController;

  projectId: string;

  constructor(stateInfo: StateCodeInfo | undefined) {
    this.stateInfo = stateInfo;

    this.hydrationState = { status: "needs hydration" };

    this.currentRawFileProcessingStatus = new CurrentRawFileProcessingStatus({
      primary: undefined,
      secondary: undefined,
    });

    this.rawDataImportDagEnabled = { primary: undefined, secondary: undefined };

    this.isFlashInProgress = false;
    this.currentStep = 0;
    this.currentStepSection = 0;
    this.currentStaleSecondaryStatus = new StaleSecondaryStatus([]);
    this.currentLockStatus = new RegionResourceLockStatus({
      primaryLocks: [],
      secondaryLocks: [],
    });

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

  setActiveChecklist(activeChecklist: FlashingChecklistType | undefined) {
    this.activeChecklist = activeChecklist;
  }

  async setCurrentStep(currentStep: number) {
    this.currentStep = currentStep;
  }

  async incrementCurrentStep() {
    this.currentStep += 1;
  }

  async resetChecklist() {
    this.setActiveChecklist(undefined);
    this.setCurrentStep(0);
    this.setCurrentStepSection(0);
    // when we set hydration state status here, useEffect in the active component
    // will pick up the change and trigger re-fetching the data
    this.setHydrationState({ status: "needs hydration" });
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
    this.currentRawFileProcessingStatus = new CurrentRawFileProcessingStatus({
      primary: currentPrimaryIngestInstanceStatus,
      secondary: currentSecondaryIngestInstanceStatus,
    });
  }

  setRawDataImportDagEnabled(primary: boolean, secondary: boolean) {
    this.rawDataImportDagEnabled = { primary, secondary };
  }

  setIsFlashInProgress(isFlashInProgress: boolean) {
    this.isFlashInProgress = isFlashInProgress;
  }

  setRegionResourceLockStatus(primaryLocks: [], secondaryLocks: []) {
    this.currentLockStatus = new RegionResourceLockStatus({
      primaryLocks,
      secondaryLocks,
    });
  }

  setCurrentStaleSecondaryStatus(currentStaleSecondaryStatus: string[]) {
    this.currentStaleSecondaryStatus = new StaleSecondaryStatus(
      currentStaleSecondaryStatus
    );
  }

  async fetchIsFlashInProgress() {
    if (this.stateInfo) {
      const flashStatus = await getIsFlashingInProgress(this.stateInfo.code);
      this.setIsFlashInProgress(await flashStatus.json());
    }
  }

  async fetchResourceLockStatus() {
    if (this.stateInfo) {
      const statusResults = await Promise.allSettled([
        getRawDataInstanceLockStatuses(
          this.stateInfo.code,
          DirectIngestInstance.PRIMARY
        ),
        getRawDataInstanceLockStatuses(
          this.stateInfo.code,
          DirectIngestInstance.SECONDARY
        ),
      ]);
      this.setRegionResourceLockStatus(
        await getValueIfResolved(statusResults[0])?.json(),
        await getValueIfResolved(statusResults[1])?.json()
      );
    }
  }

  async fetchRawDataImportDagEnabled() {
    if (this.stateInfo) {
      const enabledResults = await Promise.allSettled([
        isRawDataImportDagEnabled(
          this.stateInfo.code,
          DirectIngestInstance.PRIMARY
        ),
        isRawDataImportDagEnabled(
          this.stateInfo.code,
          DirectIngestInstance.SECONDARY
        ),
      ]);
      this.setRawDataImportDagEnabled(
        await getValueIfResolved(enabledResults[0])?.json(),
        await getValueIfResolved(enabledResults[1])?.json()
      );
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
      this.currentRawFileProcessingStatus.areIngestBucketsEmpty() &&
      this.currentRawFileProcessingStatus.hasProcessedFilesInSecondary() &&
      !this.currentRawFileProcessingStatus.hasUnprocessedFilesInSecondary() &&
      !this.currentStaleSecondaryStatus.isStale()
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

      // only fetched on initial hydration
      await this.fetchRawDataImportDagEnabled();
      await this.fetchRawFileProcessingStatus();
      await this.fetchStaleSecondaryStatus();

      // expected to change throughout the course of the checklist, so we will fetch
      // both on initial hydration as well after each step
      await this.fetchIsFlashInProgress();
      await this.fetchResourceLockStatus();

      if (this.isFlashInProgress) {
        this.setActiveChecklist(
          FlashingChecklistType.FLASH_SECONDARY_TO_PRIMARY
        );
      }

      this.setHydrationState({ status: "hydrated" });
    } catch (e) {
      this.setHydrationState({ status: "failed", error: e as Error });
    }
  }
}
