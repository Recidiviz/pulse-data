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

import { message } from "antd";
import { makeAutoObservable } from "mobx";
import { createContext, useContext } from "react";

import { getIngestRawFileProcessingStatus } from "../../../AdminPanelAPI";
import { StateCodeInfo } from "../../general/constants";
import { gcpEnvironment } from "../../Utilities/EnvironmentUtilities";
import { fetchCurrentIngestInstanceStatus } from "../../Utilities/IngestInstanceUtilities";
import {
  DirectIngestInstance,
  IngestRawFileProcessingStatus,
  IngestStatus,
} from "../constants";
import { CurrentRawDataInstanceStatus } from "./CurrentRawDataInstanceStatus";
import { CurrentRawFileProcessingStatus } from "./CurrentRawFileProcessingStatus";

function getValueIfResolved<Value>(
  result: PromiseSettledResult<Value>
): Value | undefined {
  return result.status === "fulfilled" ? result.value : undefined;
}

export class FlashChecklistStore {
  // optionally selected state code
  stateInfo?: StateCodeInfo;

  projectId: string;

  // current step section for the active flashing checklist
  currentStepSection: number;

  // current step for the current step section
  currentStep: number;

  currentRawDataInstanceStatus: CurrentRawDataInstanceStatus;

  currentRawFileProcessingStatus: CurrentRawFileProcessingStatus;

  proceedWithFlash?: boolean;

  abortController?: AbortController;

  constructor(stateInfo: StateCodeInfo | undefined) {
    this.stateInfo = stateInfo;

    this.currentRawDataInstanceStatus = new CurrentRawDataInstanceStatus({
      primary: undefined,
      secondary: undefined,
    });

    this.currentRawFileProcessingStatus = new CurrentRawFileProcessingStatus({
      primary: undefined,
      secondary: undefined,
    });

    this.currentStep = 0;
    this.currentStepSection = 0;

    this.projectId = gcpEnvironment.isProduction
      ? "recidiviz-123"
      : "recidiviz-staging";

    makeAutoObservable(this, { abortController: false }, { autoBind: true });
  }

  // instance methods

  setStateInfo(stateInfo: StateCodeInfo) {
    this.stateInfo = stateInfo;
  }

  setProceedWithFlash(proceedWithFlash: boolean) {
    this.proceedWithFlash = proceedWithFlash;
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

  setCurrentIngestInstanceStatus(
    currentPrimaryIngestInstanceStatus: string | undefined,
    currentSecondaryIngestInstanceStatus: string | undefined
  ) {
    this.currentRawDataInstanceStatus.primary =
      currentPrimaryIngestInstanceStatus as IngestStatus;
    this.currentRawDataInstanceStatus.secondary =
      currentSecondaryIngestInstanceStatus as IngestStatus;
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

  async fetchInstanceStatusData() {
    if (this.stateInfo) {
      try {
        const statusResults = await Promise.allSettled([
          fetchCurrentIngestInstanceStatus(
            this.stateInfo.code,
            DirectIngestInstance.PRIMARY
          ),
          fetchCurrentIngestInstanceStatus(
            this.stateInfo.code,
            DirectIngestInstance.SECONDARY
          ),
        ]);
        this.setCurrentIngestInstanceStatus(
          getValueIfResolved(statusResults[0]),
          getValueIfResolved(statusResults[1])
        );
      } catch (err) {
        message.error(`An error occurred: ${err}`);
      }
    }
  }

  async fetchRawFileProcessingStatus() {
    if (this.stateInfo) {
      if (this.abortController) {
        this.abortController.abort();
        this.abortController = undefined;
      }
      try {
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
      } catch (err) {
        message.error(`An error occurred: ${err}`);
      }
    }
  }
}

export const FlashChecklistContext = createContext<
  FlashChecklistStore | undefined
>(undefined);

export function useFlashChecklistStore(): FlashChecklistStore {
  const context = useContext(FlashChecklistContext);
  if (context === undefined) {
    throw new Error("Must call FlashChecklistContext within a valid provider");
  }
  return context;
}
