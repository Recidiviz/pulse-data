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

import { flowResult, makeAutoObservable } from "mobx";

import {
  getOpportunities,
  getWorkflowsStateCodeInfo,
} from "../AdminPanelAPI/WorkflowsAPI";
import { StateCodeInfo } from "../components/general/constants";
import { gcpEnvironment } from "../components/Utilities/EnvironmentUtilities";
import { Hydratable, HydrationState } from "../InsightsStore/types";
import { Opportunity } from "./models/Opportunity";

export class WorkflowsStore implements Hydratable {
  hydrationState: HydrationState;

  stateCode?: string;

  stateCodeInfo?: StateCodeInfo[];

  opportunities?: Opportunity[];

  envIsStaging: boolean;

  envIsDevelopment: boolean;

  constructor() {
    makeAutoObservable(this, {}, { autoBind: true });
    this.hydrationState = { status: "needs hydration" };

    this.envIsStaging = gcpEnvironment.isStaging;
    this.envIsDevelopment = gcpEnvironment.isDevelopment;
  }

  setStateCode(stateCode: string): void {
    this.resetStateData();
    this.stateCode = stateCode;
    this.populateOpportunities();
  }

  resetStateData() {
    this.opportunities = undefined;
  }

  *populateOpportunities(): Generator<
    Promise<Opportunity[]>,
    void,
    Opportunity[]
  > {
    if (!this.stateCode) throw new Error("missing state code");
    this.opportunities = yield getOpportunities(this.stateCode);
  }

  *populateStateCodeInfo(): Generator<
    Promise<StateCodeInfo[]>,
    void,
    StateCodeInfo[]
  > {
    this.stateCodeInfo = yield getWorkflowsStateCodeInfo();
  }

  async hydrate(): Promise<void> {
    if (
      this.hydrationState.status === "hydrated" ||
      this.hydrationState.status === "loading"
    )
      return;

    try {
      this.hydrationState = { status: "loading" };
      await flowResult(this.populateStateCodeInfo());
      this.hydrationState = { status: "hydrated" };
    } catch (e) {
      this.hydrationState = { status: "failed", error: e as Error };
    }
  }
}
