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

import { autorun, flowResult, makeAutoObservable } from "mobx";

import { Hydratable, HydrationState } from "../../InsightsStore/types";
import { WorkflowsStore } from "../WorkflowsStore";

export default class OpportunityConfigurationPresenter implements Hydratable {
  hydrationState: HydrationState;

  stateCode: string;

  opportunityType: string;

  envIsStaging: boolean;

  envIsDevelopment: boolean;

  constructor(
    private workflowsStore: WorkflowsStore,
    stateCode: string,
    opportunityType: string
  ) {
    makeAutoObservable(this, undefined, { autoBind: true });

    this.hydrationState = { status: "needs hydration" };

    this.stateCode = stateCode;
    this.opportunityType = opportunityType;

    this.envIsStaging = workflowsStore.envIsStaging;
    this.envIsDevelopment = workflowsStore.envIsDevelopment;

    autorun(async () => {
      if (this.hydrationState.status === "needs hydration") {
        await this.hydrate();
      }
    });
  }

  get opportunityConfigurations() {
    return this.workflowsStore.opportunityConfigurations;
  }

  get selectedOpportunityConfiguration() {
    return this.opportunityConfigurations?.find(
      (c) => c.id === this.workflowsStore.selectedConfigurationId
    );
  }

  async hydrate(): Promise<void> {
    if (
      this.hydrationState.status === "hydrated" ||
      this.hydrationState.status === "loading"
    )
      return;

    try {
      this.hydrationState = { status: "loading" };
      await flowResult(this.workflowsStore.populateOpportunityConfigurations());
      if (this.workflowsStore.opportunityConfigurations === undefined)
        throw new Error("Failed to populate opportunity configs");
      this.hydrationState = { status: "hydrated" };
    } catch (e) {
      this.hydrationState = { status: "failed", error: e as Error };
    }
  }
}
