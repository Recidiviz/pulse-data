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

import {
  getOpportunities,
  getOpportunityConfigurations,
  getWorkflowsStateCodeInfo,
} from "../AdminPanelAPI/WorkflowsAPI";
import { StateCodeInfo } from "../components/general/constants";
import { gcpEnvironment } from "../components/Utilities/EnvironmentUtilities";
import { Hydratable, HydrationState } from "../InsightsStore/types";
import { Opportunity } from "./models/Opportunity";
import { OpportunityConfiguration } from "./models/OpportunityConfiguration";
import OpportunityConfigurationPresenter from "./presenters/OpportunityConfigurationPresenter";
import OpportunityPresenter from "./presenters/OpportunityPresenter";

export class WorkflowsStore implements Hydratable {
  hydrationState: HydrationState;

  stateCode?: string;

  stateCodeInfo?: StateCodeInfo[];

  opportunities?: Opportunity[];

  selectedOpportunityType?: string;

  selectedConfigurationId?: number;

  opportunityConfigurations?: OpportunityConfiguration[];

  opportunityPresenter?: OpportunityPresenter;

  opportunityConfigurationPresenter?: OpportunityConfigurationPresenter;

  envIsStaging: boolean;

  envIsDevelopment: boolean;

  constructor() {
    makeAutoObservable(this, {}, { autoBind: true });
    this.hydrationState = { status: "needs hydration" };

    this.envIsStaging = gcpEnvironment.isStaging;
    this.envIsDevelopment = gcpEnvironment.isDevelopment;

    autorun(() => {
      if (this.stateCode) {
        this.initializeOpportunityPresenter();
      }
    });
    autorun(() => {
      if (this.selectedOpportunityType) {
        this.initializeOpportunityConfigurationPresenter();
      }
    });
  }

  setStateCode(stateCode: string): void {
    this.opportunities = undefined;
    this.opportunityConfigurations = undefined;
    this.stateCode = stateCode;
  }

  setSelectedOpportunityType(opportunityType?: string): void {
    this.opportunityConfigurations = undefined;
    this.selectedOpportunityType = opportunityType;
  }

  setSelectedOpportunityConfigurationId(id?: number): void {
    this.selectedConfigurationId = id;
  }

  initializeOpportunityPresenter() {
    if (!this.stateCode) return;
    this.opportunityPresenter = new OpportunityPresenter(this, this.stateCode);
  }

  initializeOpportunityConfigurationPresenter() {
    if (!this.stateCode || !this.selectedOpportunityType) return;
    this.opportunityConfigurationPresenter =
      new OpportunityConfigurationPresenter(
        this,
        this.stateCode,
        this.selectedOpportunityType
      );
  }

  *populateStateCodeInfo(): Generator<
    Promise<StateCodeInfo[]>,
    void,
    StateCodeInfo[]
  > {
    this.stateCodeInfo = yield getWorkflowsStateCodeInfo();
  }

  *populateOpportunities(): Generator<
    Promise<Opportunity[]>,
    void,
    Opportunity[]
  > {
    if (!this.stateCode) throw new Error("missing state code");
    this.opportunities = yield getOpportunities(this.stateCode);
  }

  *populateOpportunityConfigurations(): Generator<
    Promise<OpportunityConfiguration[]>,
    void,
    OpportunityConfiguration[]
  > {
    if (!this.stateCode) throw new Error("missing state code");
    if (!this.selectedOpportunityType)
      throw new Error("missing selectedOpportunity");
    this.opportunityConfigurations = yield getOpportunityConfigurations(
      this.stateCode,
      this.selectedOpportunityType
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
      await flowResult(this.populateStateCodeInfo());
      this.hydrationState = { status: "hydrated" };
    } catch (e) {
      this.hydrationState = { status: "failed", error: e as Error };
    }
  }
}
