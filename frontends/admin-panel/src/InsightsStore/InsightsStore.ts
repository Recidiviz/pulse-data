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

import { action, autorun, flowResult, makeAutoObservable } from "mobx";

import {
  getInsightsConfigurations,
  getInsightsStateCodeInfo,
} from "../AdminPanelAPI/InsightsAPI";
import { StateCodeInfo } from "../components/general/constants";
import { InsightsConfiguration } from "./models/InsightsConfiguration";
import ConfigurationPresenter from "./presenters/ConfigurationPresenter";
import { Hydratable, HydrationState } from "./types";

export class InsightsStore implements Hydratable {
  hydrationState: HydrationState;

  stateCode?: string;

  stateCodeInfo?: StateCodeInfo[];

  configs?: InsightsConfiguration[];

  configurationPresenter?: ConfigurationPresenter;

  constructor() {
    makeAutoObservable(
      this,
      { initializeConfigurationPresenter: action },
      { autoBind: true }
    );
    this.hydrationState = { status: "needs hydration" };

    autorun(() => {
      if (this.stateCode) {
        this.initializeConfigurationPresenter();
      }
    });
  }

  setStateCode(stateCode: string): void {
    this.resetStateData();
    this.stateCode = stateCode;
  }

  resetStateData() {
    this.configs = undefined;
    this.configurationPresenter = undefined;
  }

  initializeConfigurationPresenter() {
    if (!this.stateCode) return;
    this.configurationPresenter = new ConfigurationPresenter(
      this,
      this.stateCode
    );
  }

  *populateConfigs(): Generator<
    Promise<InsightsConfiguration[]>,
    void,
    InsightsConfiguration[]
  > {
    if (!this.stateCode) throw new Error("missing state code");
    this.configs = yield getInsightsConfigurations(this.stateCode);
  }

  *populateStateCodeInfo(): Generator<
    Promise<StateCodeInfo[]>,
    void,
    StateCodeInfo[]
  > {
    this.stateCodeInfo = yield getInsightsStateCodeInfo();
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
