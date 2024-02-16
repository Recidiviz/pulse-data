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
import { flowResult, makeAutoObservable } from "mobx";

import {
  createNewConfiguration,
  getInsightsConfigurations,
  getInsightsStateCodeInfo,
} from "../AdminPanelAPI/InsightsAPI";
import { StateCodeInfo } from "../components/general/constants";
import { InsightsConfiguration } from "./models/InsightsConfiguration";
import { Hydratable, HydrationState } from "./types";

export class InsightsStore implements Hydratable {
  hydrationState: HydrationState;

  stateCode: string;

  stateCodeInfo?: StateCodeInfo[];

  configs?: InsightsConfiguration[];

  constructor() {
    makeAutoObservable(this);

    this.setStateCode = this.setStateCode.bind(this);

    this.createNewVersion = this.createNewVersion.bind(this);

    this.hydrationState = { status: "needs hydration" };

    this.stateCode = "US_PA";
  }

  setStateCode(stateCode: string): void {
    this.stateCode = stateCode;
  }

  async createNewVersion(request: InsightsConfiguration): Promise<boolean> {
    try {
      await createNewConfiguration(request, this.stateCode);
      message.success("Configuration added!");
      return true;
    } catch (e) {
      message.error(`Error adding configuration: ${e}`);
      return false;
    }
  }

  *populateConfigs(): Generator<
    Promise<InsightsConfiguration[]>,
    void,
    InsightsConfiguration[]
  > {
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
      await flowResult(this.populateConfigs());
      await flowResult(this.populateStateCodeInfo());
      this.hydrationState = { status: "hydrated" };
    } catch (e) {
      this.hydrationState = { status: "failed", error: e as Error };
    }
  }
}

export default new InsightsStore();
