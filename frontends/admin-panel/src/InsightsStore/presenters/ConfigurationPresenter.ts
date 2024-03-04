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
import { uniq } from "lodash";
import { autorun, flowResult, makeAutoObservable } from "mobx";

import { createNewConfiguration } from "../../AdminPanelAPI/InsightsAPI";
import { InsightsStore } from "../InsightsStore";
import { InsightsConfiguration } from "../models/InsightsConfiguration";
import { Hydratable, HydrationState } from "../types";

export default class ConfigurationPresenter implements Hydratable {
  hydrationState: HydrationState;

  stateCode: string;

  selectedFeatureVariant: string | null | undefined;

  constructor(private insightsStore: InsightsStore, stateCode: string) {
    makeAutoObservable(this, undefined, { autoBind: true });

    this.hydrationState = { status: "needs hydration" };

    this.stateCode = stateCode;

    this.selectedFeatureVariant = undefined;

    autorun(async () => {
      if (this.hydrationState.status === "needs hydration") {
        await this.hydrate();
      }
    });
  }

  get configs() {
    return this.insightsStore.configs;
  }

  setSelectedFeatureVariant(variant: string | undefined): void {
    this.selectedFeatureVariant = variant;
  }

  get allFeatureVariants(): (string | null)[] {
    return uniq(this.configs?.map((config) => config.featureVariant));
  }

  get priorityConfigForSelectedFeatureVariant():
    | InsightsConfiguration
    | undefined {
    const activeConfig = this.configs?.find(
      (config) =>
        config.featureVariant === this.selectedFeatureVariant &&
        config.status === "ACTIVE"
    );
    return activeConfig ?? this.configs?.slice().sort((a, b) => b.id - a.id)[0];
  }

  async createNewVersion(request: InsightsConfiguration): Promise<boolean> {
    try {
      await createNewConfiguration(request, this.stateCode);
      message.success("Configuration added!");
      this.hydrationState.status = "needs hydration";
      return true;
    } catch (e) {
      message.error(`Error adding configuration: ${e}`);
      return false;
    }
  }

  async hydrate(): Promise<void> {
    if (
      this.hydrationState.status === "hydrated" ||
      this.hydrationState.status === "loading"
    )
      return;

    try {
      this.hydrationState = { status: "loading" };
      await flowResult(this.insightsStore.populateConfigs());
      if (this.insightsStore.configs === undefined)
        throw new Error("Failed to populate configs");
      this.hydrationState = { status: "hydrated" };
    } catch (e) {
      this.hydrationState = { status: "failed", error: e as Error };
    }
  }
}
