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
import { autorun, makeAutoObservable, runInAction } from "mobx";
import API from "../API";
import { Client, SupervisionLevel } from "../ClientsStore";
import { Gender, SupervisionLevels } from "../ClientsStore/Client";
import UserStore from "../UserStore";
import {
  Policy,
  ScoreMinMaxBySupervisionLevel,
  SupervisionContactFrequency,
} from "./Policy";

interface PolicyStoreProps {
  api: API;
  userStore: UserStore;
}

class PolicyStore {
  api: API;

  isLoading?: boolean;

  policies?: Policy;

  error?: string;

  userStore: UserStore;

  constructor({ api, userStore }: PolicyStoreProps) {
    makeAutoObservable(this);

    this.api = api;
    this.userStore = userStore;
    this.isLoading = false;

    autorun(() => {
      if (!this.api.ready) {
        return;
      }

      this.fetchPolicies();
    });
  }

  async fetchPolicies(): Promise<void> {
    this.isLoading = true;

    try {
      const response = await this.api.post<Policy>(
        "/api/policy_requirements_for_state",
        { state: "US_ID" }
      );

      return runInAction(() => {
        this.isLoading = false;
        this.policies = response;
      });
    } catch (error) {
      return runInAction(() => {
        this.isLoading = false;
        this.error = error;
      });
    }
  }

  getDOCName(): string {
    return this.policies?.docShortName || "DOC";
  }

  get omsName(): string {
    return this.policies?.omsName || "OMS";
  }

  getSupervisionLevelCutoffsForClient(
    client: Client
  ): ScoreMinMaxBySupervisionLevel | undefined {
    return this.policies?.assessmentScoreCutoffs[client.gender];
  }

  findSupervisionLevelForScore(
    gender: Gender,
    score: number | null
  ): SupervisionLevel | undefined {
    const supervisionLevelCutoffs =
      this.policies?.assessmentScoreCutoffs[gender];

    if (!supervisionLevelCutoffs) {
      return;
    }

    return SupervisionLevels.find((supervisionLevel) => {
      const [min, max] = supervisionLevelCutoffs[supervisionLevel];
      return score && score >= min && score <= (max || Number.MAX_SAFE_INTEGER);
    });
  }

  getSupervisionLevelName(level: SupervisionLevel): string {
    return this.policies?.supervisionLevelNames[level] || level;
  }

  getSupervisionLevelNameForClient(client: Client): string {
    return this.getSupervisionLevelName(client.supervisionLevel);
  }

  findContactFrequencyForClient(
    client: Client
  ): SupervisionContactFrequency | undefined {
    const contactFrequencies =
      this.policies?.supervisionContactFrequencies[client.caseType];

    return contactFrequencies
      ? contactFrequencies[client.supervisionLevel]
      : undefined;
  }

  findHomeVisitFrequencyForClient(
    client: Client
  ): SupervisionContactFrequency | undefined {
    return this.policies?.supervisionHomeVisitFrequencies[
      client.supervisionLevel
    ];
  }
}
export default PolicyStore;
