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
import UserStore from "../UserStore";
import { Client, DecoratedClient } from "../ClientsStore";
import {
  Policy,
  ScoreMinMaxBySupervisionLevel,
  SupervisionContactFrequency,
} from "./Policy";
import API from "../API";

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
      if (!this.userStore.isAuthorized) {
        return;
      }

      this.fetchPolicies();
    });
  }

  async fetchPolicies(): Promise<void> {
    this.isLoading = true;

    try {
      runInAction(async () => {
        this.isLoading = false;
        this.policies = await this.api.post<Policy>(
          "/api/policy_requirements_for_state",
          { state: "US_ID" }
        );
      });
    } catch (error) {
      runInAction(() => {
        this.isLoading = false;
        this.error = error;
      });
    }
  }

  getSupervisionLevelCutoffsForClient(
    client: DecoratedClient
  ): ScoreMinMaxBySupervisionLevel | undefined {
    return this.policies?.assessmentScoreCutoffs[client.gender];
  }

  getSupervisionLevelNameForClient(client: Client): string {
    return (
      this.policies?.supervisionLevelNames[client.supervisionLevel] ||
      client.supervisionLevel
    );
  }

  findContactFrequencyForClient(
    client: DecoratedClient
  ): SupervisionContactFrequency | undefined {
    const contactFrequencies = this.policies?.supervisionContactFrequencies[
      client.caseType
    ];

    return contactFrequencies
      ? contactFrequencies[client.supervisionLevel]
      : undefined;
  }

  findHomeVisitFrequencyForClient(
    client: DecoratedClient
  ): SupervisionContactFrequency | undefined {
    return this.policies?.supervisionHomeVisitFrequencies[
      client.supervisionLevel
    ];
  }
}
export default PolicyStore;
