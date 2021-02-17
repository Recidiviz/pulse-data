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
import {
  SupervisionLevels,
  SupervisionLevel,
  DecoratedClient,
} from "../ClientsStore";
import {
  ContactFrequencyByRisk,
  Policy,
  ScoreMinMaxBySupervisionLevel,
  SupervisionContactFrequency,
} from "./Policy";

interface PolicyStoreProps {
  userStore: UserStore;
}

class PolicyStore {
  isLoading?: boolean;

  policies?: Policy;

  error?: string;

  userStore: UserStore;

  constructor({ userStore }: PolicyStoreProps) {
    makeAutoObservable(this);

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

    if (!this.userStore.getTokenSilently) {
      return;
    }

    const token = await this.userStore.getTokenSilently({
      audience: "https://case-triage.recidiviz.org/api",
      scope: "email",
    });

    const response = await fetch("/api/policy_requirements_for_state", {
      body: '{"state": "US_ID"}',
      method: "POST",
      headers: {
        Authorization: `Bearer ${token}`,
        "Content-Type": "application/json",
      },
    });

    try {
      runInAction(async () => {
        this.isLoading = false;
        this.policies = await response.json();
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

  findAssessmentSupervisionLevelForClient(
    client: DecoratedClient
  ): SupervisionLevel | undefined {
    const supervisionLevelCutoffs = this.getSupervisionLevelCutoffsForClient(
      client
    );
    if (!supervisionLevelCutoffs) {
      return;
    }

    return SupervisionLevels.find((supervisionLevel: SupervisionLevel) => {
      const [min, max] = supervisionLevelCutoffs[supervisionLevel];
      return (
        client.assessmentScore !== null &&
        client.assessmentScore >= min &&
        (max === null || client.assessmentScore <= max)
      );
    });
  }

  getContactFrequenciesForClient(
    client: DecoratedClient
  ): ContactFrequencyByRisk | undefined {
    return this.policies?.supervisionContactFrequencies[client.caseType];
  }

  findContactFrequencyForClient(
    client: DecoratedClient
  ): SupervisionContactFrequency | undefined {
    const contactFrequencies = this.getContactFrequenciesForClient(client);

    return contactFrequencies
      ? contactFrequencies[client.supervisionLevel]
      : undefined;
  }
}
export default PolicyStore;
