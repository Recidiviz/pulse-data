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
import { Opportunity } from "./Opportunity";
import UserStore from "../UserStore";
import API from "../API";

interface OpportunityStoreProps {
  api: API;
  userStore: UserStore;
}

class OpportunityStore {
  api: API;

  isLoading?: boolean;

  opportunities?: Opportunity[];

  error?: string;

  userStore: UserStore;

  constructor({ api, userStore }: OpportunityStoreProps) {
    makeAutoObservable(this);

    this.api = api;
    this.userStore = userStore;
    this.isLoading = false;

    autorun(() => {
      if (!this.userStore.isAuthorized) {
        return;
      }

      this.fetchOpportunities();
    });
  }

  async fetchOpportunities(): Promise<void> {
    this.isLoading = true;

    try {
      runInAction(async () => {
        this.isLoading = false;
        this.opportunities = await this.api.get<Opportunity[]>(
          "/api/opportunities"
        );
      });
    } catch (error) {
      runInAction(() => {
        this.isLoading = false;
        this.error = error;
      });
    }
  }
}

export default OpportunityStore;
