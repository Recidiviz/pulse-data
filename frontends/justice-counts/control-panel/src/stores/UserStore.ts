// Recidiviz - a data platform for criminal justice reform
// Copyright (C) 2022 Recidiviz, Inc.
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
import { makeAutoObservable, runInAction, when } from "mobx";

import { AuthStore } from "../components/Auth";
import API from "./API";

type UserAgency = {
  name: string;
  id: number;
};
class UserStore {
  authStore: AuthStore;

  api: API;

  name: string | undefined;

  email: string | undefined;

  nameOrEmail: string | undefined;

  auth0UserID: string | undefined;

  userAgencies: UserAgency[] | undefined;

  userInfoLoaded: boolean;

  hasSeenOnboarding: boolean;

  permissions: string[];

  currentAgencyId: number | undefined;

  constructor(authStore: AuthStore, api: API) {
    makeAutoObservable(this);
    this.authStore = authStore;
    this.api = api;
    this.name = this.authStore.user?.name;
    this.email = this.authStore.user?.email;
    this.auth0UserID = this.authStore.user?.id;
    this.userAgencies = undefined;
    this.userInfoLoaded = false;
    this.hasSeenOnboarding = true;
    this.permissions = [];
    this.currentAgencyId = undefined;

    when(
      () => api.isSessionInitialized,
      () => this.retrieveUserPermissionsAndAgencies()
    );
  }

  getInitialAgencyId(): number | undefined {
    if (this.userAgencies && this.userAgencies.length > 0) {
      // attempting to access 0 index in the empty array leads to the mobx warning "[mobx] Out of bounds read: 0"
      // so check the length of the array before accessing
      return this.userAgencies[0].id;
    }
    return undefined;
  }

  get currentAgency(): UserAgency | undefined {
    return this.userAgencies?.find(
      (agency) => agency.id === this.currentAgencyId
    );
  }

  setCurrentAgencyId(agencyId: number | undefined) {
    runInAction(() => {
      this.currentAgencyId = agencyId;
    });
  }

  async retrieveUserPermissionsAndAgencies() {
    try {
      if (!this.authStore.user) {
        Promise.reject(new Error("No user information exists."));
      }
      const response = (await this.api.request({
        path: "/api/users",
        method: "POST",
        body: {
          name: this.authStore.user?.name,
        },
      })) as Response;
      const {
        agencies: userAgencies,
        permissions,
        has_seen_onboarding: hasSeenOnboarding, // will be used in future
      } = await response.json();
      runInAction(() => {
        this.nameOrEmail =
          this.authStore.user?.name || this.authStore.user?.email;
        this.userAgencies = userAgencies;
        this.permissions = permissions;
        this.hasSeenOnboarding = hasSeenOnboarding; // will be used in future
        this.currentAgencyId = this.getInitialAgencyId();
        if (this.userAgencies) {
          this.userInfoLoaded = true;
        }
      });
    } catch (error) {
      if (error instanceof Error) return error.message;
      return String(error);
    }
  }
}

export default UserStore;
