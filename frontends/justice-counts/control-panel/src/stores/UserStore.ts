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
import { User } from "@auth0/auth0-spa-js";
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

  email: string | undefined;

  userID: string | undefined;

  auth0UserID: string | undefined;

  userAgencies: UserAgency[] | undefined;

  userInfoLoaded: boolean;

  hasSeenOnboarding: boolean;

  permissions: string[];

  constructor(authStore: AuthStore, api: API) {
    makeAutoObservable(this);

    this.authStore = authStore;
    this.api = api;
    this.email = undefined;
    this.userID = undefined;
    this.auth0UserID = undefined;
    this.userAgencies = undefined;
    this.userInfoLoaded = false;
    this.hasSeenOnboarding = true;
    this.permissions = [];

    when(
      () => api.isSessionInitialized,
      () => this.updateAndRetrieveUserInfo()
    );
  }

  get userAgency(): UserAgency | undefined {
    if (this.userAgencies && this.userAgencies.length > 0) {
      // attempting to access 0 index in the empty array leads to the mobx warning "[mobx] Out of bounds read: 0"
      // so check the length of the array before accessing
      return this.userAgencies[0];
    }
    return undefined;
  }

  async updateAndRetrieveUserInfo() {
    try {
      if (!this.authStore.user) {
        Promise.reject(new Error("No user information exists."));
      }

      const { email, sub: auth0ID } = this.authStore.user as User;

      const response = (await this.api.request({
        path: "/api/users",
        method: "POST",
        body: {
          email_address: email,
          auth0_user_id: auth0ID,
        },
      })) as Response;

      const {
        email_address: emailAddress,
        id: userID,
        auth0_user_id: auth0UserID,
        agencies: userAgencies,
        permissions,
        has_seen_onboarding: hasSeenOnboarding, // will be used in future
      } = await response.json();

      runInAction(() => {
        this.email = emailAddress;
        this.userID = userID;
        this.auth0UserID = auth0UserID;
        this.userAgencies = userAgencies;
        this.permissions = permissions;
        this.hasSeenOnboarding = hasSeenOnboarding; // will be used in future

        if (this.userID && this.userAgencies) {
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
