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
import { showToast } from "../components/Toast";
import API from "./API";

type UserAgency = {
  name: string;
  id: number;
};

type UserSettingsRequestBody = {
  name: string | null;
  email: string | null;
};
class UserStore {
  authStore: AuthStore;

  api: API;

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
    this.auth0UserID = this.authStore.user?.id;
    this.userAgencies = undefined;
    this.userInfoLoaded = false;
    this.hasSeenOnboarding = true;
    this.permissions = [];
    this.currentAgencyId = undefined;

    when(
      () => api.isSessionInitialized,
      () => this.updateAndRetrieveUserPermissionsAndAgencies()
    );
  }

  async updateUserNameAndEmail(
    name: string,
    email: string
  ): Promise<string | undefined> {
    try {
      const body: UserSettingsRequestBody = { name: null, email: null };
      const isNameUpdated = name !== this.authStore.user?.name;
      const isEmailUpdated = email !== this.authStore.user?.email;
      if (isNameUpdated) {
        body.name = name;
      }
      if (isEmailUpdated) {
        body.email = email;
      }
      const response = await this.api.request({
        path: "/api/users/update",
        method: "POST",
        body,
      });
      runInAction(() => {
        this.authStore.user = { ...this.authStore.user, name, email };
      });

      if (response && response instanceof Response) {
        if (response.status === 200 && isNameUpdated && !isEmailUpdated) {
          showToast(`Name was successfully updated to ${name}.`, true);
          return;
        }
        if (response.status === 200 && isNameUpdated && isEmailUpdated) {
          showToast(
            `Name and email were successfully updated. You will be logged out. Please check your email at ${email} to verify your new email before logging in again.`,
            /* check  */ true,
            /* color */ undefined,
            /* timeout */ 4500
          );
          return;
        }
        if (response.status === 200 && !isNameUpdated && isEmailUpdated) {
          showToast(
            `Email was successfully updated. You will be logged out. Please check your email at ${email} to verify your new email before logging in again.`,
            /* check  */ true,
            /* color */ undefined,
            /* timeout */ 4500
          );
          return;
        }
        if (response.status !== 200) {
          showToast("Failed to update user details.", false, "red");
          return;
        }
      }
    } catch (error) {
      let errorMessage = "";
      if (error instanceof Error) {
        errorMessage = error.message;
      } else {
        errorMessage = String(error);
      }

      showToast(`Failed to update user details. ${errorMessage}`, false, "red");
      return errorMessage;
    }
  }

  getInitialAgencyId(): number | undefined {
    if (this.userAgencies && this.userAgencies.length > 0) {
      // attempting to access 0 index in the empty array leads to the mobx warning "[mobx] Out of bounds read: 0"
      // so check the length of the array before accessing
      return this.userAgencies[0].id;
    }
    return undefined;
  }

  get name(): string | undefined {
    return this.authStore.user?.name;
  }

  get email(): string | undefined {
    return this.authStore.user?.email;
  }

  get nameOrEmail(): string | undefined {
    return this.name || this.email;
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

  async updateAndRetrieveUserPermissionsAndAgencies() {
    try {
      const response = (await this.api.request({
        path: "/api/users",
        method: "POST",
        body: {
          name: this.name,
        },
      })) as Response;
      const {
        agencies: userAgencies,
        permissions,
        has_seen_onboarding: hasSeenOnboarding, // will be used in future
      } = await response.json();
      runInAction(() => {
        this.userAgencies = userAgencies;
        this.permissions = permissions;
        this.hasSeenOnboarding = hasSeenOnboarding; // will be used in future
        this.currentAgencyId = this.getInitialAgencyId();
        this.userInfoLoaded = true;
      });
    } catch (error) {
      if (error instanceof Error) return error.message;
      return String(error);
    }
  }
}

export default UserStore;
