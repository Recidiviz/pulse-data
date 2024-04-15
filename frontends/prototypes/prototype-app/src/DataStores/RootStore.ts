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

import { AuthSettings, AuthStore } from "@recidiviz/auth";
import { makeObservable, runInAction, when } from "mobx";

import { authenticate } from "../firebase";
import UserStore from "./UserStore";

/**
 * Returns the auth settings configured for the current environment, if any.
 */
export function getAuthSettings(): AuthSettings {
  return {
    client_id: "IcT6rZLNbi1PP180bciI63im7gmNWTPB",
    domain: "recidiviz-proto.us.auth0.com",
    audience: "https://prototypes-api.recidiviz.org",
    redirect_uri: `${window.location.origin}`,
  };
}

class RootStore {
  authStore: AuthStore;

  userStore: UserStore;

  firestoreAuthorized = false;

  constructor() {
    this.authStore = new AuthStore({ authSettings: getAuthSettings() });

    this.userStore = new UserStore({
      rootStore: this,
    });

    makeObservable(this, { firestoreAuthorized: true });

    // authenticate to Firestore once user is authorized
    when(
      () => this.authStore.isAuthorized,
      async () => {
        await authenticate(await this.authStore.getTokenSilently());
        runInAction(() => {
          this.firestoreAuthorized = true;
        });
      }
    );
  }
}

export default new RootStore();

export type { RootStore };
