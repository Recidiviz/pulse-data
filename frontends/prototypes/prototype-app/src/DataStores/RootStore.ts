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

import { Auth0ClientOptions } from "@auth0/auth0-spa-js";

import UserStore from "./UserStore";

/**
 * Returns the auth settings configured for the current environment, if any.
 */
export function getAuthSettings(): Auth0ClientOptions {
  return {
    client_id: "IcT6rZLNbi1PP180bciI63im7gmNWTPB",
    domain: "recidiviz-proto.us.auth0.com",
    audience: "https://prototypes-api.recidiviz.org",
    redirect_uri: `${window.location.origin}`,
  };
}

class RootStore {
  userStore: UserStore;

  constructor() {
    this.userStore = new UserStore({
      authSettings: getAuthSettings(),
      rootStore: this,
    });
  }
}

export default new RootStore();

export type { RootStore };
