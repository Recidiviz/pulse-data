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

import { AuthStore } from "../components/Auth";
import API from "./API";

const getAuthSettings = (): Auth0ClientOptions => {
  return {
    client_id: process.env.REACT_APP_AUTH0_CLIENT_ID as string,
    domain: process.env.REACT_APP_AUTH0_DOMAIN as string,
    audience: process.env.REACT_APP_AUTH0_AUDIENCE as string,
    redirect_uri: `${window.location.origin}`,
  };
};

class RootStore {
  authStore: AuthStore;

  api: API;

  constructor() {
    this.authStore = new AuthStore({
      authSettings: getAuthSettings(),
    });
    this.api = new API(this.authStore);
  }
}

export default new RootStore();

export type { RootStore };
