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
import ReportStore from "./ReportStore";
import UserStore from "./UserStore";

const getAuthSettings = (): Auth0ClientOptions | undefined => {
  if (window.AUTH0_CONFIG) {
    return {
      domain: window.AUTH0_CONFIG.domain,
      client_id: window.AUTH0_CONFIG.clientId,
      redirect_uri: window.location.origin,
      audience: window.AUTH0_CONFIG.audience,
      useRefreshTokens: true,
    };
  }
  return undefined;
};

class RootStore {
  authStore: AuthStore;

  api: API;

  userStore: UserStore;

  reportStore: ReportStore;

  constructor() {
    this.authStore = new AuthStore({
      authSettings: getAuthSettings(),
    });
    this.api = new API(this.authStore);
    this.userStore = new UserStore(this.authStore, this.api);
    this.reportStore = new ReportStore(this.userStore, this.api);
  }
}

export default new RootStore();

export type { RootStore };
