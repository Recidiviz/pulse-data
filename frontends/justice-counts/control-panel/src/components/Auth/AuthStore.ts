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

import createAuth0Client, {
  Auth0Client,
  Auth0ClientOptions,
  GetTokenSilentlyOptions,
  User,
} from "@auth0/auth0-spa-js";
import { makeAutoObservable, runInAction } from "mobx";
import qs from "qs";

import { identify } from "../../analytics";

export const APP_METADATA_CLAIM =
  "https://dashboard.recidiviz.org/app_metadata";

interface AuthStoreProps {
  authSettings: Auth0ClientOptions | undefined;
}
export class AuthStore {
  readonly authSettings: Auth0ClientOptions | undefined;

  private authClient: Auth0Client | undefined;

  emailVerified?: boolean;

  isAuthorized: boolean;

  isLoading: boolean;

  user?: User;

  constructor({ authSettings }: AuthStoreProps) {
    makeAutoObservable(this);

    this.authSettings = authSettings;
    this.authClient = undefined;
    this.isAuthorized = false;
    this.isLoading = true;
  }

  private get auth0Client(): Promise<Auth0Client> {
    return createAuth0Client(this.authSettings as Auth0ClientOptions);
  }

  async authenticate(): Promise<void> {
    const auth0 = await this.auth0Client;

    runInAction(() => {
      this.authClient = auth0;
    });

    /*
     * Note: need to research this issue further
     * Auth0 library responds to the presence of query parameters (code, state, and error) all of which
     * indicate that we have been redirected from the Auth0 login page. If they are not removed before
     * further interaction with the library, the application enters an infinite loop with newly regenerated
     * state/code/error params. The below logic mitigates that by stripping away the parameters
     * completely or replaces them with the `targetUrl`.
     */
    const urlQuery = qs.parse(window.location.search, {
      ignoreQueryPrefix: true,
    });

    if (urlQuery.code && urlQuery.state) {
      const { appState } = await auth0.handleRedirectCallback();
      // auth0 params are single-use, must be removed from history or they can cause errors
      let replacementUrl;

      if (appState && appState.targetUrl) {
        replacementUrl = appState.targetUrl;
      } else {
        // strip away all query params just to be safe
        replacementUrl = `${window.location.origin}${window.location.pathname}`;
      }

      window.history.replaceState({}, document.title, replacementUrl);
    }

    if (await auth0.isAuthenticated()) {
      const user = await auth0.getUser();
      if (user?.sub) {
        identify(user.sub, {
          name: user.name,
          email: user.email,
        });
      }

      runInAction(() => {
        this.isLoading = false;
        this.user = user;
        this.isAuthorized = true;
        this.emailVerified = Boolean(user?.email_verified);
      });
    } else {
      await auth0.loginWithRedirect({
        appState: { targetUrl: window.location.href },
      });
    }
  }

  async logoutUser() {
    runInAction(() => {
      this.isAuthorized = false;
      this.isLoading = true;
    });

    return this.authClient?.logout({ returnTo: window.location.origin });
  }

  async getToken(options?: GetTokenSilentlyOptions): Promise<string | Error> {
    try {
      if (this.authClient) {
        return this.authClient.getTokenSilently(options);
      }
      return Promise.reject(new Error("No auth client initialized."));
    } catch (error) {
      throw new Error(`Unable to retrieve token - ${error}`);
    }
  }
}
