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

import type { Auth0Client, Auth0ClientOptions } from "@auth0/auth0-spa-js";
import createAuth0Client from "@auth0/auth0-spa-js";
import { makeAutoObservable, runInAction } from "mobx";
import qs from "qs";

import { authenticate } from "../firebase";
import type { RootStore } from "./RootStore";

type ConstructorProps = {
  authSettings: Auth0ClientOptions;
  rootStore: RootStore;
};

/**
 * Reactive wrapper around Auth0 client.
 * If auth is disabled for this environment, all users will be immediately authorized.
 * Otherwise, call `authorize` to retrieve credentials or start login flow.
 *
 * @example
 *
 * ```js
 * const store = new UserStore({ isAuthRequired: true, authSettings: { domain, client_id, redirect_uri } });
 * if (!store.isAuthorized) {
 *   await store.authorize();
 *   // this may trigger a redirect to the Auth0 login domain;
 *   // if we're still here and user has successfully logged in,
 *   // store.isAuthorized should now be true.
 * }
 * ```
 */
export default class UserStore {
  readonly authSettings: Auth0ClientOptions;

  awaitingVerification: boolean;

  isAuthorized: boolean;

  isLoading: boolean;

  readonly rootStore: RootStore;

  constructor({ authSettings, rootStore }: ConstructorProps) {
    makeAutoObservable(this, {
      rootStore: false,
      authSettings: false,
    });

    this.authSettings = authSettings;
    this.rootStore = rootStore;

    this.awaitingVerification = false;
    this.isAuthorized = false;
    this.isLoading = true;
  }

  private get auth0Client(): Promise<Auth0Client> {
    return createAuth0Client(this.authSettings);
  }

  /**
   * If user already has a valid Auth0 credential, this method will retrieve it
   * and update class properties accordingly. If not, user will be redirected
   * to the Auth0 login domain for fresh authentication. After successful login,
   * optional `handleTargetUrl` callback will be called with post-redirect target URL
   * (useful for, e.g., client-side router that does not listen to history events).
   * Returns an Error if Auth0 configuration is not present.
   */
  async authorize({
    handleTargetUrl,
  }: {
    handleTargetUrl?: (targetUrl: string) => void;
  } = {}): Promise<void> {
    const auth0 = await this.auth0Client;

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
      if (handleTargetUrl) handleTargetUrl(replacementUrl);
    }

    if (await auth0.isAuthenticated()) {
      const user = await auth0.getUser();

      if (user?.email_verified) {
        const auth0Token = await auth0.getTokenSilently();
        await authenticate(auth0Token);
      }

      runInAction(() => {
        this.isLoading = false;
        if (user?.email_verified) {
          this.isAuthorized = true;
          this.awaitingVerification = false;
        } else {
          this.isAuthorized = false;
          this.awaitingVerification = true;
        }
      });
    } else {
      auth0.loginWithRedirect({
        appState: { targetUrl: window.location.href },
      });
    }
  }

  async logout(): Promise<void> {
    const auth0 = await this.auth0Client;
    runInAction(() => {
      this.isAuthorized = false;
      this.isLoading = true;
    });
    return auth0.logout();
  }
}
