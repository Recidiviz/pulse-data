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
import { makeAutoObservable, runInAction } from "mobx";
import createAuth0Client, {
  Auth0ClientOptions,
  GetTokenSilentlyOptions,
  LogoutOptions,
  RedirectLoginOptions,
  User,
} from "@auth0/auth0-spa-js";
import qs from "qs";

interface UserStoreProps {
  authSettings: Auth0ClientOptions;
}

export enum KNOWN_EXPERIMENTS {
  TOP_OPPORTUNITIES = "can-see-top-opportunities",
}

export interface FeatureVariants {
  [feature: string]: string;
}

export default class UserStore {
  authError?: Error;

  authSettings: Auth0ClientOptions;

  currentVersion?: string;

  featureVariants: FeatureVariants;

  isAuthorized: boolean;

  isLoading: boolean;

  lacksCaseTriageAuthorization: boolean;

  shouldReload: boolean;

  shouldNotHardReload: boolean;

  user?: User;

  getTokenSilently?(options?: GetTokenSilentlyOptions): Promise<void>;

  login?(options?: RedirectLoginOptions): Promise<void>;

  logout?(options?: LogoutOptions): void;

  constructor({ authSettings }: UserStoreProps) {
    makeAutoObservable(this);

    this.authSettings = authSettings;
    this.lacksCaseTriageAuthorization = false;
    this.featureVariants = {};
    this.isAuthorized = false;
    this.isLoading = true;
    this.shouldReload = false;
    this.shouldNotHardReload = false;
  }

  async authorize(): Promise<void> {
    const auth0 = await createAuth0Client(this.authSettings);
    this.getTokenSilently = auth0.getTokenSilently.bind(auth0);
    this.login = auth0.loginWithRedirect.bind(auth0);
    this.logout = auth0.logout.bind(auth0);

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

      runInAction(() => {
        this.user = user;
        this.isAuthorized = user ? !!user.email_verified : false;
        this.isLoading = false;
      });
    } else {
      this.isLoading = false;

      await auth0.loginWithRedirect({
        appState: { targetUrl: window.location.href },
      });
    }
  }

  static build(): UserStore {
    return new UserStore({
      authSettings: {
        domain: window.AUTH0_CONFIG.domain,
        client_id: window.AUTH0_CONFIG.clientId,
        redirect_uri: window.location.origin,
        audience: window.AUTH0_CONFIG.audience,
      },
    });
  }

  // Used to keep track of the current version and prompt a reload if the
  // current version has been updated.
  recordVersion(version: string, requireUserPromptForReload: boolean): void {
    if (this.currentVersion === undefined) {
      this.currentVersion = version;
    }

    if (this.currentVersion === version) {
      return;
    }

    this.shouldReload = true;
    if (requireUserPromptForReload) {
      this.shouldNotHardReload = true;
    } else if (!this.shouldNotHardReload) {
      setTimeout(() => {
        // If we've waited a second and we know we're out of date,
        // we can comfortably hard reload.
        if (!this.shouldNotHardReload) {
          window.location.reload();
        }
      }, 1000);
    }
  }

  setLacksCaseTriageAuthorization(lacksCaseTriageAuthorization: boolean): void {
    runInAction(() => {
      this.lacksCaseTriageAuthorization = lacksCaseTriageAuthorization;
    });
  }

  setFeatureVariants(variants: FeatureVariants): void {
    this.featureVariants = variants;
  }

  isInExperiment(experiment: KNOWN_EXPERIMENTS): boolean {
    return !!this.featureVariants[experiment];
  }
}
