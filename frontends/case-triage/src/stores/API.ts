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
import * as Sentry from "@sentry/react";
import { computed, makeObservable, observable, when } from "mobx";
import { identify } from "../analytics";
import ErrorMessageStore from "./ErrorMessageStore";
import UserStore, { FeatureVariants } from "./UserStore";
import { captureExceptionWithLogs } from "../utils";

interface APIProps {
  errorMessageStore: ErrorMessageStore;
  userStore: UserStore;
}

interface RequestProps {
  path: string;
  method: "GET" | "POST" | "DELETE";
  body?: Record<string, unknown>;
  retrying?: boolean;
}

export interface FullName {
  // eslint-disable-next-line camelcase
  given_names?: string;
  // eslint-disable-next-line camelcase
  full_name?: string;
  surname?: string;
}

interface BootstrapResponse {
  csrf: string;
  segmentUserId: string;
  intercomUserHash: string;
  knownExperiments: FeatureVariants;
  dashboardURL: string;
  officerFullName: FullName;
  isImpersonating: boolean;
  canAccessCaseTriage: boolean;
  canAccessLeadershipDashboard: boolean;
  stateCode: string;
  shouldSeeOnboarding: boolean;
}

export type ErrorResponse = {
  code: string;
  description: string | Record<string, unknown>;
};

export function isErrorResponse(x: {
  [key: string]: unknown;
}): x is ErrorResponse {
  return (
    typeof x.code === "string" &&
    (typeof x.description === "string" || typeof x.description === "object")
  );
}

const BOOTSTRAP_ROUTE = "/api/bootstrap";
const IMPERSONATED_EMAIL_KEY = "impersonated_email";

interface BootstrapProps {
  impersonate: string | null;
}

function popImpersonateParameter(): string | null {
  const params = new URLSearchParams(window.location.search);

  const impersonate = params.get(IMPERSONATED_EMAIL_KEY);
  if (impersonate) {
    params.delete(IMPERSONATED_EMAIL_KEY);

    window.history.replaceState(
      null,
      window.document.title,
      params.toString() ? `/?${params.toString()}` : "/"
    );
  }
  return impersonate;
}

class API {
  /* Boolean indicating whether we have received a response from `/api/bootstrap`.
    When true, we know whether user has access to Case Triage / Leadership tools. If they do have access, the rest of
    their user-specific information is known.
   */
  bootstrapped?: boolean;

  bootstrapping?: Promise<void>;

  csrfToken: string;

  dashboardURL: string;

  errorMessageStore: ErrorMessageStore;

  userStore: UserStore;

  constructor({ errorMessageStore, userStore }: APIProps) {
    this.csrfToken = "";
    this.dashboardURL = "";
    this.errorMessageStore = errorMessageStore;
    this.userStore = userStore;
    makeObservable(this, {
      bootstrapped: observable,
      ready: computed,
    });

    when(
      () => userStore.isAuthorized,
      () => this.bootstrap({ impersonate: popImpersonateParameter() })
    );
  }

  get ready(): boolean {
    // Indicates whether the API is ready to make requests
    return !!this.bootstrapped && !!this.csrfToken;
  }

  bootstrap({ impersonate = "" }: BootstrapProps): Promise<void> {
    const body = impersonate
      ? { [IMPERSONATED_EMAIL_KEY]: impersonate }
      : undefined;

    this.bootstrapping =
      this.bootstrapping ||
      this.get<BootstrapResponse>(BOOTSTRAP_ROUTE, body)
        .then(
          ({
            csrf,
            segmentUserId,
            intercomUserHash,
            knownExperiments: featureVariants,
            dashboardURL,
            officerFullName,
            isImpersonating,
            canAccessCaseTriage,
            canAccessLeadershipDashboard,
            stateCode,
            shouldSeeOnboarding,
          }) => {
            this.csrfToken = csrf;
            identify(segmentUserId, intercomUserHash);
            this.dashboardURL = dashboardURL;
            this.userStore.setFeatureVariants(featureVariants);
            this.userStore.setCaseTriageAccess(canAccessCaseTriage);
            this.userStore.setLeadershipDashboardAccess(
              canAccessLeadershipDashboard
            );
            this.userStore.setStateCode(stateCode);
            this.userStore.setShouldSeeOnboarding(shouldSeeOnboarding);

            Sentry.setUser({ id: segmentUserId });
            Sentry.setTag("app.version", this.userStore.currentVersion);

            this.userStore.setOfficerMetadata(officerFullName, isImpersonating);
            this.bootstrapped = true;
          }
        )
        .catch((error) => {
          if (error.code === "no_case_triage_access") {
            this.userStore.setCaseTriageAccess(false);
            this.userStore.setLeadershipDashboardAccess(false);
            this.bootstrapped = true;
          }
        });

    return this.bootstrapping;
  }

  async request<T>({
    path,
    method,
    body,
    retrying = false,
  }: RequestProps): Promise<T> {
    try {
      // Defer all requests until the API client has bootstrapped itself
      if (!this.bootstrapped && path.indexOf(BOOTSTRAP_ROUTE) === -1) {
        await this.bootstrap({ impersonate: popImpersonateParameter() });
      }

      if (!this.userStore.getTokenSilently) {
        return Promise.reject();
      }

      const token = await this.userStore.getTokenSilently({
        audience: "https://case-triage.recidiviz.org/api",
        scope: "email",
      });

      const response = await fetch(path, {
        body: method !== "GET" ? JSON.stringify(body) : null,
        method,
        headers: {
          Authorization: `Bearer ${token}`,
          "Content-Type": "application/json",
          "X-CSRF-Token": this.csrfToken,
        },
      });

      const reportedVersion = response.headers.get(
        "X-Recidiviz-Current-Version"
      );
      if (reportedVersion !== null) {
        this.userStore.recordVersion(reportedVersion, method !== "GET");
      }

      const json = await response.json();

      if (response.status === 400 && json.code === "invalid_csrf_token") {
        this.invalidateBootstrap();
        if (!retrying) {
          return this.request<T>({ path, method, body, retrying: true });
        }
      }

      if (response.status === 401 && json.code === "no_case_triage_access") {
        this.userStore.setCaseTriageAccess(false);
        this.userStore.setLeadershipDashboardAccess(false);
      }

      if (isErrorResponse(json)) {
        this.errorMessageStore.pushError(json);
        throw json;
      }

      return json;
    } catch (error) {
      captureExceptionWithLogs(error);
      throw error;
    }
  }

  async delete<T>(path: string): Promise<T> {
    return this.request({ path, method: "DELETE" });
  }

  async get<T>(path: string, body: Record<string, string> = {}): Promise<T> {
    const search = new URLSearchParams(body).toString();
    let pathWithSearch = path;
    if (search) {
      pathWithSearch = `${path}?${search}`;
    }
    return this.request({ path: pathWithSearch, method: "GET" });
  }

  async post<T>(path: string, body: Record<string, unknown>): Promise<T> {
    return this.request({ path, body, method: "POST" });
  }

  private invalidateBootstrap() {
    this.bootstrapped = false;
    this.bootstrapping = undefined;
  }
}

export default API;
