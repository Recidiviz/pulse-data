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
import { identify } from "../analytics";
import UserStore, { FeatureVariants } from "./UserStore";

interface APIProps {
  userStore: UserStore;
}

interface RequestProps {
  path: string;
  method: "GET" | "POST" | "DELETE";
  body?: Record<string, unknown>;
}

interface BootstrapResponse {
  csrf: string;
  segmentUserId: string;
  knownExperiments: FeatureVariants;
  dashboardURL: string;

  canAccessCaseTriage: boolean;
  canAccessLeadershipDashboard: boolean;
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

class API {
  bootstrapped?: boolean;

  bootstrapping?: Promise<void>;

  csrfToken: string;

  dashboardURL: string;

  userStore: UserStore;

  constructor({ userStore }: APIProps) {
    this.csrfToken = "";
    this.dashboardURL = "";
    this.userStore = userStore;
  }

  bootstrap(): Promise<void> {
    this.bootstrapping =
      this.bootstrapping ||
      this.get<BootstrapResponse>(BOOTSTRAP_ROUTE).then(
        ({
          csrf,
          segmentUserId,
          knownExperiments: featureVariants,
          dashboardURL,
          canAccessCaseTriage,
          canAccessLeadershipDashboard,
        }) => {
          this.csrfToken = csrf;
          identify(segmentUserId);
          this.dashboardURL = dashboardURL;
          this.userStore.setFeatureVariants(featureVariants);
          this.userStore.setCaseTriageAccess(canAccessCaseTriage);
          this.userStore.setLeadershipDashboardAccess(
            canAccessLeadershipDashboard
          );

          Sentry.setUser({ id: segmentUserId });
          Sentry.setTag("app.version", this.userStore.currentVersion);

          this.bootstrapped = true;
        }
      );

    return this.bootstrapping;
  }

  async request<T>({ path, method, body }: RequestProps): Promise<T> {
    try {
      // Defer all requests until the API client has bootstrapped itself
      if (!this.bootstrapped && path !== BOOTSTRAP_ROUTE) {
        await this.bootstrap();
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
        this.bootstrapped = false;
        return this.request<T>({ path, method, body });
      }

      if (response.status === 401 && json.code === "no_case_triage_access") {
        this.userStore.setCaseTriageAccess(false);
      }

      if (isErrorResponse(json)) {
        throw json;
      }

      return json;
    } catch (err) {
      Sentry.captureException(err);
      throw err;
    }
  }

  async delete<T>(path: string): Promise<T> {
    return this.request({ path, method: "DELETE" });
  }

  async get<T>(path: string): Promise<T> {
    return this.request({ path, method: "GET" });
  }

  async post<T>(path: string, body: Record<string, unknown>): Promise<T> {
    return this.request({ path, body, method: "POST" });
  }
}

export default API;
