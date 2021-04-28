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
import UserStore from "./UserStore";
import { identify } from "../analytics";

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
}

const BOOTSTRAP_ROUTE = "/api/bootstrap";

class API {
  bootstrapped?: boolean;

  bootstrapping?: Promise<void>;

  csrfToken: string;

  userStore: UserStore;

  constructor({ userStore }: APIProps) {
    this.csrfToken = "";
    this.userStore = userStore;
  }

  bootstrap(): Promise<void> {
    this.bootstrapping =
      this.bootstrapping ||
      this.get<BootstrapResponse>(BOOTSTRAP_ROUTE).then(
        ({ csrf, segmentUserId }) => {
          this.csrfToken = csrf;
          identify(segmentUserId);
          this.bootstrapped = true;
        }
      );

    return this.bootstrapping;
  }

  async request<T>({ path, method, body }: RequestProps): Promise<T> {
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

    const reportedVersion = response.headers.get("X-Recidiviz-Current-Version");
    if (reportedVersion !== null) {
      this.userStore.recordVersion(reportedVersion, method !== "GET");
    }

    const json = await response.json();

    if (response.status === 401 && json.code === "no_app_access") {
      this.userStore.setLacksCaseTriageAuthorization(true);
    }

    return json;
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
