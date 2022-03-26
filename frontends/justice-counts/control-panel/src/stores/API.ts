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

import { makeAutoObservable } from "mobx";

import { AuthStore } from "../components/Auth";

interface RequestProps {
  path: string;
  method: "GET" | "POST";
  body?: Record<string, unknown>;
}

class API {
  authStore: AuthStore;

  constructor(authStore: AuthStore) {
    makeAutoObservable(this);

    this.authStore = authStore;
  }

  async request({ path, method, body }: RequestProps): Promise<void | Error> {
    try {
      if (!this.authStore.getToken) {
        return Promise.reject();
      }

      const token = await this.authStore.getToken();

      const response = await fetch(path, {
        body: method !== "GET" ? JSON.stringify(body) : null,
        method,
        headers: {
          Authorization: `Bearer ${token}`,
          "Content-Type": "application/json",
        },
      });

      return response.json();
    } catch (error) {
      if (error instanceof Error) {
        return error;
      }
    }
  }
}

export default API;
