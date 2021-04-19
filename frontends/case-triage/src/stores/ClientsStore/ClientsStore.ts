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
import { autorun, makeAutoObservable, runInAction } from "mobx";
import { parse } from "query-string";
import PolicyStore from "../PolicyStore";
import UserStore from "../UserStore";
import { Client, DecoratedClient, decorateClient } from "./Client";

import API from "../API";
import { trackPersonSelected } from "../../analytics";
import { caseInsensitiveIncludes } from "../../utils";

interface ClientsStoreProps {
  api: API;
  userStore: UserStore;
  policyStore: PolicyStore;
}

class ClientsStore {
  api: API;

  isLoading?: boolean;

  activeClient?: DecoratedClient;

  activeClientOffset: number;

  clientSearchString: string;

  clientPendingView: DecoratedClient | null;

  unfilteredClients: DecoratedClient[];

  clients: DecoratedClient[];

  error?: string;

  policyStore: PolicyStore;

  userStore: UserStore;

  constructor({ api, policyStore, userStore }: ClientsStoreProps) {
    makeAutoObservable(this);
    this.api = api;
    this.userStore = userStore;

    this.unfilteredClients = [];
    this.clients = [];
    this.activeClientOffset = 0;
    this.clientPendingView = null;
    this.userStore = userStore;
    this.policyStore = policyStore;
    this.isLoading = false;

    const searchParam = parse(window.location.search).search;
    this.clientSearchString = Array.isArray(searchParam)
      ? searchParam[0]
      : searchParam || "";

    const checkAuthAndFetchClients = () => {
      if (!this.userStore.isAuthorized) {
        return;
      }

      this.fetchClientsList();
    };

    autorun(checkAuthAndFetchClients);
    // Re-fetch once an hour for long-lived sessions.
    setInterval(checkAuthAndFetchClients, 3600 * 1000);
  }

  async fetchClientsList(): Promise<void> {
    this.isLoading = true;

    try {
      const clients = await this.api.get<Client[]>("/api/clients");

      runInAction(() => {
        this.isLoading = false;

        this.unfilteredClients = clients
          .map((client) => decorateClient(client, this.policyStore))
          .sort((self: DecoratedClient, other: DecoratedClient) => {
            // No upcoming contact recommended. Shift myself to the right
            if (!self.nextFaceToFaceDate) {
              return 1;
            }
            // I have upcoming contact recommended, but they do not. Shift myself left
            if (!other.nextFaceToFaceDate) {
              return -1;
            }
            // My next face to face is before theirs. Shift myself left
            if (self.nextFaceToFaceDate < other.nextFaceToFaceDate) {
              return -1;
            }
            // Their face to face date is before mine. Shift them left
            if (self.nextFaceToFaceDate > other.nextFaceToFaceDate) {
              return 1;
            }

            // If the sorting dates are the same, sort by external id so the sort is stable.
            if (self.personExternalId < other.personExternalId) {
              return -1;
            }
            if (self.personExternalId > other.personExternalId) {
              return 1;
            }

            // We both have scheduled contacts on the same day
            return 0;
          });

        this.filterClients(this.clientSearchString);
      });
    } catch (error) {
      runInAction(() => {
        this.isLoading = false;
        this.error = error;
      });
    }
  }

  filterClients(searchString: string): void {
    runInAction(() => {
      this.clientSearchString = searchString;

      // Set query params
      const queryParams = new URLSearchParams(window.location.search);
      if (queryParams.get("search") !== this.clientSearchString) {
        if (this.clientSearchString !== "") {
          queryParams.set("search", this.clientSearchString);
          window.history.replaceState(null, "", `?${queryParams.toString()}`);
        } else {
          window.history.replaceState(null, "", window.location.pathname);
        }
      }

      this.clients = this.unfilteredClients.filter((client) =>
        this.isVisible(client)
      );
      if (this.activeClient) {
        this.clientPendingView = this.activeClient;
      }
    });
  }

  isVisible({ name }: DecoratedClient): boolean {
    return caseInsensitiveIncludes(name, this.clientSearchString);
  }

  view(client: DecoratedClient | undefined = undefined, offset = 0): void {
    if (this.clientPendingView === null && client) {
      trackPersonSelected(client);
    }

    this.activeClient =
      this.clientPendingView !== null && !client
        ? this.clientPendingView
        : client;
    this.activeClientOffset = offset;
    this.clientPendingView = null;
  }
}

export default ClientsStore;
