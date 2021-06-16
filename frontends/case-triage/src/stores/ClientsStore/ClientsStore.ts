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
import { action, autorun, makeAutoObservable, runInAction } from "mobx";
import { parse } from "query-string";
import PolicyStore from "../PolicyStore";
import UserStore from "../UserStore";
import { ClientData, Client } from "./Client";

import API from "../API";

import { CLIENT_LIST_KIND, ClientListBuilder } from "./ClientListBuilder";

interface ClientsStoreProps {
  api: API;
  clientListBuilder: ClientListBuilder;
  policyStore: PolicyStore;
  userStore: UserStore;
}

class ClientsStore {
  private api: API;

  private clients: Client[];

  private clientListBuilder: ClientListBuilder;

  isLoading?: boolean;

  activeClient?: Client;

  activeClientOffset: number;

  private showClientCardIfVisible = false;

  clientSearchString: string;

  clientPendingView: Client | null;

  clientPendingAnimation: boolean;

  error?: string;

  policyStore: PolicyStore;

  userStore: UserStore;

  constructor({
    api,
    clientListBuilder,
    policyStore,
    userStore,
  }: ClientsStoreProps) {
    makeAutoObservable(this, {
      view: action,
    });
    this.api = api;
    this.userStore = userStore;

    this.activeClientOffset = 0;
    this.clientListBuilder = clientListBuilder;
    this.clients = [];

    this.clientPendingView = null;
    this.policyStore = policyStore;
    this.userStore = userStore;
    this.clientPendingAnimation = false;
    this.isLoading = false;

    const searchParam = parse(window.location.search).search;
    this.clientSearchString = Array.isArray(searchParam)
      ? searchParam[0]
      : searchParam || "";

    const checkAuthAndFetchClients = () => {
      if (!this.userStore.isAuthorized || this.userStore.isLoading) {
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
      const clients = await this.api.get<ClientData[]>("/api/clients");

      runInAction(() => {
        this.isLoading = false;

        const { api } = this;
        this.clients = clients.map((client) =>
          Client.build({ api, client, clientsStore: this })
        );
      });
    } catch (error) {
      runInAction(() => {
        this.isLoading = false;
        this.error = error;
      });
    }
  }

  get unfilteredLists(): Record<CLIENT_LIST_KIND, Client[]> {
    return this.clientListBuilder.build(this.clients);
  }

  get lists(): Record<CLIENT_LIST_KIND, Client[]> {
    const lists = {
      [CLIENT_LIST_KIND.UP_NEXT]: [
        ...this.unfilteredLists[CLIENT_LIST_KIND.UP_NEXT],
      ].filter((client) => client.isVisible),
      [CLIENT_LIST_KIND.PROCESSING_FEEDBACK]: [
        ...this.unfilteredLists[CLIENT_LIST_KIND.PROCESSING_FEEDBACK],
      ].filter((client) => client.isVisible),
    };

    return lists;
  }

  filterClients(searchString: string): void {
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

    if (this.activeClient) {
      this.setClientPendingView(this.activeClient);
    }
  }

  setClientPendingAnimation(animation: boolean): void {
    this.clientPendingAnimation = animation;
  }

  setClientPendingView(client?: Client): void {
    if (!client) {
      return;
    }

    this.clientPendingView = client;
  }

  get showClientCard(): boolean {
    let showCard = this.showClientCardIfVisible;
    if (showCard && this.activeClient) {
      showCard = this.activeClient.isVisible;
    }
    return showCard;
  }

  setShowClientCard(show: boolean): void {
    this.showClientCardIfVisible = show;
  }

  view(client: Client | undefined = undefined, offset = 0): void {
    this.activeClient =
      this.clientPendingView !== null && !client
        ? this.clientPendingView
        : client;

    this.setShowClientCard(Boolean(client));

    this.activeClientOffset = offset;

    this.clientPendingView = null;
  }
}

export default ClientsStore;
