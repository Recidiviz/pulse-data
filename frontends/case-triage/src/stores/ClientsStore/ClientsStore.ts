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
import { Client, decorateClient, DecoratedClient } from "./Client";

import API from "../API";
import { caseInsensitiveIncludes } from "../../utils";
import { CLIENT_LIST_KIND, ClientListBuilder } from "./ClientListBuilder";

interface ClientsStoreProps {
  api: API;
  clientListBuilder: ClientListBuilder;
  policyStore: PolicyStore;
  userStore: UserStore;
}

class ClientsStore {
  private api: API;

  private clients: DecoratedClient[];

  private clientListBuilder: ClientListBuilder;

  isLoading?: boolean;

  activeClient?: DecoratedClient;

  activeClientOffset: number;

  clientSearchString: string;

  clientPendingView: DecoratedClient | null;

  clientPendingAnimation: boolean;

  lists: Record<CLIENT_LIST_KIND, DecoratedClient[]>;

  unfilteredLists: Record<CLIENT_LIST_KIND, DecoratedClient[]>;

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
    this.lists = {
      [CLIENT_LIST_KIND.UP_NEXT]: [],
      [CLIENT_LIST_KIND.PROCESSING_FEEDBACK]: [],
    };
    this.unfilteredLists = {
      [CLIENT_LIST_KIND.UP_NEXT]: [],
      [CLIENT_LIST_KIND.PROCESSING_FEEDBACK]: [],
    };

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
      const clients = await this.api.get<Client[]>("/api/clients");

      runInAction(() => {
        this.isLoading = false;

        this.clients = clients.map((client) =>
          decorateClient(client, this.policyStore)
        );

        this.updateClientsList();
      });
    } catch (error) {
      runInAction(() => {
        this.isLoading = false;
        this.error = error;
      });
    }
  }

  updateClientsList(): void {
    this.unfilteredLists = this.clientListBuilder.build(this.clients);
    this.lists = {
      [CLIENT_LIST_KIND.UP_NEXT]: [
        ...this.unfilteredLists[CLIENT_LIST_KIND.UP_NEXT],
      ],
      [CLIENT_LIST_KIND.PROCESSING_FEEDBACK]: [
        ...this.unfilteredLists[CLIENT_LIST_KIND.PROCESSING_FEEDBACK],
      ],
    };
    this.filterClients(this.clientSearchString);
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

      this.lists[CLIENT_LIST_KIND.UP_NEXT] = this.unfilteredLists[
        CLIENT_LIST_KIND.UP_NEXT
      ].filter((client) => this.isVisible(client));

      this.lists[CLIENT_LIST_KIND.PROCESSING_FEEDBACK] = this.unfilteredLists[
        CLIENT_LIST_KIND.PROCESSING_FEEDBACK
      ].filter((client) => this.isVisible(client));

      if (this.activeClient) {
        this.setClientPendingView(this.activeClient);
      }
    });
  }

  isVisible({ name }: DecoratedClient): boolean {
    return caseInsensitiveIncludes(name, this.clientSearchString);
  }

  isActive({ personExternalId }: DecoratedClient): boolean {
    return (
      this.clientPendingView?.personExternalId === personExternalId ||
      this.activeClient?.personExternalId === personExternalId
    );
  }

  setClientPendingAnimation(animation: boolean): void {
    this.clientPendingAnimation = animation;
  }

  setClientPendingView(client?: DecoratedClient): void {
    if (!client) {
      return;
    }

    this.clientPendingView = client;
  }

  view(client: DecoratedClient | undefined = undefined, offset = 0): void {
    this.activeClient =
      this.clientPendingView !== null && !client
        ? this.clientPendingView
        : client;

    this.activeClientOffset = offset;

    this.clientPendingView = null;
  }
}

export default ClientsStore;
