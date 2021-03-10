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
import { autorun, makeAutoObservable, remove, runInAction, set } from "mobx";
import PolicyStore from "../PolicyStore";
import UserStore from "../UserStore";
import { Client, DecoratedClient, decorateClient } from "./Client";
import API from "../API";

interface ClientsStoreProps {
  api: API;
  userStore: UserStore;
  policyStore: PolicyStore;
}

export interface ClientMarkedInProgress {
  client: DecoratedClient;
  wasPositiveAction: boolean;
}

class ClientsStore {
  api: API;

  isLoading?: boolean;

  activeClient?: DecoratedClient;

  activeClientOffset?: number;

  clientPendingView: DecoratedClient | null;

  clients: DecoratedClient[];

  // All clients who are in progress
  inProgressClients: DecoratedClient[];

  // Clients who have been marked in-progress during this session
  clientsMarkedInProgress: Record<
    DecoratedClient["personExternalId"],
    ClientMarkedInProgress
  >;

  error?: string;

  policyStore: PolicyStore;

  userStore: UserStore;

  constructor({ api, policyStore, userStore }: ClientsStoreProps) {
    makeAutoObservable(this);
    this.api = api;
    this.userStore = userStore;

    this.clients = [];
    this.inProgressClients = [];
    this.clientPendingView = null;
    this.clientsMarkedInProgress = {};
    this.userStore = userStore;
    this.policyStore = policyStore;
    this.isLoading = false;

    autorun(() => {
      if (!this.userStore.isAuthorized) {
        return;
      }

      this.fetchClientsList();
    });
  }

  async fetchClientsList(): Promise<void> {
    this.isLoading = true;

    try {
      const clients = await this.api.get<Client[]>("/api/clients");

      runInAction(() => {
        this.isLoading = false;
        const decoratedClients = clients
          .map((client: Client) => decorateClient(client, this.policyStore))
          // Filter out clients already manged by `clientsMarkedInProgress`
          .filter(
            (client: DecoratedClient) =>
              !this.clientsMarkedInProgress[client.personExternalId]
          );

        const markedInProgressClients = Object.values(
          this.clientsMarkedInProgress
        )
          .filter(({ client }) => !client.inProgressActions?.length)
          .map(({ client }) => client);

        const upNextClients = decoratedClients
          .filter((client: DecoratedClient) => !client.inProgressActions)
          // Concatenate clients that have been moved to "In progress"
          // This allows us to render their ClientMarkedInProgress list item overlay
          .concat(markedInProgressClients);

        this.clients = upNextClients.sort(
          (self: DecoratedClient, other: DecoratedClient) => {
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

            // We both have scheduled contacts on the same day
            return 0;
          }
        );

        this.inProgressClients = decoratedClients
          .filter((client: DecoratedClient) => client.inProgressSubmissionDate)
          .concat(markedInProgressClients)
          .sort((self: DecoratedClient, other: DecoratedClient) => {
            if (
              self.inProgressSubmissionDate! > other.inProgressSubmissionDate!
            ) {
              return 1;
            }

            if (
              self.inProgressSubmissionDate! < other.inProgressSubmissionDate!
            ) {
              return -1;
            }
            return 0;
          });
      });
    } catch (error) {
      runInAction(() => {
        this.isLoading = false;
        this.error = error;
      });
    }
  }

  markAsInProgress(client: DecoratedClient, wasPositiveAction: boolean): void {
    runInAction(() => {
      const alreadyInProgress = this.wasRecentlyMarkedInProgress(client);

      if (!alreadyInProgress) {
        set(this.clientsMarkedInProgress, client.personExternalId, {
          client,
          wasPositiveAction,
        });
      }
    });
  }

  undoMarkAsInProgress(client: DecoratedClient): void {
    runInAction(() => {
      remove(this.clientsMarkedInProgress, client.personExternalId);

      // Viewing a CaseCard requires an `activeClientOffset`, which is derived from the DOM `offsetTop` layout property
      // As such, rather than viewing our client directly, we mark them as `clientPendingView`
      // so that the `ClientListCard` calculate the appropriate offset once rendered
      this.clientPendingView = client;
    });
  }

  wasRecentlyMarkedInProgress(
    client: DecoratedClient
  ): ClientMarkedInProgress | undefined {
    return this.clientsMarkedInProgress[client.personExternalId];
  }

  view(client: DecoratedClient | undefined = undefined, offset = 0): void {
    this.activeClient = client;
    this.activeClientOffset = offset;
    this.clientPendingView = null;
  }
}

export default ClientsStore;
