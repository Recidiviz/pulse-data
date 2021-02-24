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
import UserStore from "../UserStore";
import { Client, DecoratedClient, decorateClient } from "./Client";

interface ClientsStoreProps {
  userStore: UserStore;
}

export interface ClientMarkedInProgress {
  client: DecoratedClient;
  wasPositiveAction: boolean;
}

class ClientsStore {
  isLoading?: boolean;

  activeClient?: DecoratedClient;

  activeClientOffset?: number;

  clients: DecoratedClient[];

  // All clients who are in progress
  inProgressClients: DecoratedClient[];

  // Clients who have been marked in-progress during this session
  clientsMarkedInProgress: Record<
    DecoratedClient["personExternalId"],
    ClientMarkedInProgress
  >;

  error?: string;

  userStore: UserStore;

  constructor({ userStore }: ClientsStoreProps) {
    makeAutoObservable(this);

    this.clients = [];
    this.inProgressClients = [];
    this.clientsMarkedInProgress = {};
    this.userStore = userStore;
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

    if (!this.userStore.getTokenSilently) {
      return;
    }

    const token = await this.userStore.getTokenSilently({
      audience: "https://case-triage.recidiviz.org/api",
      scope: "email",
    });

    const response = await fetch("/api/clients", {
      headers: {
        Authorization: `Bearer ${token}`,
      },
    });

    try {
      const clients = await response.json();
      runInAction(() => {
        this.isLoading = false;
        const decoratedClients = clients.map((client: Client) =>
          decorateClient(client)
        );

        const upNextClients = decoratedClients
          .filter((client: DecoratedClient) => !client.inProgressActions)
          // Concatenate clients that have been moved to "In progress"
          // This allows us to render their ClientMarkedInProgress list item overlay
          .concat(
            Object.values(this.clientsMarkedInProgress).map(
              ({ client }) => client
            )
          );

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
        this.clientsMarkedInProgress[client.personExternalId] = {
          client,
          wasPositiveAction,
        };
      }
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
  }
}

export default ClientsStore;
