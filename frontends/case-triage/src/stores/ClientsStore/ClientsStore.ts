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

class ClientsStore {
  isLoading?: boolean;

  activeClient?: DecoratedClient;

  activeClientOffset?: number;

  clients?: DecoratedClient[];

  error?: string;

  userStore: UserStore;

  constructor({ userStore }: ClientsStoreProps) {
    makeAutoObservable(this);

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
        this.clients = clients
          .map((client: Client) => decorateClient(client))
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

            // We both have scheduled contacts on the same day
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

  view(client: DecoratedClient, offset: number): void {
    this.activeClient = client;
    this.activeClientOffset = offset;
  }
}

export default ClientsStore;
