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
import { parse } from "query-string";
import PolicyStore from "../PolicyStore";
import UserStore from "../UserStore";
import {
  Client,
  ClientListSubsection,
  DecoratedClient,
  decorateClient,
} from "./Client";

import API from "../API";
import { trackPersonSelected } from "../../analytics";
import { CaseUpdateActionType } from "../CaseUpdatesStore";
import { caseInsensitiveIncludes } from "../../utils";

interface ClientsStoreProps {
  api: API;
  userStore: UserStore;
  policyStore: PolicyStore;
}

export interface ClientMarkedInProgress {
  client: DecoratedClient;
  listSubsection: ClientListSubsection;
  wasPositiveAction: boolean;
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

  // All clients who are in progress
  unfilteredInProgressClients: DecoratedClient[];

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

    this.unfilteredClients = [];
    this.clients = [];
    this.unfilteredInProgressClients = [];
    this.inProgressClients = [];
    this.activeClientOffset = 0;
    this.clientPendingView = null;
    this.clientsMarkedInProgress = {};
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

        const decoratedClients = clients.map((client) =>
          decorateClient(client, this.policyStore)
        );

        // This contains the list of clients who have transitioned from being
        // up next to in-progress and are no longer in the list served by the API.
        //
        // We use this to represent users in the up next list who should have a transition
        // overlay.
        const transitionedInProgressClients = Object.values(
          this.clientsMarkedInProgress
        )
          .filter(({ listSubsection }) => listSubsection === "ACTIVE")
          .map(({ client }) => client);

        const upNextClients = decoratedClients
          .filter((client) => !client.inProgressActions?.length)
          .filter(
            (client) =>
              // If it has been marked in progress in another section, exclude
              // it from up next
              !this.clientsMarkedInProgress[client.personExternalId] ||
              this.clientsMarkedInProgress[client.personExternalId]
                .listSubsection === "ACTIVE"
          )
          .concat(transitionedInProgressClients);

        this.unfilteredClients = upNextClients.sort(
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

            // If the sorting dates are the same, sort by external id so the sort is stable.
            if (self.personExternalId < other.personExternalId) {
              return -1;
            }
            if (self.personExternalId > other.personExternalId) {
              return 1;
            }

            // We both have scheduled contacts on the same day
            return 0;
          }
        );

        this.unfilteredInProgressClients = decoratedClients
          .filter((client: DecoratedClient) => client.inProgressSubmissionDate)
          .sort((self: DecoratedClient, other: DecoratedClient) => {
            if (
              // eslint-disable-next-line @typescript-eslint/no-non-null-assertion
              self.inProgressSubmissionDate! > other.inProgressSubmissionDate!
            ) {
              return 1;
            }

            if (
              // eslint-disable-next-line @typescript-eslint/no-non-null-assertion
              self.inProgressSubmissionDate! < other.inProgressSubmissionDate!
            ) {
              return -1;
            }

            // If the sorting dates are the same, sort by external id so the sort is stable.
            if (self.personExternalId < other.personExternalId) {
              return -1;
            }
            if (self.personExternalId > other.personExternalId) {
              return 1;
            }

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
      this.inProgressClients = this.unfilteredInProgressClients.filter(
        (client) => this.isVisible(client)
      );

      if (this.activeClient) {
        this.clientPendingView = this.activeClient;
      }
    });
  }

  isVisible({ name }: DecoratedClient): boolean {
    return caseInsensitiveIncludes(name, this.clientSearchString);
  }

  markAsInProgress(
    client: DecoratedClient,
    inProgressActions: CaseUpdateActionType[],
    listSubsection: ClientListSubsection,
    wasPositiveAction: boolean
  ): void {
    runInAction(() => {
      set(this.clientsMarkedInProgress, client.personExternalId, {
        client: {
          ...client,
          inProgressActions,
          previousInProgressActions: client.inProgressActions,
        },
        listSubsection,
        wasPositiveAction,
      });
    });
  }

  undoMarkAsInProgress(client: DecoratedClient): void {
    runInAction(() => {
      const storedProgress = this.clientsMarkedInProgress[
        client.personExternalId
      ];
      remove(this.clientsMarkedInProgress, client.personExternalId);

      // Viewing a CaseCard requires an `activeClientOffset`, which is derived from the DOM `offsetTop` layout property
      // As such, rather than viewing our client directly, we mark them as `clientPendingView`
      // so that the `ClientListCard` calculate the appropriate offset once rendered
      const storedClientActions =
        storedProgress?.client.previousInProgressActions;
      this.clientPendingView = {
        ...client,
        inProgressActions: storedClientActions,
        previousInProgressActions: undefined,
      };
    });
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
