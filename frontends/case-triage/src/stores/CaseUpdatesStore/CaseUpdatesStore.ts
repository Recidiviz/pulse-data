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
import { makeAutoObservable, runInAction } from "mobx";
import {
  CaseUpdateActionType,
  POSITIVE_CASE_UPDATE_ACTIONS,
} from "./CaseUpdates";
import ClientsStore, { DecoratedClient } from "../ClientsStore";
import UserStore from "../UserStore";
import API from "../API";

interface CaseUpdatesStoreProps {
  api: API;
  clientsStore: ClientsStore;
  userStore: UserStore;
}

class CaseUpdatesStore {
  api: API;

  isLoading: boolean;

  clientsStore: ClientsStore;

  userStore: UserStore;

  constructor({ api, clientsStore, userStore }: CaseUpdatesStoreProps) {
    makeAutoObservable(this);

    this.api = api;
    this.isLoading = false;
    this.clientsStore = clientsStore;
    this.userStore = userStore;
  }

  async submit(
    client: DecoratedClient,
    actions: CaseUpdateActionType[],
    otherText: string
  ): Promise<void> {
    this.isLoading = true;

    if (!this.userStore.getTokenSilently) {
      return;
    }

    await this.api.post("/api/record_client_action", {
      personExternalId: client.personExternalId,
      actions:
        client.inProgressActions !== undefined
          ? [...client.inProgressActions, ...actions]
          : [...actions],
      otherText,
    });

    this.clientsStore.view();
    const wasPositiveAction = actions.some((action) =>
      POSITIVE_CASE_UPDATE_ACTIONS.includes(action)
    );
    this.clientsStore.markAsInProgress(client, wasPositiveAction);
    this.clientsStore.fetchClientsList();

    this.isLoading = false;
  }

  async undo(client: DecoratedClient): Promise<void> {
    /*
      Given a client, who was recently marked as in-progress, revert their CaseUpdate to its previous state
      To do so, we feed the `ClientsStore.clientsMarkedInProgress` dictionary entry, which contains the previous
      representation of the client, back into the `record_client_action` API.
     */
    if (!this.clientsStore.clientsMarkedInProgress[client.personExternalId]) {
      return;
    }

    this.isLoading = true;

    const {
      client: { inProgressActions: previousInProgressActions = [] },
    } = this.clientsStore.clientsMarkedInProgress[client.personExternalId];

    try {
      await this.api.post("/api/record_client_action", {
        personExternalId: client.personExternalId,
        actions: previousInProgressActions,
        otherText: "",
      });

      this.clientsStore.undoMarkAsInProgress(client);
      this.clientsStore.fetchClientsList();
      this.isLoading = false;
    } catch (error) {
      this.isLoading = false;
    }
  }
}

export default CaseUpdatesStore;
