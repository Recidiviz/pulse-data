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
import { makeAutoObservable } from "mobx";
import { CaseUpdateActionType } from "./CaseUpdates";
import ClientsStore, { DecoratedClient } from "../ClientsStore";
import UserStore from "../UserStore";

import API from "../API";
import {
  trackPersonActionRemoved,
  trackPersonActionTaken,
} from "../../analytics";

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

  async recordAction(
    client: DecoratedClient,
    actionType: CaseUpdateActionType,
    comment?: string
  ): Promise<void> {
    this.isLoading = true;

    if (!this.userStore.getTokenSilently) {
      return;
    }

    await this.api.post("/api/case_updates", {
      personExternalId: client.personExternalId,
      actionType,
      comment,
    });
    trackPersonActionTaken(client, actionType);

    await this.clientsStore.fetchClientsList();
    this.isLoading = false;
  }

  async removeAction(
    client: DecoratedClient,
    updateId: string,
    actionType: CaseUpdateActionType
  ): Promise<void> {
    this.isLoading = true;

    if (!this.userStore.getTokenSilently) {
      return;
    }

    await this.api.delete(`/api/case_updates/${updateId}`);
    trackPersonActionRemoved(client, updateId, actionType);

    await this.clientsStore.fetchClientsList();
    this.isLoading = false;
  }
}

export default CaseUpdatesStore;
