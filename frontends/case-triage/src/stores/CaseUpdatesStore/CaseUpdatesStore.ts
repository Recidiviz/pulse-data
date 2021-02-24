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

interface CaseUpdatesStoreProps {
  clientsStore: ClientsStore;
  userStore: UserStore;
}

class CaseUpdatesStore {
  isLoading: boolean;

  clientsStore: ClientsStore;

  userStore: UserStore;

  constructor({ clientsStore, userStore }: CaseUpdatesStoreProps) {
    makeAutoObservable(this);

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

    const token = await this.userStore.getTokenSilently({
      audience: "https://case-triage.recidiviz.org/api",
      scope: "email",
    });

    const response = await fetch("/api/record_client_action", {
      headers: {
        Authorization: `Bearer ${token}`,
        "Content-Type": `application/json`,
      },
      method: "POST",
      body: JSON.stringify({
        personExternalId: client.personExternalId,
        actions,
        otherText,
      }),
    }).then(() => {
      this.clientsStore.view();
      const wasPositiveAction = actions.some((action) =>
        POSITIVE_CASE_UPDATE_ACTIONS.includes(action)
      );
      this.clientsStore.markAsInProgress(client, wasPositiveAction);
      this.clientsStore.fetchClientsList();
    });

    runInAction(() => {
      this.isLoading = false;
    });
  }
}

export default CaseUpdatesStore;
