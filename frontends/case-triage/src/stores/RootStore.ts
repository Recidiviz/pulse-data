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
import { configure } from "mobx";
import ClientsStore from "./ClientsStore";
import OpportunityStore from "./OpportunityStore";
import PolicyStore from "./PolicyStore";
import UserStore from "./UserStore";
import CaseUpdatesStore from "./CaseUpdatesStore";
import API from "./API";
import { ClientListBuilder } from "./ClientsStore/ClientListBuilder";

configure({
  useProxies: "never",
});

export default class RootStore {
  api: API;

  caseUpdatesStore: CaseUpdatesStore;

  clientsStore: ClientsStore;

  opportunityStore: OpportunityStore;

  policyStore: PolicyStore;

  userStore: UserStore;

  // refresh data once an hour for long-lived sessions
  refetchInterval = 3600 * 1000;

  constructor() {
    this.userStore = UserStore.build();
    this.api = new API({ userStore: this.userStore });

    this.policyStore = new PolicyStore({
      api: this.api,
      userStore: this.userStore,
    });

    this.opportunityStore = new OpportunityStore({
      rootStore: this,
      api: this.api,
      userStore: this.userStore,
    });

    const clientListBuilder = new ClientListBuilder({
      opportunityStore: this.opportunityStore,
      policyStore: this.policyStore,
    });

    this.clientsStore = new ClientsStore({
      api: this.api,
      clientListBuilder,
      rootStore: this,
    });

    this.caseUpdatesStore = new CaseUpdatesStore({
      api: this.api,
      clientsStore: this.clientsStore,
      userStore: this.userStore,
    });
  }
}
