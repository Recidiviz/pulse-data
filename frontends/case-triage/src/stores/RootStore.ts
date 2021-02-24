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
import PolicyStore from "./PolicyStore";
import UserStore from "./UserStore";
import CaseUpdatesStore from "./CaseUpdatesStore";

configure({
  useProxies: "never",
});

export default class RootStore {
  caseUpdatesStore: CaseUpdatesStore;

  clientsStore: ClientsStore;

  policyStore: PolicyStore;

  userStore: UserStore;

  constructor() {
    this.userStore = UserStore.build();
    this.clientsStore = new ClientsStore({ userStore: this.userStore });
    this.caseUpdatesStore = new CaseUpdatesStore({
      clientsStore: this.clientsStore,
      userStore: this.userStore,
    });
    this.policyStore = new PolicyStore({ userStore: this.userStore });
  }
}
