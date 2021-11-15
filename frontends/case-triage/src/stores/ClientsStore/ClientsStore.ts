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
import assertNever from "assert-never";
import { action, autorun, makeAutoObservable, runInAction, set } from "mobx";
import { parse } from "query-string";
import API, { ErrorResponse } from "../API";
import ErrorMessageStore from "../ErrorMessageStore";
import OpportunityStore from "../OpportunityStore";
import PolicyStore from "../PolicyStore";
import RootStore from "../RootStore";
import UserStore from "../UserStore";
import {
  Client,
  ClientData,
  PreferredContactMethod,
  SupervisionLevel,
} from "./Client";
import {
  CLIENT_LIST_KIND,
  ClientListAssessmentComparator,
  ClientListBuilder,
  ClientListContactComparator,
  ClientListHomeVisitComparator,
  ClientListDaysWithCurrentPOComparator,
  ClientListPriorityComparator,
  ClientListSupervisionStartComparator,
} from "./ClientListBuilder";
import { captureExceptionWithLogs } from "../../utils";

interface ClientsStoreProps {
  api: API;
  clientListBuilder: ClientListBuilder;
  rootStore: RootStore;
}

export const SortOrderList = [
  "RELEVANCE",
  "CONTACT_DATE",
  "HOME_VISIT_DATE",
  "ASSESSMENT_DATE",
  "DAYS_WITH_PO",
  "START_DATE",
] as const;
export type SortOrder = typeof SortOrderList[number];

class ClientsStore {
  private api: API;

  private clients: Client[];

  private clientListBuilder: ClientListBuilder;

  isLoading?: boolean;

  activeClient?: Client;

  activeClientOffset: number;

  private showClientCardIfVisible = false;

  clientSearchString: string;

  clientPendingView: Client | null;

  clientPendingAnimation: boolean;

  error?: string;

  sortOrder: SortOrder = "RELEVANCE";

  supervisionLevelFilter?: SupervisionLevel;

  riskLevelFilter?: SupervisionLevel;

  supervisionTypeFilter?: string;

  policyStore: PolicyStore;

  userStore: UserStore;

  opportunityStore: OpportunityStore;

  errorMessageStore: ErrorMessageStore;

  constructor({ api, clientListBuilder, rootStore }: ClientsStoreProps) {
    makeAutoObservable(this, {
      view: action,
      userStore: false,
      policyStore: false,
      opportunityStore: false,
      errorMessageStore: false,
    });
    this.api = api;

    this.activeClientOffset = 0;
    this.clientListBuilder = clientListBuilder;
    this.clients = [];

    this.clientPendingView = null;
    this.policyStore = rootStore.policyStore;
    this.userStore = rootStore.userStore;
    this.opportunityStore = rootStore.opportunityStore;
    this.errorMessageStore = rootStore.errorMessageStore;
    this.clientPendingAnimation = false;
    this.isLoading = false;

    const searchParam = parse(window.location.search).search;
    this.clientSearchString = Array.isArray(searchParam)
      ? searchParam[0]
      : searchParam || "";

    const checkAuthAndFetchClients = () => {
      if (!this.api.ready) {
        return;
      }

      this.fetchClientsList();
    };

    autorun(checkAuthAndFetchClients);
    // Re-fetch for long-lived sessions.
    setInterval(checkAuthAndFetchClients, rootStore.refetchInterval);

    // fetch events for the active client (including after a refetch)
    autorun(() => {
      const { activeClient } = this;
      if (activeClient?.needsTimelineHydration) {
        activeClient.hydrateTimeline();
      }
    });
  }

  async fetchClientsList(): Promise<void> {
    this.isLoading = true;
    try {
      const clients = await this.api.get<ClientData[]>("/api/clients");

      runInAction(() => {
        this.isLoading = false;

        const { api } = this;
        this.clients = clients.map((client) =>
          Client.build({
            api,
            client,
            clientsStore: this,
            opportunityStore: this.opportunityStore,
            policyStore: this.policyStore,
            errorMessageStore: this.errorMessageStore,
          })
        );
      });
    } catch (error) {
      runInAction(() => {
        this.isLoading = false;
        this.error = error;
        captureExceptionWithLogs(error);
      });
    }
  }

  get supervisionTypes(): string[] {
    return Array.from(
      new Set(this.clients.map((c) => c.supervisionType))
    ).sort();
  }

  get unfilteredLists(): Record<CLIENT_LIST_KIND, Client[]> {
    return this.clientListBuilder.build(this.clients);
  }

  get lists(): Record<CLIENT_LIST_KIND, Client[]> {
    return this.filteredSortedLists;
  }

  private get unsortedBuckets() {
    return this.clientListBuilder.buildBuckets(this.clients);
  }

  private get sortedActiveClients() {
    let sortFn;

    switch (this.sortOrder) {
      case "RELEVANCE":
        sortFn = ClientListPriorityComparator;
        break;
      case "ASSESSMENT_DATE":
        sortFn = ClientListAssessmentComparator;
        break;
      case "START_DATE":
        sortFn = ClientListSupervisionStartComparator;
        break;
      case "CONTACT_DATE":
        sortFn = ClientListContactComparator;
        break;
      case "HOME_VISIT_DATE":
        sortFn = ClientListHomeVisitComparator;
        break;
      case "DAYS_WITH_PO":
        sortFn = ClientListDaysWithCurrentPOComparator;
        break;
      default:
        assertNever(this.sortOrder);
    }

    return [
      ...this.unsortedBuckets.TOP_OPPORTUNITY,
      ...this.unsortedBuckets.CONTACT_CLIENTS,
    ].sort(sortFn);
  }

  private get sortedLists(): Record<CLIENT_LIST_KIND, Client[]> {
    return {
      [CLIENT_LIST_KIND.UP_NEXT]: [
        ...this.sortedActiveClients,
        ...this.unsortedBuckets.IN_CUSTODY,
      ],
      [CLIENT_LIST_KIND.PROCESSING_FEEDBACK]: [
        ...this.unsortedBuckets.NOT_ON_CASELOAD,
      ],
    };
  }

  private get filteredSortedLists(): Record<CLIENT_LIST_KIND, Client[]> {
    const filterFn = (client: Client) => {
      return (
        client.isVisible &&
        (this.supervisionLevelFilter
          ? client.supervisionLevel === this.supervisionLevelFilter
          : true) &&
        (this.riskLevelFilter
          ? client.riskLevel === this.riskLevelFilter
          : true) &&
        (this.supervisionTypeFilter
          ? client.supervisionType === this.supervisionTypeFilter
          : true)
      );
    };

    return {
      [CLIENT_LIST_KIND.UP_NEXT]: [
        ...this.sortedLists[CLIENT_LIST_KIND.UP_NEXT],
      ].filter(filterFn),
      [CLIENT_LIST_KIND.PROCESSING_FEEDBACK]: [
        ...this.sortedLists[CLIENT_LIST_KIND.PROCESSING_FEEDBACK],
      ].filter(filterFn),
    };
  }

  filterClients(searchString: string): void {
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

    if (this.activeClient) {
      this.setClientPendingView(this.activeClient);
    }
  }

  setClientPendingAnimation(animation: boolean): void {
    this.clientPendingAnimation = animation;
  }

  setClientPendingView(client?: Client): void {
    if (!client) {
      return;
    }

    this.clientPendingView = client;
  }

  get showClientCard(): boolean {
    let showCard = this.showClientCardIfVisible;
    if (showCard && this.activeClient) {
      showCard = this.activeClient.isVisible;
    }
    return showCard;
  }

  setShowClientCard(show: boolean): void {
    this.showClientCardIfVisible = show;
  }

  view(client: Client | undefined = undefined, offset = 0): void {
    this.activeClient =
      this.clientPendingView !== null && !client
        ? this.clientPendingView
        : client;

    this.setShowClientCard(Boolean(client));

    this.activeClientOffset = offset;

    this.clientPendingView = null;
  }

  updateClientPreferredContactMethod(
    client: Client,
    contactMethod: PreferredContactMethod
  ): Promise<void | ErrorResponse> {
    const { preferredContactMethod: previousContactMethod } = client;
    set(client, "preferredContactMethod", contactMethod);

    return this.api
      .post<void>("/api/set_preferred_contact_method", {
        personExternalId: client.personExternalId,
        contactMethod,
      })
      .catch((error) => {
        set(client, "preferredContactMethod", previousContactMethod);
        throw error;
      });
  }

  updateClientPreferredName(
    client: Client,
    name: string
  ): Promise<void | ErrorResponse> {
    const { preferredName: previousPreferredName } = client;
    set(client, "preferredName", name);

    return this.api
      .post<void>("/api/set_preferred_name", {
        personExternalId: client.personExternalId,
        name,
      })
      .catch((error) => {
        set(client, "preferredName", previousPreferredName);
        throw error;
      });
  }
}

export default ClientsStore;
