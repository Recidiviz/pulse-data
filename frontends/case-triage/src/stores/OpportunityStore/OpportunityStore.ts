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
import { autorun, makeAutoObservable, remove, runInAction } from "mobx";
import moment from "moment";
import {
  Opportunity,
  OPPORTUNITY_PRIORITY,
  opportunityPriorityComparator,
  OpportunityType,
} from "./Opportunity";
import UserStore from "../UserStore";
import API from "../API";
import { Client } from "../ClientsStore";
import RootStore from "../RootStore";

interface OpportunityStoreProps {
  api: API;
  rootStore: RootStore;
  userStore: UserStore;
}

class OpportunityStore {
  api: API;

  rootStore: RootStore;

  isLoading?: boolean;

  opportunities?: Opportunity[];

  opportunitiesByPerson?: Record<string, Opportunity[]>;

  error?: string;

  userStore: UserStore;

  constructor({ api, rootStore, userStore }: OpportunityStoreProps) {
    makeAutoObservable(this);

    this.api = api;
    this.rootStore = rootStore;
    this.userStore = userStore;
    this.isLoading = false;

    autorun(() => {
      if (!this.userStore.isAuthorized) {
        return;
      }

      this.fetchOpportunities();
    });
  }

  async fetchOpportunities(): Promise<void> {
    this.isLoading = true;

    try {
      await runInAction(async () => {
        this.isLoading = false;
        let opportunities = await this.api.get<Opportunity[]>(
          "/api/opportunities"
        );

        opportunities = opportunities.filter((opportunity: Opportunity) => {
          return (
            OPPORTUNITY_PRIORITY[opportunity.opportunityType] !== undefined
          );
        });

        runInAction(() => {
          this.opportunities = opportunities;
          this.opportunitiesByPerson = opportunities.reduce(
            (memo, opportunity) => {
              const list = memo[opportunity.personExternalId] || [];

              return {
                ...memo,
                [opportunity.personExternalId]: [...list, opportunity],
              };
            },
            {} as Record<string, Opportunity[]>
          );
        });
      });
    } catch (error) {
      runInAction(() => {
        this.isLoading = false;
        this.error = error;
      });
    }
  }

  *createOpportunityDeferral(
    client: Client,
    opportunityType: OpportunityType,
    deferUntil: moment.Moment
  ): Generator {
    yield this.api.post("/api/opportunity_deferrals", {
      personExternalId: client.personExternalId,
      opportunityType,
      deferralType: "REMINDER",
      deferUntil: deferUntil.format(),
      requestReminder: true,
    });

    yield this.fetchOpportunities();
  }

  *deleteOpportunityDeferral(opportunity: Opportunity): Generator {
    yield this.api.delete(
      `/api/opportunity_deferrals/${opportunity.deferralId}`
    );

    remove(opportunity, "deferredUntil");
    remove(opportunity, "deferralId");
    remove(opportunity, "deferralType");

    // HACK: Due to a combination of bugs in React/Chrome, the scroll position is reset to the top of the page when re-rendering the client list
    // Store scroll position and re-scroll to the position upon the next re-render
    // https://github.com/facebook/react/issues/19695#issuecomment-839079273
    // https://bugs.chromium.org/p/chromium/issues/detail?id=1208152&q=label%3AHotlist-Polish
    const scroll = window.scrollY;
    requestAnimationFrame(() => {
      window.scrollTo(window.scrollX, scroll);
    });
  }

  getTopOpportunityForClient(
    personExternalId: string
  ): Opportunity | undefined {
    if (
      !this.opportunitiesByPerson ||
      !this.opportunitiesByPerson[personExternalId]
    ) {
      return;
    }

    return this.opportunitiesByPerson[personExternalId]
      .slice()
      .sort(opportunityPriorityComparator)[0];
  }

  getOpportunitiesForClient(personExternalId: string): Opportunity[] {
    return (
      this.opportunities?.filter(
        (opportunity: Opportunity) =>
          opportunity.personExternalId === personExternalId
      ) || []
    );
  }
}

export default OpportunityStore;
