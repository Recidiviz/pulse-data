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
import moment from "moment";
import API from "../API";
import RootStore from "../RootStore";
import UserStore, { KNOWN_EXPERIMENTS } from "../UserStore";
import {
  Opportunity,
  OpportunityData,
  opportunityPriorityComparator,
  OpportunityType,
} from "./Opportunity";

interface OpportunityStoreProps {
  api: API;
  rootStore: RootStore;
  userStore: UserStore;
}

class OpportunityStore {
  api: API;

  rootStore: RootStore;

  isLoading?: boolean;

  opportunitiesFetched?: Opportunity[];

  error?: string;

  userStore: UserStore;

  constructor({ api, rootStore, userStore }: OpportunityStoreProps) {
    makeAutoObservable(this);

    this.api = api;
    this.rootStore = rootStore;
    this.userStore = userStore;
    this.isLoading = false;

    const checkAuthAndFetch = () => {
      if (!this.userStore.isAuthorized) {
        return;
      }

      this.fetchOpportunities();
    };
    autorun(checkAuthAndFetch);
    // refetch for long-lived sessions
    setInterval(checkAuthAndFetch, rootStore.refetchInterval);
  }

  async fetchOpportunities(): Promise<void> {
    this.isLoading = true;

    try {
      await runInAction(async () => {
        this.isLoading = false;
        let opportunities = (
          await this.api.get<OpportunityData[]>("/api/opportunities")
        ).map((data) => new Opportunity(data));

        opportunities = opportunities.filter((opportunity: Opportunity) => {
          return opportunity.opportunityType in OpportunityType;
        });

        runInAction(() => {
          this.opportunitiesFetched = opportunities;
        });
      });
    } catch (error) {
      runInAction(() => {
        this.isLoading = false;
        this.error = error;
      });
    }
  }

  get opportunities(): Opportunity[] | undefined {
    // TODO(#7635): filter out snoozed and incorrect opportunities
    return this.opportunitiesFetched
      ?.slice()
      .sort((a, b) => a.priority - b.priority);
  }

  get opportunitiesByPerson(): Record<string, Opportunity[] | undefined> {
    const { opportunities } = this;
    if (opportunities) {
      return opportunities.reduce((memo, opportunity) => {
        const list = memo[opportunity.personExternalId] || [];

        return {
          ...memo,
          [opportunity.personExternalId]: [...list, opportunity],
        };
      }, {} as Record<string, Opportunity[]>);
    }
    return {};
  }

  *createOpportunityDeferral(
    opportunity: Opportunity,
    deferUntil: moment.Moment
  ): Generator {
    yield this.api.post("/api/opportunity_deferrals", {
      personExternalId: opportunity.personExternalId,
      opportunityType: opportunity.opportunityType,
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

    /* eslint-disable no-param-reassign */
    opportunity.deferredUntil = undefined;
    opportunity.deferralId = undefined;
    opportunity.deferralType = undefined;
    /* eslint-enable no-param-reassign */

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

    let baseOpportunities =
      this.opportunitiesByPerson[personExternalId]?.slice();
    if (!this.userStore.isInExperiment(KNOWN_EXPERIMENTS.ProfileV2)) {
      baseOpportunities = baseOpportunities?.filter(
        (opp) => opp.opportunityType === OpportunityType.OVERDUE_DOWNGRADE
      );
    }
    return baseOpportunities?.sort(opportunityPriorityComparator)[0];
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
