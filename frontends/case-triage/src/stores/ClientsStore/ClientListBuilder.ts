import type { Moment } from "moment";
import { Client } from "./Client";
import OpportunityStore, { Opportunity } from "../OpportunityStore";
import { opportunityPriorityComparator } from "../OpportunityStore/Opportunity";
import PolicyStore from "../PolicyStore";
import { CaseUpdateActionType } from "../CaseUpdatesStore";

type ClientSortFn = (a: Client, b: Client) => -1 | 0 | 1;

function getDateComparator(
  accessor: (c: Client) => Moment | null
): ClientSortFn {
  return (self, other) => {
    const dateSelf = accessor(self);
    const dateOther = accessor(other);

    // No date for me. Shift myself to the right
    if (!dateSelf) {
      return 1;
    }
    // I have a date, but they do not. Shift myself left
    if (!dateOther) {
      return -1;
    }
    // My date is before theirs. Shift myself left
    if (dateSelf < dateOther) {
      return -1;
    }
    // Their date is before mine. Shift them left
    if (dateSelf > dateOther) {
      return 1;
    }

    // If the sorting dates are the same, sort by external id so the sort is stable.
    if (self.personExternalId < other.personExternalId) {
      return -1;
    }
    if (self.personExternalId > other.personExternalId) {
      return 1;
    }

    // Our tie cannot be broken
    return 0;
  };
}

function getReverseDateComparator(
  accessor: (c: Client) => Moment | null
): ClientSortFn {
  const sortFn = getDateComparator(accessor);
  return (a, b) => {
    return sortFn(b, a);
  };
}

/**
 * Sorts chronologically by next contact date.
 */
export const ClientListContactComparator = getDateComparator(
  (c) => c.nextContactDate
);

/**
 * Sorts chronologically by next risk assessment date.
 */
export const ClientListAssessmentComparator = getDateComparator(
  (c) => c.nextAssessmentDate
);

/**
 * Sorts reverse-chronologically by supervision start date.
 */
export const ClientListSupervisionStartComparator = getReverseDateComparator(
  (c) => c.supervisionStartDate
);

/**
 * Sorts clients by the priority of their alerts.
 * Ties are broken by next contact date.
 */
export const ClientListPriorityComparator: ClientSortFn = (a, b) => {
  // NOTE: assumes alerts are in priority order
  for (let i = 0; i < Math.max(a.alerts.length, b.alerts.length); i += 1) {
    const alertA = a.alerts[i];
    const alertB = b.alerts[i];
    // first alert with higher priority wins
    if (alertA && alertB) {
      if (alertA.priority < alertB.priority) return -1;
      if (alertA.priority > alertB.priority) return 1;
    }
    // if we've exhausted all matching pairs of alerts,
    // if one client still has more then it wins
    if (alertA && !alertB) return -1;
    if (alertB && !alertA) return 1;
  }

  return ClientListContactComparator(a, b);
};

interface ClientListBuilderProps {
  opportunityStore: OpportunityStore;
  policyStore: PolicyStore;
}

export enum CLIENT_LIST_KIND {
  UP_NEXT = "UP_NEXT",
  PROCESSING_FEEDBACK = "PROCESSING_FEEDBACK",
}

export enum CLIENT_BUCKET {
  TOP_OPPORTUNITY = "TOP_OPPORTUNITY",
  CONTACT_CLIENTS = "CONTACT_CLIENTS",
  IN_CUSTODY = "IN_CUSTODY",
  NOT_ON_CASELOAD = "NOT_ON_CASELOAD",
}

class ClientListBuilder {
  private policyStore: PolicyStore;

  private opportunityStore: OpportunityStore;

  constructor({ opportunityStore, policyStore }: ClientListBuilderProps) {
    this.opportunityStore = opportunityStore;
    this.policyStore = policyStore;
  }

  buildBuckets(clients: Client[]): Record<CLIENT_BUCKET, Client[]> {
    const buckets: Record<CLIENT_BUCKET, Client[]> = {
      [CLIENT_BUCKET.TOP_OPPORTUNITY]: [],
      [CLIENT_BUCKET.CONTACT_CLIENTS]: [],
      [CLIENT_BUCKET.IN_CUSTODY]: [],
      [CLIENT_BUCKET.NOT_ON_CASELOAD]: [],
    };

    return clients.reduce((memo, client) => {
      const notOnCaseload = client.hasInProgressUpdate(
        CaseUpdateActionType.NOT_ON_CASELOAD
      );
      const currentlyInCustody = client.hasInProgressUpdate(
        CaseUpdateActionType.CURRENTLY_IN_CUSTODY
      );
      const opportunity = this.opportunityStore.getTopOpportunityForClient(
        client.personExternalId
      );

      if (notOnCaseload) {
        memo[CLIENT_BUCKET.NOT_ON_CASELOAD].push(client);
      } else if (currentlyInCustody) {
        memo[CLIENT_BUCKET.IN_CUSTODY].push(client);
      } else if (opportunity && !opportunity.deferredUntil) {
        memo[CLIENT_BUCKET.TOP_OPPORTUNITY].push(client);
      } else {
        memo[CLIENT_BUCKET.CONTACT_CLIENTS].push(client);
      }

      return memo;
    }, buckets);
  }

  build(clients: Client[]): Record<CLIENT_LIST_KIND, Client[]> {
    const buckets = this.buildBuckets(clients);

    return {
      [CLIENT_LIST_KIND.UP_NEXT]: [
        ...buckets[CLIENT_BUCKET.TOP_OPPORTUNITY].sort((self, other) =>
          opportunityPriorityComparator(
            this.opportunityStore.getTopOpportunityForClient(
              self.personExternalId
            ) as Opportunity,
            this.opportunityStore.getTopOpportunityForClient(
              other.personExternalId
            ) as Opportunity
          )
        ),
        ...buckets[CLIENT_BUCKET.CONTACT_CLIENTS].sort(
          ClientListContactComparator
        ),
        ...buckets[CLIENT_BUCKET.IN_CUSTODY].sort(ClientListContactComparator),
      ],
      [CLIENT_LIST_KIND.PROCESSING_FEEDBACK]: [
        ...buckets[CLIENT_BUCKET.NOT_ON_CASELOAD],
      ],
    };
  }
}
export { ClientListBuilder };
