import { DecoratedClient, getNextContactDate } from "./Client";
import OpportunityStore, { Opportunity } from "../OpportunityStore";
import { opportunityPriorityComparator } from "../OpportunityStore/Opportunity";
import PolicyStore from "../PolicyStore";
import { CaseUpdateActionType } from "../CaseUpdatesStore";

const ClientListContactComparator = (
  self: DecoratedClient,
  other: DecoratedClient
) => {
  const selfNextContactDate = getNextContactDate(self);
  const otherNextContactDate = getNextContactDate(other);

  // No upcoming contact recommended. Shift myself to the right
  if (!selfNextContactDate) {
    return 1;
  }
  // I have upcoming contact recommended, but they do not. Shift myself left
  if (!otherNextContactDate) {
    return -1;
  }
  // My next face to face is before theirs. Shift myself left
  if (selfNextContactDate < otherNextContactDate) {
    return -1;
  }
  // Their face to face date is before mine. Shift them left
  if (selfNextContactDate > otherNextContactDate) {
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

  buildBuckets(
    clients: DecoratedClient[]
  ): Record<CLIENT_BUCKET, DecoratedClient[]> {
    const buckets: Record<CLIENT_BUCKET, DecoratedClient[]> = {
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

  build(
    clients: DecoratedClient[]
  ): Record<CLIENT_LIST_KIND, DecoratedClient[]> {
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
