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
export enum OpportunityDeferralType {
  REMINDER = "REMINDER",
  ACTION_TAKEN = "ACTION_TAKEN",
  INCORRECT_DATA = "INCORRECT_DATA",
}

export enum OpportunityType {
  OVERDUE_DOWNGRADE = "OVERDUE_DOWNGRADE",
}

export const OPPORTUNITY_TITLES: Record<OpportunityType, string> = {
  [OpportunityType.OVERDUE_DOWNGRADE]: "Supervision level mismatch",
};

export const OPPORTUNITY_PRIORITY: Record<
  keyof typeof OpportunityType,
  number
> = {
  [OpportunityType.OVERDUE_DOWNGRADE]: 1,
};

export const opportunityPriorityComparator = (
  self: Opportunity,
  other: Opportunity
): number => {
  const first = OPPORTUNITY_PRIORITY[self.opportunityType];
  const second = OPPORTUNITY_PRIORITY[other.opportunityType];
  if (first < second) return -1;
  if (first > second) return 1;

  // If the sorting priority is the same, sort by external id so the sort is stable.
  if (self.personExternalId < other.personExternalId) {
    return -1;
  }
  if (self.personExternalId > other.personExternalId) {
    return 1;
  }

  return 0;
};

export type Opportunity = {
  personExternalId: string;
  stateCode: string;
  supervisingOfficerExternalId: string;
  opportunityType: OpportunityType;
  opportunityMetadata: { [index: string]: unknown };
  deferredUntil?: string;
  deferralType?: OpportunityDeferralType;
  deferralId?: string;
};
