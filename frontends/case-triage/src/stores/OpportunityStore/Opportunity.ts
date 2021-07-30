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

import { palette } from "@recidiviz/design-system";
import assertNever from "assert-never";
import { makeAutoObservable } from "mobx";
import moment from "moment";
import { PillKind } from "../../components/Pill";
import { getTimeDifference } from "../../utils";

// =============================================================================
export enum OpportunityDeferralType {
  REMINDER = "REMINDER",
  ACTION_TAKEN = "ACTION_TAKEN",
  INCORRECT_DATA = "INCORRECT_DATA",
}

export enum OpportunityType {
  OVERDUE_DOWNGRADE = "OVERDUE_DOWNGRADE",
  EMPLOYMENT = "EMPLOYMENT",
  ASSESSMENT = "ASSESSMENT",
  CONTACT = "CONTACT",
}

const OPPORTUNITY_TITLES: Record<OpportunityType, string> = {
  [OpportunityType.OVERDUE_DOWNGRADE]: "Supervision level mismatch",
  [OpportunityType.EMPLOYMENT]: "Unemployed",
  [OpportunityType.ASSESSMENT]: "Risk assessment",
  [OpportunityType.CONTACT]: "Contact",
};

export const opportunityPriorityComparator = (
  self: Opportunity,
  other: Opportunity
): number => {
  const first = self.priority;
  const second = other.priority;
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

export type OpportunityData = {
  personExternalId: string;
  stateCode: string;
  supervisingOfficerExternalId: string;
  opportunityType: OpportunityType;
  opportunityMetadata: { [index: string]: unknown };
  deferredUntil?: string;
  deferralType?: OpportunityDeferralType;
  deferralId?: string;
};

export class Opportunity {
  personExternalId: string;

  stateCode: string;

  supervisingOfficerExternalId: string;

  opportunityType: OpportunityType;

  opportunityMetadata: { [index: string]: unknown };

  deferredUntil?: string;

  deferralType?: OpportunityDeferralType;

  deferralId?: string;

  constructor(apiData: OpportunityData) {
    this.personExternalId = apiData.personExternalId;
    this.stateCode = apiData.stateCode;
    this.supervisingOfficerExternalId = apiData.supervisingOfficerExternalId;
    this.opportunityType = apiData.opportunityType;
    this.opportunityMetadata = apiData.opportunityMetadata;
    this.deferredUntil = apiData.deferredUntil;
    this.deferralType = apiData.deferralType;
    this.deferralId = apiData.deferralId;

    makeAutoObservable(this);
  }

  get isDeferred(): boolean {
    return this.deferredUntil !== undefined;
  }

  get previewText(): string {
    switch (this.opportunityType) {
      case OpportunityType.OVERDUE_DOWNGRADE:
      case OpportunityType.EMPLOYMENT:
        return OPPORTUNITY_TITLES[this.opportunityType];
      case OpportunityType.ASSESSMENT:
      case OpportunityType.CONTACT:
        return `${
          OPPORTUNITY_TITLES[this.opportunityType]
        } ${`${this.opportunityMetadata.status}`.toLowerCase()}`;
      default:
        assertNever(this.opportunityType);
    }
  }

  get priority(): number {
    switch (this.opportunityType) {
      case OpportunityType.OVERDUE_DOWNGRADE:
        return 1;
      case OpportunityType.EMPLOYMENT:
        return 2;
      case OpportunityType.ASSESSMENT:
        return this.opportunityMetadata.status === "OVERDUE" ? 3 : 4;
      case OpportunityType.CONTACT:
        return this.opportunityMetadata.status === "OVERDUE" ? 5 : 6;
      default:
        assertNever(this.opportunityType);
    }
  }

  get dueDaysFormatted(): string {
    const { daysUntilDue } = this.opportunityMetadata;
    if (typeof daysUntilDue === "number") {
      return getTimeDifference(moment().add(daysUntilDue, "days"));
    }
    return "";
  }

  get title(): string {
    const titleBase = OPPORTUNITY_TITLES[this.opportunityType];

    switch (this.opportunityType) {
      case OpportunityType.OVERDUE_DOWNGRADE:
      case OpportunityType.EMPLOYMENT:
        return titleBase;
      case OpportunityType.CONTACT:
      case OpportunityType.ASSESSMENT: {
        if (this.opportunityMetadata.status === "OVERDUE") {
          return `${titleBase} overdue`;
        }
        return `${titleBase} due ${this.dueDaysFormatted}`;
      }
      default:
        assertNever(this.opportunityType);
    }
  }

  get tooltipText(): string | undefined {
    switch (this.opportunityType) {
      case OpportunityType.OVERDUE_DOWNGRADE:
      case OpportunityType.EMPLOYMENT:
        return undefined;
      case OpportunityType.ASSESSMENT:
        return `Risk Assessment needed ${this.dueDaysFormatted}`;
      case OpportunityType.CONTACT:
        return `Face to Face Contact recommended ${this.dueDaysFormatted}`;
      default:
        assertNever(this.opportunityType);
    }
  }

  get alertOptions(): {
    icon: {
      kind: string;
      color: string;
    };
    pill: {
      kind: PillKind;
    };
  } {
    let iconKind = "Alert";
    let iconColor;
    let pillKind: PillKind;

    switch (this.opportunityType) {
      case OpportunityType.OVERDUE_DOWNGRADE:
        iconKind = "StarCircled";
        iconColor = palette.signal.highlight;
        pillKind = "highlight";
        break;
      case OpportunityType.EMPLOYMENT:
        iconColor = palette.data.gold1;
        pillKind = "warn";
        break;
      case OpportunityType.ASSESSMENT:
      case OpportunityType.CONTACT:
        if (this.opportunityMetadata.status === "OVERDUE") {
          iconColor = palette.signal.error;
          pillKind = "error";
        } else {
          iconColor = palette.data.gold1;
          pillKind = "warn";
        }
        break;
      default:
        assertNever(this.opportunityType);
    }

    return {
      icon: { kind: iconKind, color: iconColor },
      pill: { kind: pillKind },
    };
  }
}
