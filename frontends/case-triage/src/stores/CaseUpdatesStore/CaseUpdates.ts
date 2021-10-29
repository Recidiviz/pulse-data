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
import { OpportunityType } from "../OpportunityStore/Opportunity";

export enum CaseUpdateActionType {
  // Risk Assessment
  INCORRECT_ASSESSMENT_DATA = "INCORRECT_ASSESSMENT_DATA",
  // Employment
  INCORRECT_EMPLOYMENT_DATA = "INCORRECT_EMPLOYMENT_DATA",
  // Face to face contact
  INCORRECT_CONTACT_DATA = "INCORRECT_CONTACT_DATA",
  // Home visit contact
  INCORRECT_HOME_VISIT_DATA = "INCORRECT_HOME_VISIT_DATA",
  // Supervision Level
  INCORRECT_SUPERVISION_LEVEL_DATA = "INCORRECT_SUPERVISION_LEVEL_DATA",

  NOT_ON_CASELOAD = "NOT_ON_CASELOAD",
  CURRENTLY_IN_CUSTODY = "CURRENTLY_IN_CUSTODY",

  INCORRECT_NEW_TO_CASELOAD_DATA = "INCORRECT_NEW_TO_CASELOAD_DATA",
}

export const isErrorReport = (action: CaseUpdateActionType): boolean => {
  switch (action) {
    case CaseUpdateActionType.INCORRECT_SUPERVISION_LEVEL_DATA:
    case CaseUpdateActionType.INCORRECT_ASSESSMENT_DATA:
    case CaseUpdateActionType.INCORRECT_EMPLOYMENT_DATA:
    case CaseUpdateActionType.INCORRECT_CONTACT_DATA:
    case CaseUpdateActionType.INCORRECT_HOME_VISIT_DATA:
    case CaseUpdateActionType.INCORRECT_NEW_TO_CASELOAD_DATA:
    case CaseUpdateActionType.NOT_ON_CASELOAD:
    case CaseUpdateActionType.CURRENTLY_IN_CUSTODY:
      return true;
    default:
      return false;
  }
};

export enum CaseUpdateStatus {
  IN_PROGRESS = "IN_PROGRESS",
}

export interface CaseUpdate {
  actionTs: string;
  actionType: CaseUpdateActionType;
  comment: string | null;
  status: CaseUpdateStatus;
  updateId?: string;
}

export const CASE_UPDATE_OPPORTUNITY_ASSOCIATION: Record<
  OpportunityType,
  CaseUpdateActionType
> = {
  [OpportunityType.OVERDUE_DOWNGRADE]:
    CaseUpdateActionType.INCORRECT_SUPERVISION_LEVEL_DATA,
  [OpportunityType.EMPLOYMENT]: CaseUpdateActionType.INCORRECT_EMPLOYMENT_DATA,
  [OpportunityType.ASSESSMENT]: CaseUpdateActionType.INCORRECT_ASSESSMENT_DATA,
  [OpportunityType.CONTACT]: CaseUpdateActionType.INCORRECT_CONTACT_DATA,
  [OpportunityType.HOME_VISIT]: CaseUpdateActionType.INCORRECT_HOME_VISIT_DATA,
  [OpportunityType.NEW_TO_CASELOAD]:
    CaseUpdateActionType.INCORRECT_NEW_TO_CASELOAD_DATA,
};

export const ACTION_TITLES: Record<CaseUpdateActionType, string> = {
  [CaseUpdateActionType.INCORRECT_SUPERVISION_LEVEL_DATA]:
    "Incorrect supervision level data",
  [CaseUpdateActionType.INCORRECT_ASSESSMENT_DATA]: "Assessment not needed",
  [CaseUpdateActionType.INCORRECT_EMPLOYMENT_DATA]: "Employment not needed",
  [CaseUpdateActionType.INCORRECT_CONTACT_DATA]: "Contact not needed",
  [CaseUpdateActionType.INCORRECT_HOME_VISIT_DATA]: "Home visit not needed",
  [CaseUpdateActionType.INCORRECT_NEW_TO_CASELOAD_DATA]:
    "Incorrect time on caseload",
  [CaseUpdateActionType.NOT_ON_CASELOAD]: "Not on Caseload",
  [CaseUpdateActionType.CURRENTLY_IN_CUSTODY]: "In Custody",
};
