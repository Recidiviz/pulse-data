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
  COMPLETED_ASSESSMENT = "COMPLETED_ASSESSMENT",
  INCORRECT_ASSESSMENT_DATA = "INCORRECT_ASSESSMENT_DATA",
  // Employment
  FOUND_EMPLOYMENT = "FOUND_EMPLOYMENT",
  INCORRECT_EMPLOYMENT_DATA = "INCORRECT_EMPLOYMENT_DATA",
  // Face to face contact
  SCHEDULED_FACE_TO_FACE = "SCHEDULED_FACE_TO_FACE",
  INCORRECT_CONTACT_DATA = "INCORRECT_CONTACT_DATA",

  DISCHARGE_INITIATED = "DISCHARGE_INITIATED",

  // Supervision Level
  DOWNGRADE_INITIATED = "DOWNGRADE_INITIATED",
  INCORRECT_SUPERVISION_LEVEL_DATA = "INCORRECT_SUPERVISION_LEVEL_DATA",

  NOT_ON_CASELOAD = "NOT_ON_CASELOAD",
  CURRENTLY_IN_CUSTODY = "CURRENTLY_IN_CUSTODY",
}

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
  [CaseUpdateActionType, CaseUpdateActionType]
> = {
  [OpportunityType.OVERDUE_DOWNGRADE]: [
    CaseUpdateActionType.DOWNGRADE_INITIATED,
    CaseUpdateActionType.INCORRECT_SUPERVISION_LEVEL_DATA,
  ],
  [OpportunityType.EMPLOYMENT]: [
    CaseUpdateActionType.FOUND_EMPLOYMENT,
    CaseUpdateActionType.INCORRECT_EMPLOYMENT_DATA,
  ],
};
