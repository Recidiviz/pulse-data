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
export enum CaseUpdateActionType {
  COMPLETED_ASSESSMENT = "COMPLETED_ASSESSMENT",
  DISCHARGE_INITIATED = "DISCHARGE_INITIATED",
  DOWNGRADE_INITIATED = "DOWNGRADE_INITIATED",
  FOUND_EMPLOYMENT = "FOUND_EMPLOYMENT",
  SCHEDULED_FACE_TO_FACE = "SCHEDULED_FACE_TO_FACE",

  INFORMATION_DOESNT_MATCH_OMS = "INFORMATION_DOESNT_MATCH_OMS",
  NOT_ON_CASELOAD = "NOT_ON_CASELOAD",
  FILED_REVOCATION_OR_VIOLATION = "FILED_REVOCATION_OR_VIOLATION",
  OTHER_DISMISSAL = "OTHER_DISMISSAL",
}

export enum CaseUpdateStatus {
  IN_PROGRESS = "IN_PROGRESS",
}

export interface CaseUpdate {
  actionTs: string;
  actionType: CaseUpdateActionType;
  comment: string;
  status: CaseUpdateStatus;
  updateId: string;
}
