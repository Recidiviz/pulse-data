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
import moment from "moment";
import { titleCase } from "../../utils";
import {
  CaseUpdate,
  CaseUpdateActionType,
  CaseUpdateStatus,
} from "../CaseUpdatesStore";
import PolicyStore from "../PolicyStore";

/* eslint-disable camelcase */
interface ClientFullName {
  given_names: string;
  middle_name: string;
  surname: string;
}
/* eslint-enable camelcase */

export type Gender = "FEMALE" | "MALE" | "TRANS_FEMALE" | "TRANS_MALE";

export type CaseType = "GENERAL" | "SEX_OFFENDER";

export const SupervisionLevels = <const>["HIGH", "MEDIUM", "MINIMUM"];
export type SupervisionLevel = typeof SupervisionLevels[number];

type APIDate = string | moment.Moment | null;

const POSSESSIVE_PRONOUNS: Record<Gender, string> = {
  FEMALE: "her",
  MALE: "his",
  TRANS_FEMALE: "her",
  TRANS_MALE: "his",
};

export interface NeedsMet {
  assessment: boolean;
  employment: boolean;
  faceToFaceContact: boolean;
  homeVisitContact: boolean;
}

export enum PreferredContactMethod {
  Email = "EMAIL",
  Call = "CALL",
  Text = "TEXT",
}

export const PENDING_ID = "PENDING";

export type Note = {
  createdDatetime: APIDate;
  noteId: string;
  resolved: boolean;
  text: string;
  updatedDatetime: APIDate;
};

export interface Client {
  assessmentScore: number | null;
  caseType: CaseType;
  caseUpdates: Record<CaseUpdateActionType, CaseUpdate>;
  currentAddress: string;
  fullName: ClientFullName;
  employer?: string;
  gender: Gender;
  supervisionStartDate: APIDate;
  projectedEndDate: APIDate;
  supervisionType: string;
  supervisionLevel: SupervisionLevel;
  personExternalId: string;
  mostRecentFaceToFaceDate: APIDate;
  mostRecentHomeVisitDate: APIDate;
  mostRecentAssessmentDate: APIDate;
  emailAddress?: string;
  needsMet: NeedsMet;
  nextAssessmentDate: APIDate;
  nextFaceToFaceDate: APIDate;
  nextHomeVisitDate: APIDate;
  preferredName?: string;
  preferredContactMethod?: PreferredContactMethod;
  notes?: Note[];
}

export interface DecoratedClient extends Client {
  name: string;
  formalName: string;
  possessivePronoun: string;
  supervisionStartDate: moment.Moment | null;
  projectedEndDate: moment.Moment | null;
  mostRecentFaceToFaceDate: moment.Moment | null;
  mostRecentHomeVisitDate: moment.Moment | null;
  mostRecentAssessmentDate: moment.Moment | null;
  nextAssessmentDate: moment.Moment | null;
  nextFaceToFaceDate: moment.Moment | null;
  nextHomeVisitDate: moment.Moment | null;

  previousInProgressActions?: CaseUpdateActionType[];

  supervisionLevelText: string;
  hasCaseUpdateInStatus(
    action: CaseUpdateActionType,
    status: CaseUpdateStatus
  ): boolean;

  hasInProgressUpdate(action: CaseUpdateActionType): boolean;
}

const parseDate = (date?: APIDate) => {
  if (!date) {
    return null;
  }

  return moment(date);
};

const decorateClient = (
  client: Client,
  policyStore: PolicyStore
): DecoratedClient => {
  const { given_names: given, surname } = client.fullName;

  const name = `${titleCase(given)} ${titleCase(surname)}`;

  let formalName = titleCase(surname);
  if (given) {
    formalName += `, ${titleCase(given)}`;
  }

  return {
    ...client,
    name,
    formalName,
    possessivePronoun: POSSESSIVE_PRONOUNS[client.gender] || "their",
    supervisionStartDate: parseDate(client.supervisionStartDate),
    projectedEndDate: parseDate(client.projectedEndDate),
    mostRecentFaceToFaceDate: parseDate(client.mostRecentFaceToFaceDate),
    mostRecentHomeVisitDate: parseDate(client.mostRecentHomeVisitDate),
    mostRecentAssessmentDate: parseDate(client.mostRecentAssessmentDate),
    nextFaceToFaceDate: parseDate(client.nextFaceToFaceDate),
    nextHomeVisitDate: parseDate(client.nextHomeVisitDate),
    nextAssessmentDate: parseDate(client.nextAssessmentDate),
    supervisionLevelText: policyStore.getSupervisionLevelNameForClient(client),

    hasCaseUpdateInStatus(
      action: CaseUpdateActionType,
      status: CaseUpdateStatus
    ) {
      return this.caseUpdates[action]?.status === status;
    },

    hasInProgressUpdate(action: CaseUpdateActionType) {
      return this.hasCaseUpdateInStatus(action, CaseUpdateStatus.IN_PROGRESS);
    },
  };
};

export { decorateClient };

export const getNextContactDate = (
  client: DecoratedClient
): moment.Moment | null => {
  // TODO(#7320): Our current `nextHomeVisitDate` determines when the next home visit
  // should be assuming that home visits must be F2F visits and not collateral visits.
  // As a result, until we do more investigation into what the appropriate application
  // of state policy is, we're not showing home visits as the next contact dates.
  return client.nextFaceToFaceDate;
};
