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

export interface NeedsMet {
  assessment: boolean;
  employment: boolean;
  faceToFaceContact: boolean;
  homeVisitContact: boolean;
}

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
  needsMet: NeedsMet;
  nextAssessmentDate: APIDate;
  nextFaceToFaceDate: APIDate;
  nextHomeVisitDate: APIDate;
}

export interface DecoratedClient extends Client {
  name: string;
  formalName: string;
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
      return client.caseUpdates[action]?.status === status;
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
  if (client.nextHomeVisitDate === null) {
    return client.nextFaceToFaceDate;
  }
  if (client.nextFaceToFaceDate === null) {
    return client.nextHomeVisitDate;
  }
  return client.nextFaceToFaceDate < client.nextHomeVisitDate
    ? client.nextFaceToFaceDate
    : client.nextHomeVisitDate;
};
