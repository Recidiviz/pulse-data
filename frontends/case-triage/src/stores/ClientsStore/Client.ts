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
import { CaseUpdateActionType } from "../CaseUpdatesStore";
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

export interface Client {
  assessmentScore: number | null;
  caseType: CaseType;
  currentAddress: string;
  fullName: ClientFullName;
  employer?: string;
  gender: Gender;
  supervisionStartDate: APIDate;
  projectedEndDate: APIDate;
  inProgressActions?: CaseUpdateActionType[];
  inProgressSubmissionDate?: APIDate;
  supervisionType: string;
  supervisionLevel: SupervisionLevel;
  personExternalId: string;
  mostRecentFaceToFaceDate: APIDate;
  mostRecentAssessmentDate: APIDate;
  needsMet: {
    assessment: boolean;
    employment: boolean;
    faceToFaceContact: boolean;
  };
  nextAssessmentDate: APIDate;
  nextFaceToFaceDate: APIDate;
}

export interface DecoratedClient extends Client {
  name: string;
  formalName: string;
  supervisionStartDate: moment.Moment | null;
  projectedEndDate: moment.Moment | null;
  mostRecentFaceToFaceDate: moment.Moment | null;
  mostRecentAssessmentDate: moment.Moment | null;
  nextAssessmentDate: moment.Moment | null;
  nextFaceToFaceDate: moment.Moment | null;
  inProgressSubmissionDate: moment.Moment | null;

  supervisionLevelText: string;
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
    mostRecentAssessmentDate: parseDate(client.mostRecentAssessmentDate),
    nextFaceToFaceDate: parseDate(client.nextFaceToFaceDate),
    nextAssessmentDate: parseDate(client.nextAssessmentDate),
    inProgressSubmissionDate: parseDate(client.inProgressSubmissionDate),

    supervisionLevelText: policyStore.getSupervisionLevelNameForClient(client),
  };
};

export { decorateClient };
