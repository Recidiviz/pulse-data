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
/* eslint-disable camelcase */
interface ClientFullName {
  given_names: string;
  middle_name: string;
  surname: string;
}
/* eslint-enable camelcase */

export interface Client {
  currentAddress: string;
  fullName: ClientFullName;
  employer?: string;
  supervisionType: string;
  supervisionLevel: string;
  personExternalId: string;
  mostRecentFaceToFaceDate: string | moment.Moment;
  mostRecentAssessmentDate: string | moment.Moment;
  needsMet: {
    assessment: boolean;
    employment: boolean;
    faceToFaceContact: boolean;
  };
}

export interface DecoratedClient extends Client {
  name: string;
  formalName: string;
  mostRecentFaceToFaceDate: moment.Moment;
  mostRecentAssessmentDate: moment.Moment;
}

const decorateClient = (client: Client): DecoratedClient => {
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
    mostRecentFaceToFaceDate: moment(client.mostRecentFaceToFaceDate),
    mostRecentAssessmentDate: moment(client.mostRecentAssessmentDate),
  };
};

export { decorateClient };
