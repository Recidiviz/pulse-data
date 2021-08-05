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

import {
  OpportunityData,
  OpportunityType,
} from "../OpportunityStore/Opportunity";
import { Policy } from "../PolicyStore/Policy";
import { ClientData } from "./Client";

export const clientData: ClientData = {
  assessmentScore: 1,
  birthdate: "1990-04-01",
  caseType: "GENERAL",
  caseUpdates: {},
  currentAddress: "",
  emailAddress: "demo@recidiviz.org",
  employer: undefined,
  employmentStartDate: null,
  fullName: {
    given_names: "TONYE",
    middle_name: "BARBY",
    surname: "THOMPSON",
  },
  gender: "MALE",
  mostRecentAssessmentDate: "2019-10-24",
  mostRecentFaceToFaceDate: "2021-06-18",
  mostRecentHomeVisitDate: "2021-06-18",
  mostRecentViolationDate: null,
  needsMet: {
    assessment: false,
    employment: false,
    faceToFaceContact: true,
    homeVisitContact: true,
  },
  nextAssessmentDate: "2020-10-23",
  nextFaceToFaceDate: "2022-06-18",
  nextHomeVisitDate: "2022-06-18",
  notes: [],
  personExternalId: "100",
  projectedEndDate: null,
  phoneNumber: null,
  receivingSSIOrDisabilityIncome: false,
  supervisionLevel: "MEDIUM",
  supervisionStartDate: "2020-01-26",
  supervisionType: "PAROLE",
};

export const clientOpportunityData: OpportunityData[] = [
  {
    opportunityMetadata: {
      assessmentScore: 1,
      latestAssessmentDate: "2019-10-26",
    },
    opportunityType: OpportunityType.OVERDUE_DOWNGRADE,
    personExternalId: "100",
    stateCode: "US_TEST",
    supervisingOfficerExternalId: "TESTID",
  },
  {
    opportunityMetadata: {},
    opportunityType: OpportunityType.EMPLOYMENT,
    personExternalId: "100",
    stateCode: "US_TEST",
    supervisingOfficerExternalId: "TESTID",
  },
];

export const statePolicy: Policy = {
  assessmentScoreCutoffs: {
    FEMALE: {
      HIGH: [21, null],
      MEDIUM: [11, 20],
      MINIMUM: [0, 10],
    },
    MALE: {
      HIGH: [11, null],
      MEDIUM: [6, 10],
      MINIMUM: [0, 5],
    },
    TRANS_FEMALE: {
      HIGH: [21, null],
      MEDIUM: [11, 20],
      MINIMUM: [0, 10],
    },
    TRANS_MALE: {
      HIGH: [11, null],
      MEDIUM: [6, 10],
      MINIMUM: [0, 5],
    },
  },
  docShortName: "TestDOC",
  omsName: "TEST",
  policyReferencesForOpportunities: {
    OVERDUE_DOWNGRADE: "http://example.com",
    EMPLOYMENT: "http://example.com",
    ASSESSMENT: "http://example.com",
    CONTACT: "http://example.com",
  },
  supervisionContactFrequencies: {
    GENERAL: {
      HIGH: [2, 30],
      MEDIUM: [2, 90],
      MINIMUM: [1, 180],
    },
    SEX_OFFENDER: {
      HIGH: [2, 30],
      MEDIUM: [1, 30],
      MINIMUM: [1, 90],
    },
  },
  supervisionHomeVisitFrequencies: {
    HIGH: [2, 365],
    MEDIUM: [1, 365],
    MINIMUM: [1, 365],
  },
  supervisionLevelNames: {
    HIGH: "High",
    MEDIUM: "Moderate",
    MINIMUM: "Low",
  },
  supervisionPolicyReference: "http://example.com",
};
