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

import { cloneDeep } from "lodash";
import MockDate from "mockdate";
import { runInAction } from "mobx";
import API from "../API";
import RootStore from "../RootStore";
import { clientData, clientOpportunityData, statePolicy } from "./__fixtures__";
import type ClientsStore from "./ClientsStore";
import { ClientData } from "./Client";
import { KNOWN_EXPERIMENTS } from "../UserStore";
import { Opportunity } from "../OpportunityStore";

jest.mock("../API");
const APIMock = API as jest.MockedClass<typeof API>;

let store: ClientsStore;
let rootStore: RootStore;
let apiData: ClientData[];

function expectClients(ids: number[]) {
  expect(store.lists.UP_NEXT.map((c) => +c.personExternalId)).toEqual(ids);
}

beforeEach(async () => {
  // initialize the store for testing
  rootStore = new RootStore();
  store = rootStore.clientsStore;
  // tests in this file require an experiment flag
  rootStore.userStore.setFeatureVariants({
    [KNOWN_EXPERIMENTS.NewClientList]: "foo",
  });

  // easy-to-remember date for time-sensitive alerts
  MockDate.set("2021-01-01");
  // simulate having loaded clients
  apiData = Array.from(Array(6), (_, index) => {
    return {
      ...cloneDeep(clientData),
      personExternalId: `${index}`,
      // by default, suppress all alerts
      nextAssessmentDate: "2022-01-01",
      nextFaceToFaceDate: "2022-01-01",
      needsMet: { ...clientData.needsMet, employment: true },
    };
  });
  APIMock.prototype.get.mockResolvedValueOnce(apiData);
});

afterEach(() => {
  MockDate.reset();
});

test("sort by relevance (default)", async () => {
  rootStore.opportunityStore.opportunitiesFetched = [
    // clients 0, 2, 3 are unemployed
    {
      ...clientOpportunityData[1],
      personExternalId: "0",
    },
    {
      ...clientOpportunityData[1],
      personExternalId: "2",
    },
    {
      ...clientOpportunityData[1],
      personExternalId: "3",
    },
    // clients 2, 4 are eligible for downgrade
    {
      ...clientOpportunityData[0],
      personExternalId: "2",
    },
    {
      ...clientOpportunityData[0],
      personExternalId: "4",
    },
  ].map((data) => new Opportunity(data));

  // clients 1, 2 have contact alerts
  apiData[1].nextFaceToFaceDate = "2021-01-03";
  apiData[2].nextFaceToFaceDate = "2020-12-01";

  // clients 0, 3, 4 have upcoming assessments
  apiData[0].nextAssessmentDate = "2021-01-03";
  apiData[3].nextAssessmentDate = "2021-01-03";
  apiData[4].nextAssessmentDate = "2021-01-03";

  // 0 has earlier contact date than 3 (but neither has an alert for it)
  apiData[0].nextFaceToFaceDate = "2021-06-01";
  apiData[3].nextFaceToFaceDate = "2021-07-01";

  await store.fetchClientsList();
  expectClients([2, 4, 0, 3, 1, 5]);
});

test("sort by next contact date", async () => {
  apiData[5].nextFaceToFaceDate = "2020-06-01";
  apiData[0].nextFaceToFaceDate = "2020-11-01";
  apiData[3].nextFaceToFaceDate = "2021-01-01";
  apiData[2].nextFaceToFaceDate = "2021-01-01";
  apiData[4].nextFaceToFaceDate = "2021-02-01";
  apiData[1].nextFaceToFaceDate = "2022-01-01";

  await store.fetchClientsList();
  store.sortOrder = "CONTACT_DATE";
  expectClients([5, 0, 2, 3, 4, 1]);
});

test("sort by risk assessment date", async () => {
  apiData[3].nextAssessmentDate = "2020-06-01";
  apiData[5].nextAssessmentDate = "2020-11-01";
  apiData[1].nextAssessmentDate = "2021-01-01";
  apiData[0].nextAssessmentDate = "2021-01-01";
  apiData[4].nextAssessmentDate = "2021-02-01";
  apiData[2].nextAssessmentDate = "2022-01-01";

  await store.fetchClientsList();
  store.sortOrder = "ASSESSMENT_DATE";
  expectClients([3, 5, 0, 1, 4, 2]);
});

test("sort by time on supervision", async () => {
  apiData[1].supervisionStartDate = "2020-12-15";
  apiData[5].supervisionStartDate = "2020-02-01";
  apiData[4].supervisionStartDate = "2020-01-01";
  apiData[2].supervisionStartDate = "2020-01-01";
  apiData[3].supervisionStartDate = "2019-11-01";
  apiData[0].supervisionStartDate = "2019-06-01";

  await store.fetchClientsList();
  store.sortOrder = "START_DATE";
  expectClients([1, 5, 4, 2, 3, 0]);
});

test("supervision level filter", async () => {
  apiData[0].supervisionLevel = "HIGH";
  apiData[1].supervisionLevel = "HIGH";
  apiData[2].supervisionLevel = "MEDIUM";
  apiData[3].supervisionLevel = "MEDIUM";
  apiData[4].supervisionLevel = "MINIMUM";
  apiData[5].supervisionLevel = "MINIMUM";

  await store.fetchClientsList();

  store.supervisionLevelFilter = "MEDIUM";
  expectClients([2, 3]);

  store.supervisionLevelFilter = "HIGH";
  expectClients([0, 1]);

  store.supervisionLevelFilter = "MINIMUM";
  expectClients([4, 5]);
});

test("risk level filter", async () => {
  runInAction(() => {
    rootStore.policyStore.policies = statePolicy;
  });

  apiData[0].assessmentScore = 12;
  apiData[0].gender = "MALE";
  apiData[1].assessmentScore = 23;
  apiData[1].gender = "FEMALE";
  apiData[2].assessmentScore = 7;
  apiData[2].gender = "MALE";
  apiData[3].assessmentScore = 18;
  apiData[3].gender = "FEMALE";
  apiData[4].assessmentScore = 3;
  apiData[4].gender = "MALE";
  apiData[5].assessmentScore = 8;
  apiData[5].gender = "FEMALE";

  await store.fetchClientsList();

  store.riskLevelFilter = "MEDIUM";
  expectClients([2, 3]);

  store.riskLevelFilter = "HIGH";
  expectClients([0, 1]);

  store.riskLevelFilter = "MINIMUM";
  expectClients([4, 5]);
});

test("supervision type filter", async () => {
  apiData[0].supervisionType = "PAROLE";
  apiData[1].supervisionType = "PAROLE";
  apiData[2].supervisionType = "PAROLE";
  apiData[3].supervisionType = "PROBATION";
  apiData[4].supervisionType = "PROBATION";
  apiData[5].supervisionType = "PROBATION";

  await store.fetchClientsList();

  store.supervisionTypeFilter = "PROBATION";
  expectClients([3, 4, 5]);
});
