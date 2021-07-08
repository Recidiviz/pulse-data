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
import API from "../API";
import { OpportunityType } from "../OpportunityStore/Opportunity";
import RootStore from "../RootStore";
import { AlertKindList, Client, PENDING_ID } from "./Client";
import { Note } from "./Note";
import { clientData, clientOpportunities } from "./__fixtures__";

jest.mock("../API");
const APIMock = API as jest.MockedClass<typeof API>;

const TEST_NOTE_TEXT = "test note";

let rootStore: RootStore;
let testClient: Client;

beforeEach(() => {
  rootStore = new RootStore();
  testClient = Client.build({
    api: rootStore.api,
    client: cloneDeep(clientData),
    clientsStore: rootStore.clientsStore,
    opportunityStore: rootStore.opportunityStore,
    policyStore: rootStore.policyStore,
  });
  APIMock.prototype.post.mockResolvedValue({});
});
afterEach(() => {
  APIMock.mockReset();
  MockDate.reset();
});

test("contact status", () => {
  // date in fixture is 2022-06-18
  MockDate.set("2022-08-18");
  expect(testClient.contactStatus).toBe("OVERDUE");

  MockDate.set("2022-06-18");
  expect(testClient.contactStatus).toBe("UPCOMING");

  // shouldn't have to be midnight
  MockDate.set(new Date(2022, 5, 18, 14, 22));
  expect(testClient.contactStatus).toBe("UPCOMING");

  MockDate.set("2022-06-11");
  expect(testClient.contactStatus).toBe("UPCOMING");

  // beyond the threshold to be considered "upcoming"
  MockDate.set("2022-06-10");
  expect(testClient.contactStatus).toBe(null);
});

test("risk assessment status", () => {
  // date in fixture is 2020-10-23
  MockDate.set("2020-10-25");
  expect(testClient.riskAssessmentStatus).toBe("OVERDUE");

  MockDate.set("2020-10-23");
  expect(testClient.riskAssessmentStatus).toBe("UPCOMING");

  // shouldn't have to be midnight
  MockDate.set(new Date(2020, 9, 23, 14, 22));
  expect(testClient.riskAssessmentStatus).toBe("UPCOMING");

  MockDate.set("2020-09-23");
  expect(testClient.riskAssessmentStatus).toBe("UPCOMING");

  // beyond the threshold to be considered "upcoming"
  MockDate.set("2020-09-22");
  expect(testClient.riskAssessmentStatus).toBe(null);
});

describe("alerts", () => {
  test("include all Opportunity types", () => {
    expect(AlertKindList).toEqual(
      expect.arrayContaining(Object.values(OpportunityType))
    );
  });

  test("from opportunities", () => {
    expect(
      testClient.alerts.find(
        (alert) => alert.kind === OpportunityType.OVERDUE_DOWNGRADE
      )
    ).toBeUndefined();

    expect(
      testClient.alerts.find(
        (alert) => alert.kind === OpportunityType.EMPLOYMENT
      )
    ).toBeUndefined();

    rootStore.opportunityStore.opportunities = [...clientOpportunities];

    expect(
      testClient.alerts.find(
        (alert) => alert.kind === OpportunityType.OVERDUE_DOWNGRADE
      )
    ).toBeDefined();

    expect(
      testClient.alerts.find(
        (alert) => alert.kind === OpportunityType.EMPLOYMENT
      )
    ).toBeDefined();
  });

  test("risk assessment", () => {
    // date in fixture is 2020-10-23
    MockDate.set("2020-10-25");
    expect(
      testClient.alerts.find((alert) => alert.kind === "ASSESSMENT_OVERDUE")
    ).toBeDefined();
    expect(
      testClient.alerts.find((alert) => alert.kind === "ASSESSMENT_UPCOMING")
    ).toBeUndefined();

    MockDate.set("2020-10-20");
    expect(
      testClient.alerts.find((alert) => alert.kind === "ASSESSMENT_OVERDUE")
    ).toBeUndefined();
    expect(
      testClient.alerts.find((alert) => alert.kind === "ASSESSMENT_UPCOMING")
    ).toBeDefined();
  });

  test("contact", () => {
    // date in fixture is 2022-06-18
    MockDate.set("2022-08-18");
    expect(
      testClient.alerts.find((alert) => alert.kind === "CONTACT_OVERDUE")
    ).toBeDefined();
    expect(
      testClient.alerts.find((alert) => alert.kind === "CONTACT_UPCOMING")
    ).toBeUndefined();

    MockDate.set("2022-06-15");
    expect(
      testClient.alerts.find((alert) => alert.kind === "CONTACT_OVERDUE")
    ).toBeUndefined();
    expect(
      testClient.alerts.find((alert) => alert.kind === "CONTACT_UPCOMING")
    ).toBeDefined();
  });
});

test("create a note", async () => {
  const mockNoteData = {
    createdDatetime: "Wed, 09 Jun 2021 01:01:26 GMT",
    noteId: "f51f196b-1992-4921-8620-8f45299122c7",
    resolved: false,
    text: TEST_NOTE_TEXT,
    updatedDatetime: "Wed, 09 Jun 2021 01:01:26 GMT",
  };

  APIMock.prototype.post.mockResolvedValueOnce(mockNoteData);

  const response = testClient.createNote({
    text: TEST_NOTE_TEXT,
  });

  const [newNote] = testClient.notes;
  expect(newNote.text).toEqual(TEST_NOTE_TEXT);
  expect(newNote.noteId).toEqual(PENDING_ID);

  await response;
  expect(APIMock.prototype.post).toHaveBeenCalledWith("/api/create_note", {
    personExternalId: clientData.personExternalId,
    text: TEST_NOTE_TEXT,
  });

  expect(newNote.createdDatetime).toEqual(mockNoteData.createdDatetime);
  expect(newNote.noteId).toEqual(mockNoteData.noteId);
  expect(newNote.resolved).toEqual(mockNoteData.resolved);
  expect(newNote.text).toEqual(mockNoteData.text);
  expect(newNote.updatedDatetime).toEqual(mockNoteData.updatedDatetime);
});

test("handle failed note creation", async () => {
  APIMock.prototype.post.mockRejectedValueOnce({
    code: "bad_request",
    description: "Something bad was requested.",
  });

  await testClient.createNote({
    text: TEST_NOTE_TEXT,
  });

  expect(testClient.notes).toEqual([]);
});

describe("update notes", () => {
  let testNote: Note;

  beforeEach(() => {
    testNote = new Note(
      {
        createdDatetime: "Wed, 09 Jun 2021 01:01:26 GMT",
        noteId: "f51f196b-1992-4921-8620-8f45299122c7",
        resolved: false,
        text: TEST_NOTE_TEXT,
        updatedDatetime: "Wed, 09 Jun 2021 01:01:26 GMT",
      },
      rootStore.api
    );
  });

  test("resolve", async () => {
    const response = testNote.setResolution(true);
    expect(testNote.resolved).toBe(true);

    await response;
    expect(APIMock.prototype.post).toHaveBeenCalledWith("/api/resolve_note", {
      noteId: testNote.noteId,
      isResolved: true,
    });
    expect(testNote.resolved).toBe(true);
  });

  test("resolve failure", async () => {
    APIMock.prototype.post.mockResolvedValueOnce({
      code: "not_found",
      description: "Note not found",
    });

    const request = testNote.setResolution(true);
    // be optimistic locally
    expect(testNote.resolved).toBe(true);
    // revert on failure
    await request;
    expect(testNote.resolved).toBe(false);
  });

  test("un-resolve", async () => {
    const response = testNote.setResolution(false);
    expect(testNote.resolved).toBe(false);

    await response;
    expect(APIMock.prototype.post).toHaveBeenCalledWith("/api/resolve_note", {
      noteId: testNote.noteId,
      isResolved: false,
    });
    expect(testNote.resolved).toBe(false);
  });

  test("un-resolve failure", async () => {
    APIMock.prototype.post.mockResolvedValueOnce({
      code: "not_found",
      description: "Note not found",
    });

    const request = testNote.setResolution(false);
    // be optimistic locally
    expect(testNote.resolved).toBe(false);
    // revert on failure
    await request;
    expect(testNote.resolved).toBe(true);
  });
});
