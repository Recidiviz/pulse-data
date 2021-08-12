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
import { when } from "mobx";
import MockDate from "mockdate";
import moment from "moment";
import API from "../API";
import { CaseUpdateActionType, CaseUpdateStatus } from "../CaseUpdatesStore";
import { Opportunity } from "../OpportunityStore";
import { OpportunityDeferralType } from "../OpportunityStore/Opportunity";
import RootStore from "../RootStore";
import { Client, PENDING_ID } from "./Client";
import { Note } from "./Note";
import { clientData, clientOpportunityData } from "./__fixtures__";

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
    errorMessageStore: rootStore.errorMessageStore,
  });
  APIMock.prototype.post.mockResolvedValue({});
});
afterEach(() => {
  APIMock.mockReset();
  MockDate.reset();
});

test("phone number parsing", () => {
  testClient = Client.build({
    api: rootStore.api,
    client: cloneDeep({
      ...clientData,
      phoneNumber: null,
    }),
    clientsStore: rootStore.clientsStore,
    opportunityStore: rootStore.opportunityStore,
    policyStore: rootStore.policyStore,
    errorMessageStore: rootStore.errorMessageStore,
  });

  expect(testClient.phoneNumber).toBe(undefined);

  testClient = Client.build({
    api: rootStore.api,
    client: cloneDeep({
      ...clientData,
      phoneNumber: "5108675309",
    }),
    clientsStore: rootStore.clientsStore,
    opportunityStore: rootStore.opportunityStore,
    policyStore: rootStore.policyStore,
    errorMessageStore: rootStore.errorMessageStore,
  });

  expect(testClient.phoneNumber).toEqual("(510) 867-5309");
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

test("active opportunities", () => {
  rootStore.opportunityStore.opportunitiesFetched = clientOpportunityData.map(
    (data) => new Opportunity(data)
  );
  expect(testClient.activeOpportunities.length).toBe(2);
});

test("no active opportunities", () => {
  rootStore.opportunityStore.opportunitiesFetched = [
    {
      ...clientOpportunityData[0],
      deferredUntil: moment().format(),
      deferralType: OpportunityDeferralType.REMINDER,
    },
    clientOpportunityData[1],
  ].map((data) => new Opportunity(data));
  testClient.caseUpdates.INCORRECT_EMPLOYMENT_DATA = {
    actionTs: moment().format(),
    actionType: CaseUpdateActionType.INCORRECT_EMPLOYMENT_DATA,
    comment: "",
    status: CaseUpdateStatus.IN_PROGRESS,
  };
  expect(testClient.activeOpportunities.length).toBe(0);
});

test("employment milestone", () => {
  MockDate.set("2021-04-15");
  testClient.employmentStartDate = moment(new Date(2021, 0, 4));

  expect(testClient.milestones.employment).toBe("3 months");

  testClient.employmentStartDate.subtract(6, "months");
  expect(testClient.milestones.employment).toBe("9 months");
});

test("no employment milestone", () => {
  MockDate.set("2021-04-01");

  expect(testClient.milestones.employment).toBeUndefined();

  // less than 3 months
  testClient.employmentStartDate = moment(new Date(2021, 2, 12));
  expect(testClient.milestones.employment).toBeUndefined();
});

test("violation milestone", () => {
  MockDate.set("2021-04-15");

  // in the absence of a violation, the milestone is relative to supervision start
  expect(testClient.milestones.violationFree).toBe("a year");

  testClient.mostRecentViolationDate = moment(new Date(2021, 0, 4));

  expect(testClient.milestones.violationFree).toBe("3 months");

  testClient.mostRecentViolationDate.subtract(6, "months");
  expect(testClient.milestones.violationFree).toBe("9 months");
});

test("no violation milestone", () => {
  MockDate.set("2021-04-01");

  // less than 3 months
  testClient.mostRecentViolationDate = moment(new Date(2021, 2, 12));
  expect(testClient.milestones.violationFree).toBeUndefined();
});

describe("timeline", () => {
  test("initial state", () => {
    expect(testClient.needsTimelineHydration).toBe(true);
    expect(testClient.isTimelineLoaded).toBe(false);
    expect(testClient.timeline).toEqual([]);
  });

  test("hydration", async () => {
    // NOTE: we expect reverse chronological order from the API
    APIMock.prototype.get.mockResolvedValueOnce([
      {
        eventType: "CONTACT",
        eventDate: "2021-06-01",
        eventMetadata: { contactType: "FACE_TO_FACE", location: null },
      },
      {
        eventType: "CONTACT",
        eventDate: "2021-01-01",
        eventMetadata: { contactType: "FACE_TO_FACE", location: null },
      },
      {
        eventType: "CONTACT",
        eventDate: "2020-07-01",
        eventMetadata: { contactType: "FACE_TO_FACE", location: null },
      },
    ]);

    testClient.hydrateTimeline();
    await when(() => testClient.isTimelineLoaded);
    expect(APIMock.prototype.get).toHaveBeenCalledWith(
      `/api/events/${clientData.personExternalId}`
    );

    expect(testClient.needsTimelineHydration).toBe(false);
    // verify order was retained; don't need to deeply inspect objects, tested elsewhere
    expect(testClient.timeline.map((e) => e.eventDate)).toEqual([
      moment("2021-06-01"),
      moment("2021-01-01"),
      moment("2020-07-01"),
    ]);
  });
});
