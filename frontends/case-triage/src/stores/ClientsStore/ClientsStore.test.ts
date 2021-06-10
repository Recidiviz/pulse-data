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

import API from "../API";
import RootStore from "../RootStore";
import { PENDING_ID } from "./Client";
import type ClientsStore from "./ClientsStore";
import { clientData } from "./__fixtures__";

jest.mock("../API", () => {
  const actual = jest.requireActual("../API");
  return {
    ...jest.createMockFromModule<{ default: typeof API }>("../API"),
    isErrorResponse: actual.isErrorResponse,
  };
});

const APIMock = API as jest.MockedClass<typeof API>;

function getStore() {
  return new RootStore().clientsStore;
}

function getMockClientData() {
  return JSON.parse(JSON.stringify(clientData));
}

const TEST_NOTE_TEXT = "test note";

let store: ClientsStore;

beforeEach(() => {
  store = getStore();
  APIMock.prototype.post.mockResolvedValue({});
});
afterEach(() => {
  APIMock.mockReset();
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

  const testClient = getMockClientData();

  const response = store.createNoteForClient({
    client: testClient,
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
  // note should now reflect all the data we got from the API
  expect(newNote).toEqual(mockNoteData);
  expect(testClient.notes).toEqual([newNote]);
});

test("handle failed note creation", async () => {
  APIMock.prototype.post.mockResolvedValueOnce({
    code: "bad_request",
    description: "Something bad was requested.",
  });

  const testClient = getMockClientData();
  await store.createNoteForClient({
    client: testClient,
    text: TEST_NOTE_TEXT,
  });

  expect(testClient.notes).toEqual([]);
});

test("resolve note", async () => {
  const mockNote = {
    createdDatetime: "Wed, 09 Jun 2021 01:01:26 GMT",
    noteId: "f51f196b-1992-4921-8620-8f45299122c7",
    resolved: false,
    text: TEST_NOTE_TEXT,
    updatedDatetime: "Wed, 09 Jun 2021 01:01:26 GMT",
  };

  const response = store.setNoteResolution({
    note: mockNote,
    isResolved: true,
  });
  expect(mockNote.resolved).toBe(true);

  await response;
  expect(APIMock.prototype.post).toHaveBeenCalledWith("/api/resolve_note", {
    noteId: mockNote.noteId,
    isResolved: true,
  });
  expect(mockNote.resolved).toBe(true);
});

test("resolve note failure", async () => {
  const mockNote = {
    createdDatetime: "Wed, 09 Jun 2021 01:01:26 GMT",
    noteId: "f51f196b-1992-4921-8620-8f45299122c7",
    resolved: false,
    text: TEST_NOTE_TEXT,
    updatedDatetime: "Wed, 09 Jun 2021 01:01:26 GMT",
  };

  APIMock.prototype.post.mockResolvedValueOnce({
    code: "not_found",
    description: "Note not found",
  });
  const request = store.setNoteResolution({ note: mockNote, isResolved: true });
  // be optimistic locally
  expect(mockNote.resolved).toBe(true);
  // revert on failure
  await request;
  expect(mockNote.resolved).toBe(false);
});

test("un-resolve note", async () => {
  const mockNote = {
    createdDatetime: "Wed, 09 Jun 2021 01:01:26 GMT",
    noteId: "f51f196b-1992-4921-8620-8f45299122c7",
    resolved: true,
    text: TEST_NOTE_TEXT,
    updatedDatetime: "Wed, 09 Jun 2021 01:01:26 GMT",
  };

  const response = store.setNoteResolution({
    note: mockNote,
    isResolved: false,
  });
  expect(mockNote.resolved).toBe(false);

  await response;
  expect(APIMock.prototype.post).toHaveBeenCalledWith("/api/resolve_note", {
    noteId: mockNote.noteId,
    isResolved: false,
  });
  expect(mockNote.resolved).toBe(false);
});

test("un-resolve note failure", async () => {
  const mockNote = {
    createdDatetime: "Wed, 09 Jun 2021 01:01:26 GMT",
    noteId: "f51f196b-1992-4921-8620-8f45299122c7",
    resolved: true,
    text: TEST_NOTE_TEXT,
    updatedDatetime: "Wed, 09 Jun 2021 01:01:26 GMT",
  };

  APIMock.prototype.post.mockResolvedValueOnce({
    code: "not_found",
    description: "Note not found",
  });
  const request = store.setNoteResolution({
    note: mockNote,
    isResolved: false,
  });
  // be optimistic locally
  expect(mockNote.resolved).toBe(false);
  // revert on failure
  await request;
  expect(mockNote.resolved).toBe(true);
});
