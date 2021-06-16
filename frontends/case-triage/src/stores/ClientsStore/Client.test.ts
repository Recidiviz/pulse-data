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
import { Client, PENDING_ID } from "./Client";
import { Note } from "./Note";
import { clientData } from "./__fixtures__";

jest.mock("../API", () => {
  const actual = jest.requireActual("../API");
  return {
    ...jest.createMockFromModule<{ default: typeof API }>("../API"),
    isErrorResponse: actual.isErrorResponse,
  };
});

const APIMock = API as jest.MockedClass<typeof API>;

function getMockClientData() {
  return JSON.parse(JSON.stringify(clientData));
}

const TEST_NOTE_TEXT = "test note";

let rootStore: RootStore;
let testClient: Client;

beforeEach(() => {
  rootStore = new RootStore();
  testClient = Client.build({
    api: rootStore.api,
    client: getMockClientData(),
    clientsStore: rootStore.clientsStore,
  });
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
  APIMock.prototype.post.mockResolvedValueOnce({
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
