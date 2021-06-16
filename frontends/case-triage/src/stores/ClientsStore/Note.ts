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

import { makeAutoObservable, runInAction } from "mobx";
import API, { isErrorResponse } from "../API";

export type NoteData = {
  createdDatetime: string;
  noteId: string;
  resolved: boolean;
  text: string;
  updatedDatetime: string;
};

export class Note {
  createdDatetime: string;

  noteId: string;

  resolved: boolean;

  text: string;

  updatedDatetime: string;

  constructor(data: NoteData, private api: API) {
    makeAutoObservable<Note, "api">(this, { api: false });

    this.createdDatetime = data.createdDatetime;
    this.noteId = data.noteId;
    this.resolved = data.resolved;
    this.text = data.text;
    this.updatedDatetime = data.updatedDatetime;
  }

  /**
   * Marks the note with specified resolution and updates
   * the backend via API request. Will make local updates optimistically
   * and revert on API failure.
   */
  async setResolution(isResolved: boolean): Promise<void> {
    this.resolved = isResolved;

    const response = await this.api.post<{ status: string }>(
      "/api/resolve_note",
      {
        noteId: this.noteId,
        isResolved,
      }
    );

    if (isErrorResponse(response)) {
      runInAction(() => {
        this.resolved = !isResolved;
      });
    }
  }
}
