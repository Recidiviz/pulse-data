// Recidiviz - a data platform for criminal justice reform
// Copyright (C) 2023 Recidiviz, Inc.
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

import express, { Express } from "express";
import supertest from "supertest";
import tk from "timekeeper";
import {
  afterAll,
  beforeAll,
  beforeEach,
  expect,
  Mock,
  test,
  vi,
} from "vitest";

import {
  getTamperedTestToken,
  getTestToken,
  mockImageData,
} from "../testUtils";
import { routes } from "./index";
import { readFile } from "./readFile";

vi.mock("./readFile");

let testApp: Express;

beforeAll(() => {
  tk.freeze("2023-05-03");
});

afterAll(() => {
  tk.reset();
});

beforeEach(() => {
  testApp = express();
  testApp.use(routes);
});

test("valid token", async () => {
  const url = "test/path/to/file";
  const token = getTestToken(url);

  (readFile as Mock).mockResolvedValue(mockImageData);

  const response = await supertest(testApp).get(`/${token}`).expect(200);

  expect(readFile).toHaveBeenCalledWith(url);

  expect(response.type).toBe("image/png");
  expect(response.body).toEqual(mockImageData);
});

test("invalid token", async () => {
  const token = getTamperedTestToken();
  await supertest(testApp).get(`/${token}`).expect(404);
});

test("missing file", async () => {
  const token = getTestToken();
  (readFile as Mock).mockRejectedValue(new Error("file not found"));
  await supertest(testApp).get(`/${token}`).expect(404);
});

test("unknown file type", async () => {
  const token = getTestToken();
  const mockNonbinaryData = Buffer.from("some random text file");
  (readFile as Mock).mockResolvedValue(mockNonbinaryData);

  await supertest(testApp).get(`/${token}`).expect(404);
});
