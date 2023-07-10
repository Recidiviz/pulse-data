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
import { afterAll, beforeAll, beforeEach, expect, test, vi } from "vitest";

import {
  fittingSupervisorData,
  noPrevRateSupervisorData,
} from "../../../components/OutliersSupervisorChart/fixtures";
import { routes } from "../routes";
import { writeFile } from "../writeFile";

vi.mock("../writeFile");

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

test("valid input", async () => {
  const response = await supertest(testApp)
    .post("/outliers-supervisor-chart")
    .send({
      stateCode: "US_XX",
      width: 570,
      id: "test-officer-metric",
      data: fittingSupervisorData,
    })
    .expect(200);

  expect(writeFile).toHaveBeenCalledWith(
    expect.stringMatching(
      /^outliers-supervisor-chart\/US_XX\/2023-05-03\/test-officer-metric\.png$/
    ),
    expect.any(Buffer)
  );

  expect(response.body).toMatchInlineSnapshot(`
    {
      "url": "/asset/eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJzdWIiOiJvdXRsaWVycy1zdXBlcnZpc29yLWNoYXJ0L1VTX1hYLzIwMjMtMDUtMDMvdGVzdC1vZmZpY2VyLW1ldHJpYy5wbmciLCJpYXQiOjE2ODMwNzIwMDB9.TXxCL8WAHpAqw9Ksn2h-exb4l_hlanJsSICfrxNqQJI",
    }
  `);
});

test("prevRate can be missing", async () => {
  await supertest(testApp)
    .post("/outliers-supervisor-chart")
    .send({
      stateCode: "US_XX",
      width: 570,
      id: "test-officer-metric",
      data: noPrevRateSupervisorData,
    })
    .expect(200);
});
