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

import { supervisorsData } from "../../../components/OutliersAggregatedChart/fixtures";
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
    .post("/outliers-aggregated-chart")
    .send({
      stateCode: "US_XX",
      width: 570,
      aggregationType: "SUPERVISION_OFFICER_SUPERVISOR",
      id: "test-director-test-metric",
      data: supervisorsData,
    })
    .expect(200);

  expect(writeFile).toHaveBeenCalledWith(
    expect.stringMatching(
      /^outliers-aggregated-supervision-officer-supervisor-chart\/US_XX\/2023-05-03\/test-director-test-metric\.png$/
    ),
    expect.any(Buffer)
  );

  expect(response.body).toMatchInlineSnapshot(`
    {
      "url": "/asset/eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJzdWIiOiJvdXRsaWVycy1hZ2dyZWdhdGVkLXN1cGVydmlzaW9uLW9mZmljZXItc3VwZXJ2aXNvci1jaGFydC9VU19YWC8yMDIzLTA1LTAzL3Rlc3QtZGlyZWN0b3ItdGVzdC1tZXRyaWMucG5nIiwiaWF0IjoxNjgzMDcyMDAwfQ.tvPagjSVgtFKyGVMRt2gZA-_vqCv17H1rjTJBL6oBG8",
    }
  `);
});
