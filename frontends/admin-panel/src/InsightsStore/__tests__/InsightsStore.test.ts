// Recidiviz - a data platform for criminal justice reform
// Copyright (C) 2024 Recidiviz, Inc.
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

import { configure } from "mobx";

import { getInsightsStateCodeInfo } from "../../AdminPanelAPI/InsightsAPI";
import { InsightsStore } from "../InsightsStore";
import { rawStateCodeInfoFixture } from "../models/fixtures/StateCodeInfoFixture";

jest.mock("../../AdminPanelAPI/InsightsAPI");
const mockGetInsightsStateCodeInfo = getInsightsStateCodeInfo as jest.Mock;

let store: InsightsStore;

beforeEach(async () => {
  jest.resetModules();
  configure({ safeDescriptors: false });
  store = new InsightsStore();
  mockGetInsightsStateCodeInfo.mockResolvedValue(rawStateCodeInfoFixture);
});

afterEach(() => {
  jest.restoreAllMocks();
  configure({ safeDescriptors: true });
});

test("hydrate store", async () => {
  expect(store.hydrationState.status).toEqual("needs hydration");
  await store.hydrate();
  expect(store.hydrationState.status).toEqual("hydrated");
});

test("hydration -- error", async () => {
  const error = new Error("hydrationError");
  mockGetInsightsStateCodeInfo.mockRejectedValue(error);
  await store.hydrate();
  expect(store.hydrationState).toEqual({
    status: "failed",
    error,
  });
});

test("stateCodeInfo", async () => {
  await store.hydrate();

  expect(store.stateCodeInfo).toMatchSnapshot();
});
