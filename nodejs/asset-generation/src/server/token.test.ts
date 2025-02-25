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

import { expect, test } from "vitest";

import { getTamperedTestToken, getTestToken } from "./testUtils";
import { getUrlFromAssetToken } from "./token";

test("token for URL", async () => {
  const url = "/test/url.png";
  const token = await getTestToken(url);
  expect(await getUrlFromAssetToken(token)).toBe(url);
});

test("doctored token is rejected", async () => {
  const doctoredToken = await getTamperedTestToken();

  expect(() => getUrlFromAssetToken(doctoredToken)).rejects.toThrow();
});
