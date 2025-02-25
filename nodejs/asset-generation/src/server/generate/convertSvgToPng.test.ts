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

import { convertSvgToPng } from "./convertSvgToPng";

test("converts svg to png", async () => {
  const imageData = await convertSvgToPng(
    `<svg xmlns="http://www.w3.org/2000/svg" width="30" height="30"><rect width="10" height="20" x="5" y="5"></rect></svg>`
  );

  expect(imageData).toMatchImageSnapshot();
});
