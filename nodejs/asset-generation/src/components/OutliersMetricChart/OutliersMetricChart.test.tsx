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

import { convertToImage } from "../../server/convertToImage";
import { renderToStaticSvg } from "../utils";
import { officerData } from "./fixtures";
import { OutliersMetricChart } from "./OutliersMetricChart";

function TestComponent() {
  return (
    <OutliersMetricChart
      data={officerData}
      width={570}
      entityLabel="Officers"
    />
  );
}

test("rendering", () => {
  expect(renderToStaticSvg(TestComponent)).toMatchFileSnapshot(
    "./__snapshots__/OfficerChart.svg"
  );
});

test("image rendering", async () => {
  expect(
    await convertToImage(renderToStaticSvg(TestComponent))
  ).toMatchImageSnapshot({
    // Set a .5% failure threshold to account for differences between machines
    failureThreshold: 0.005,
    failureThresholdType: "percent",
  });
});
