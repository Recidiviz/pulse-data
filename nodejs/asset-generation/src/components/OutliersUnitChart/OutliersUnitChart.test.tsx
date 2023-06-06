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

import { beforeEach, describe, expect, test, vi } from "vitest";

import { convertToImage } from "../../server/generate/convertToImage";
import { renderToStaticSvg } from "../utils";
import {
  fittingUnitDataTransformed,
  overflowingUnitDataTransformed,
} from "./fixtures";
import { OutliersUnitChart } from "./OutliersUnitChart";

const syncHeight = vi.fn();

beforeEach(() => {
  vi.resetAllMocks();
});

describe.each([
  {
    label: "unit that fits within swarm height",
    data: fittingUnitDataTransformed,
    expectedHeight: 213,
  },
  {
    label: "unit overflowing swarm height",
    data: overflowingUnitDataTransformed,
    expectedHeight: 279,
  },
])("data with $label", ({ data, expectedHeight }) => {
  function TestComponent() {
    return (
      <OutliersUnitChart data={data} width={570} syncHeight={syncHeight} />
    );
  }

  test("syncs height to parent", () => {
    renderToStaticSvg(TestComponent);

    expect(syncHeight).toHaveBeenCalledOnce();
    expect(syncHeight).toHaveBeenCalledWith(expectedHeight);
  });

  test("render to image", async () => {
    expect(
      await convertToImage(renderToStaticSvg(TestComponent))
    ).toMatchImageSnapshot({
      // Set a higher failure threshold to account for differences between machines
      failureThreshold: 0.02,
      failureThresholdType: "percent",
      updatePassedSnapshot: true,
    });
  });
});
