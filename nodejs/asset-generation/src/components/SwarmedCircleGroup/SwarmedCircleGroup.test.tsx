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

import { ScaleLinear, scaleLinear } from "d3-scale";
import { beforeEach, expect, test } from "vitest";

import { convertSvgToPng } from "../../server/generate/convertSvgToPng";
import { renderToStaticSvg } from "../utils";
import { calculateSwarm } from "./calculateSwarm";
import {
  officersDataNonZeroModeTransformed,
  officersDataZeroModeTransformed,
} from "./fixtures";
import {
  SwarmedCircleGroup,
  SwarmedCircleGroupProps,
} from "./SwarmedCircleGroup";

const WIDTH = 400;
const RADIUS = 4;
let valueScale: ScaleLinear<number, number, never>;

function getTestComponent(
  height: number,
  data: SwarmedCircleGroupProps["data"]
) {
  return function TestComponent() {
    return (
      <svg width={WIDTH} height={height}>
        <SwarmedCircleGroup
          data={data}
          radius={RADIUS}
          opacity={0.6}
          transform={`translate(0 ${height / 2})`}
        />
      </svg>
    );
  };
}

beforeEach(() => {
  valueScale = scaleLinear().range([RADIUS, WIDTH - RADIUS]);
});

test("auto spread, zero-modal distribution", async () => {
  valueScale.domain([
    0,
    Math.max(...officersDataZeroModeTransformed.map((d) => d.value)),
  ]);

  const { swarmPoints, swarmSpread } = calculateSwarm({
    data: officersDataZeroModeTransformed,
    radius: RADIUS,
    valueScale,
  });

  expect(
    await convertSvgToPng(
      renderToStaticSvg(getTestComponent(swarmSpread, swarmPoints))
    )
  ).toMatchImageSnapshot({
    // Set a higher failure threshold to account for differences between machines
    failureThreshold: 0.02,
    failureThresholdType: "percent",
    updatePassedSnapshot: true,
  });
});

test("auto spread, nonzero-modal distribution", async () => {
  valueScale.domain([
    0,
    Math.max(...officersDataNonZeroModeTransformed.map((d) => d.value)),
  ]);

  const { swarmPoints, swarmSpread } = calculateSwarm({
    data: officersDataNonZeroModeTransformed,
    radius: RADIUS,
    valueScale,
  });

  expect(
    await convertSvgToPng(
      renderToStaticSvg(getTestComponent(swarmSpread, swarmPoints))
    )
  ).toMatchImageSnapshot({
    // Set a higher failure threshold to account for differences between machines
    failureThreshold: 0.02,
    failureThresholdType: "percent",
    updatePassedSnapshot: true,
  });
});

test("minimum spread", async () => {
  valueScale.domain([
    0,
    Math.max(...officersDataNonZeroModeTransformed.map((d) => d.value)),
  ]);

  const { swarmPoints, swarmSpread } = calculateSwarm({
    data: officersDataNonZeroModeTransformed,
    radius: RADIUS,
    valueScale,
    minSpread: 550,
  });

  expect(
    await convertSvgToPng(
      renderToStaticSvg(getTestComponent(swarmSpread, swarmPoints))
    )
  ).toMatchImageSnapshot({
    // Set a higher failure threshold to account for differences between machines
    failureThreshold: 0.02,
    failureThresholdType: "percent",
    updatePassedSnapshot: true,
  });
});

test("fixed spread", async () => {
  valueScale.domain([
    0,
    Math.max(...officersDataNonZeroModeTransformed.map((d) => d.value)),
  ]);

  const { swarmPoints, swarmSpread } = calculateSwarm({
    data: officersDataNonZeroModeTransformed,
    radius: RADIUS,
    valueScale,
    spread: 150,
  });

  expect(
    await convertSvgToPng(
      renderToStaticSvg(getTestComponent(swarmSpread, swarmPoints))
    )
  ).toMatchImageSnapshot({
    // Set a higher failure threshold to account for differences between machines
    failureThreshold: 0.02,
    failureThresholdType: "percent",
    updatePassedSnapshot: true,
  });
});
