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

import { convertSvgToPng } from "../../server/generate/convertSvgToPng";
import { renderToStaticSvg } from "../utils";
import {
  cappedSwarmSupervisorDataTransformed,
  fittingSupervisorDataTransformed,
  noPrevRateSupervisorDataTransformed,
  overflowingSupervisorDataTransformed,
} from "./fixtures";
import { OutliersSupervisorChart } from "./OutliersSupervisorChart";

beforeEach(() => {
  vi.resetAllMocks();
});

describe.each([
  {
    label: "highlights that fit within swarm height",
    data: fittingSupervisorDataTransformed,
  },
  {
    label: "highlights overflowing swarm height",
    data: overflowingSupervisorDataTransformed,
  },
  {
    label: "highlights with missing prevRate",
    data: noPrevRateSupervisorDataTransformed,
  },
  {
    label: "swarm overflowing max height",
    data: cappedSwarmSupervisorDataTransformed,
  },
])("data with $label", ({ data }) => {
  function TestComponent() {
    return <OutliersSupervisorChart data={data} width={570} />;
  }

  test("render to image", async () => {
    expect(
      await convertSvgToPng(renderToStaticSvg(TestComponent))
    ).toMatchImageSnapshot({
      // Set a higher failure threshold to account for differences between machines
      failureThreshold: 0.02,
      failureThresholdType: "percent",
      updatePassedSnapshot: true,
    });
  });
});
