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

import {
  OutliersSupervisorChartInput,
  outliersSupervisorChartInputSchema,
} from "../../server/generate/outliersSupervisorChart/types";
import { targetStatusSchema } from "../../server/generate/schema/helpers";
import {
  officersDataNonZeroMode,
  officersDataZeroMode,
} from "../SwarmedCircleGroup/fixtures";

export const fittingSupervisorData: OutliersSupervisorChartInput["data"] = {
  highlightedOfficers: [
    {
      name: "Jeanette Schneider-Cox",
      rate: 0.19904024430145054,
      targetStatus: targetStatusSchema.enum.FAR,
      prevRate: 0.15804024430145053,
    },
    {
      name: "Mario McCarthy",
      rate: 0.10228673915480327,
      targetStatus: targetStatusSchema.enum.FAR,
      prevRate: 0.08228673915480327,
    },
    {
      name: "Ryan Luna",
      rate: 0.129823,
      prevRate: 0.121354,
      targetStatus: targetStatusSchema.enum.FAR,
    },
  ],
  target: 0.05428241659992843,
  otherOfficers: officersDataZeroMode,
};

export const fittingSupervisorDataTransformed =
  outliersSupervisorChartInputSchema.shape.data.parse(fittingSupervisorData);

export const noPrevRateSupervisorData = structuredClone(fittingSupervisorData);
noPrevRateSupervisorData.highlightedOfficers =
  noPrevRateSupervisorData.highlightedOfficers.slice(0, 1);
noPrevRateSupervisorData.highlightedOfficers[0].prevRate = null;

export const noPrevRateSupervisorDataTransformed =
  outliersSupervisorChartInputSchema.shape.data.parse(noPrevRateSupervisorData);

export const overflowingSupervisorData = structuredClone(fittingSupervisorData);
// needs enough entries to overflow the height of the swarm
overflowingSupervisorData.highlightedOfficers.push(
  ...[
    {
      name: "Julian Stewart",
      rate: 0.265398,
      prevRate: 0.27423,
      targetStatus: targetStatusSchema.enum.FAR,
    },

    {
      name: "Grace Fletcher",
      rate: 0.173946,
      prevRate: 0.085724,
      targetStatus: targetStatusSchema.enum.FAR,
    },
    {
      name: "Kenneth Hodges",
      rate: 0.16195862,
      prevRate: 0.22047,
      targetStatus: targetStatusSchema.enum.FAR,
    },
  ]
);

export const overflowingSupervisorDataTransformed =
  outliersSupervisorChartInputSchema.shape.data.parse(
    overflowingSupervisorData
  );

export const cappedSwarmSupervisorData = structuredClone(fittingSupervisorData);
cappedSwarmSupervisorData.otherOfficers = officersDataNonZeroMode;

export const cappedSwarmSupervisorDataTransformed =
  outliersSupervisorChartInputSchema.shape.data.parse(
    cappedSwarmSupervisorData
  );
