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
  OutliersUnitChartInput,
  outliersUnitChartInputSchema,
} from "../../server/generate/outliersUnitChart/types";
import { goalStatusSchema } from "../../server/generate/schema/helpers";
import { officersDataZeroMode } from "../SwarmedCircleGroup/fixtures";

export const fittingUnitData: OutliersUnitChartInput["data"] = {
  unitOfficers: [
    {
      name: "Tatiana Alvarez-Thomas",
      rate: 0.19904024430145054,
      goalStatus: "far",
      previousRate: 0.15804024430145053,
    },
    {
      name: "Mario McCarthy",
      rate: 0.10228673915480327,
      goalStatus: "far",
      previousRate: 0.08228673915480327,
    },
    {
      name: "Ryan Luna",
      rate: 0.129823,
      previousRate: 0.121354,
      goalStatus: goalStatusSchema.enum.far,
    },
  ],
  context: {
    target: 0.05428241659992843,
    otherOfficers: officersDataZeroMode,
  },
};

export const fittingUnitDataTransformed =
  outliersUnitChartInputSchema.shape.data.parse(fittingUnitData);

export const overflowingUnitData = structuredClone(fittingUnitData);
// needs enough entries to overflow the height of the swarm
overflowingUnitData.unitOfficers.push(
  ...[
    {
      name: "Julian Stewart",
      rate: 0.265398,
      previousRate: 0.27423,
      goalStatus: goalStatusSchema.enum.far,
    },

    {
      name: "Grace Fletcher",
      rate: 0.173946,
      previousRate: 0.085724,
      goalStatus: goalStatusSchema.enum.far,
    },
    {
      name: "Kenneth Hodges",
      rate: 0.16195862,
      previousRate: 0.22047,
      goalStatus: goalStatusSchema.enum.far,
    },
  ]
);

export const overflowingUnitDataTransformed =
  outliersUnitChartInputSchema.shape.data.parse(overflowingUnitData);
