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

import { z } from "zod";

const goalStatusSchema = z.enum(["met", "near", "far"]);

export const outliersMetricChartInputSchema = z.object({
  id: z.string(),
  width: z.number(),
  entityLabel: z.string(),
  data: z.object({
    min: z.number(),
    max: z.number(),
    goal: z.number(),
    entities: z.array(
      z.object({
        name: z.string(),
        rate: z.number(),
        goalStatus: goalStatusSchema,
        previousRate: z.number(),
        previousGoalStatus: goalStatusSchema,
      })
    ),
  }),
});

export type OutliersMetricChartInput = z.infer<
  typeof outliersMetricChartInputSchema
>;

export type ChartData = OutliersMetricChartInput["data"];
