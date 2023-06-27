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

import {
  chartInputSchemaBase,
  valuesByTargetStatusSchema,
} from "../schema/helpers";

export const outliersAggregatedChartInputSchema = chartInputSchemaBase.extend({
  aggregationType: z.enum([
    "SUPERVISION_DISTRICT",
    "SUPERVISION_OFFICER_SUPERVISOR",
  ]),
  data: z.object({
    target: z.number(),
    entities: z.array(
      z.object({
        name: z.string(),
        officersFarPct: z.number(),
        prevOfficersFarPct: z.number(),
        officerRates: valuesByTargetStatusSchema,
      })
    ),
  }),
});

export type OutliersAggregatedChartInput = z.input<
  typeof outliersAggregatedChartInputSchema
>;

export type OutliersAggregatedChartInputTransformed = z.infer<
  typeof outliersAggregatedChartInputSchema
>;
