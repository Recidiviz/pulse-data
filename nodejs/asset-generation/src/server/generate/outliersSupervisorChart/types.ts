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

import { GoalStatus, goalStatusSchema } from "../schema/helpers";

export const outliersSupervisorChartInputSchema = z.object({
  stateCode: z.string(),
  id: z.string(),
  width: z.number(),
  data: z.object({
    target: z.number(),
    otherOfficers: z
      // receiving this as a mapping is more size-efficient over the network...
      .record(goalStatusSchema, z.array(z.number()))
      // ...but spreading the mapping across all records will make plotting easier
      .transform((mapping) => {
        const flattenedRecords: { value: number; goalStatus: GoalStatus }[] =
          [];
        goalStatusSchema.options.forEach((goalStatus) => {
          const values = mapping[goalStatus];
          if (values) {
            flattenedRecords.push(
              ...values.map((value) => ({ value, goalStatus }))
            );
          }
        });
        return flattenedRecords;
      }),
    highlightedOfficers: z.array(
      z.object({
        name: z.string(),
        rate: z.number(),
        goalStatus: goalStatusSchema,
        previousRate: z.number(),
      })
    ),
  }),
});

export type OutliersSupervisorChartInput = z.input<
  typeof outliersSupervisorChartInputSchema
>;

export type OutliersSupervisorChartInputTransformed = z.infer<
  typeof outliersSupervisorChartInputSchema
>;
