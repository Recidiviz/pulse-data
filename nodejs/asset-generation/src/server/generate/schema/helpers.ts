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

export const targetStatusSchema = z.enum(["MET", "NEAR", "FAR"]);

export type TargetStatus = z.infer<typeof targetStatusSchema>;

export const valuesByTargetStatusSchema = z
  // receiving this as a mapping is more size-efficient over the network...
  .record(targetStatusSchema, z.array(z.number()))
  // ...but spreading the mapping across all records will make plotting easier
  .transform((mapping) => {
    const flattenedRecords: {
      value: number;
      targetStatus: TargetStatus;
    }[] = [];
    targetStatusSchema.options.forEach((targetStatus) => {
      const values = mapping[targetStatus];
      if (values) {
        flattenedRecords.push(
          ...values.map((value) => ({ value, targetStatus }))
        );
      }
    });
    return flattenedRecords;
  });

export const chartInputSchemaBase = z.object({
  stateCode: z.string(),
  id: z.string(),
  width: z.number(),
});
