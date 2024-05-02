// Recidiviz - a data platform for criminal justice reform
// Copyright (C) 2024 Recidiviz, Inc.
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
import { parseISO } from "date-fns";
import { z } from "zod";

function unEnum(raw: string) {
  const splat = raw.split(".");
  return splat[splat.length - 1];
}

export const opportunitySchema = z.object({
  stateCode: z.string(),
  opportunityType: z.string(),
  systemType: z.string().transform(unEnum),
  urlSection: z.string(),
  completionEvent: z.string().transform(unEnum),
  experimentId: z.string(),
  lastUpdatedAt: z.string().transform((r) => parseISO(r)),
  lastUpdatedBy: z.string(),
  gatingFeatureVariant: z.string().nullish(),
});

export type Opportunity = z.infer<typeof opportunitySchema>;
