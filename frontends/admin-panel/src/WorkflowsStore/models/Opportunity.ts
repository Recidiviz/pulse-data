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

import { nullishAsUndefined } from "./OpportunityConfiguration";

function unEnum(raw: string) {
  const splat = raw.split(".");
  return splat[splat.length - 1];
}

export const babyOpportunitySchema = z.object({
  gatingFeatureVariant: nullishAsUndefined(z.string()),
  homepagePosition: nullishAsUndefined(z.number()),
});

export type BabyOpportunity = z.infer<typeof babyOpportunitySchema>;

export const opportunitySchema = babyOpportunitySchema.extend({
  stateCode: z.string(),
  opportunityType: z.string(),
  systemType: z.string().transform(unEnum),
  urlSection: z.string(),
  completionEvent: z.string().transform(unEnum),
  experimentId: z.string(),
  lastUpdatedAt: nullishAsUndefined(z.string()).transform(
    (r) => r && parseISO(r)
  ),
  lastUpdatedBy: nullishAsUndefined(z.string()),
});

export type Opportunity = z.infer<typeof opportunitySchema>;

export const updatedStringForOpportunity = (opp: Opportunity) =>
  opp.lastUpdatedAt
    ? `${opp.lastUpdatedAt.toLocaleString()} by ${opp.lastUpdatedBy}`
    : "Not yet provisioned";
