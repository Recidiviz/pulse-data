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
import { z } from "zod";

// const snoozeConfigurationSchema = z.object({
//   defaultSnoozeDays: z.number(),
//   maxSnoozeDays: z.number(),
// });

const criteriaCopySchema = z.record(
  z.object({
    text: z.string(),
    tooltip: z.string().optional(),
  })
);

export const opportunityConfigurationSchema = z.object({
  id: z.number(),
  stateCode: z.string(),
  displayName: z.string(),
  featureVariant: z.string().nullish(),
  dynamicEligibilityText: z.string(),
  callToAction: z.string(),
  // snooze: snoozeConfigurationSchema.nullish(),
  denialReasons: z.record(z.string()),
  denialText: z.string().nullish(),
  eligibleCriteriaCopy: criteriaCopySchema,
  ineligibleCriteriaCopy: criteriaCopySchema,
  sidebarComponents: z.array(z.string()),
  methodologyUrl: z.string(),

  description: z.string(),
  createdAt: z.string(),
  createdBy: z.string(),
  status: z.enum(["ACTIVE", "INACTIVE"]),
});

export type OpportunityConfiguration = z.infer<
  typeof opportunityConfigurationSchema
>;
