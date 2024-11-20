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

export function nullishAsUndefined<T extends z.ZodTypeAny>(schema: T) {
  return schema.nullish().transform((output) => {
    return output === null ? undefined : output;
  });
}

const snoozeConfigurationSchema = z
  .object({
    defaultSnoozeDays: z.number(),
    maxSnoozeDays: z.number(),
  })
  .or(
    z.object({
      autoSnoozeParams: z.object({
        // It would be nice to type the types we know about, but I'm not sure
        // how to do that without causing a parse error on unknown types
        type: z.string(),
        params: z.record(z.any()),
      }),
    })
  );

const criteriaCopySchema = z.array(
  z.object({
    key: z.string(),
    text: z.string(),
    tooltip: nullishAsUndefined(z.string()),
  })
);

export const notificationsSchema = z.array(
  z.object({
    id: z.string(),
    title: nullishAsUndefined(z.string()),
    body: z.string(),
    cta: nullishAsUndefined(z.string()),
  })
);

// A BabyOpportunityConfigurationSchema just contains the parts
// that are set in the form, not those that are set by the backend
// or presenter
export const babyOpportunityConfigurationSchema = z
  .object({
    variantDescription: z.string(),
    revisionDescription: z.string(),
    displayName: z.string(),
    featureVariant: nullishAsUndefined(z.string()),
    dynamicEligibilityText: z.string(),
    callToAction: z.string(),
    subheading: nullishAsUndefined(z.string()),
    snooze: nullishAsUndefined(snoozeConfigurationSchema),
    denialReasons: z.array(z.object({ key: z.string(), text: z.string() })),
    denialText: nullishAsUndefined(z.string()),
    initialHeader: nullishAsUndefined(z.string()),
    eligibleCriteriaCopy: criteriaCopySchema,
    ineligibleCriteriaCopy: criteriaCopySchema,
    sidebarComponents: z.array(z.string()),
    methodologyUrl: z.string(),
    isAlert: z.boolean(),
    priority: z.enum(["HIGH", "NORMAL"]),
    eligibilityDateText: nullishAsUndefined(z.string()),
    hideDenialRevert: z.boolean(),
    tooltipEligibilityText: nullishAsUndefined(z.string()),
    tabGroups: nullishAsUndefined(
      z.array(z.object({ key: z.string(), tabs: z.string().array() }))
    ),
    notifications: notificationsSchema,
    compareBy: nullishAsUndefined(
      z.array(
        z.object({
          field: z.string(),
          sortDirection: z.string().optional(),
          undefinedBehavior: z.string().optional(),
        })
      )
    ),
    zeroGrantsTooltip: nullishAsUndefined(z.string()),
    stagingId: nullishAsUndefined(z.number()),
  })
  .strict();
// strict() prevents us from accidentally dropping fields the frontend doesn't know about

export type BabyOpportunityConfiguration = z.infer<
  typeof babyOpportunityConfigurationSchema
>;

export const opportunityConfigurationSchema =
  babyOpportunityConfigurationSchema.extend({
    id: z.number(),
    stateCode: z.string(),
    createdAt: z.string(),
    createdBy: z.string(),
    status: z.enum(["ACTIVE", "INACTIVE"]),
  });

export type OpportunityConfiguration = z.infer<
  typeof opportunityConfigurationSchema
>;
