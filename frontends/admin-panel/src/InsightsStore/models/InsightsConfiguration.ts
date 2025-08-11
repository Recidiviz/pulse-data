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

export const insightsConfigurationSchema = z.object({
  featureVariant: z.string().nullable(),
  supervisionOfficerLabel: z.string(),
  supervisionDistrictLabel: z.string(),
  supervisionUnitLabel: z.string(),
  supervisionSupervisorLabel: z.string(),
  supervisionDistrictManagerLabel: z.string(),
  supervisionJiiLabel: z.string(),
  supervisorHasNoOutlierOfficersLabel: z.string(),
  officerHasNoOutlierMetricsLabel: z.string(),
  supervisorHasNoOfficersWithEligibleClientsLabel: z.string(),
  officerHasNoEligibleClientsLabel: z.string(),
  learnMoreUrl: z.string(),
  atOrBelowRateLabel: z.string(),
  exclusionReasonDescription: z.string().nullable(),
  id: z.number(),
  noneAreOutliersLabel: z.string(),
  slightlyWorseThanRateLabel: z.string(),
  status: z.string(),
  updatedAt: z.string(),
  updatedBy: z.string(),
  worseThanRateLabel: z.string(),
  abscondersLabel: z.string(),
  atOrAboveRateLabel: z.string(),
  outliersHover: z.string(),
  actionStrategyCopy: z.record(
    z.object({
      prompt: z.string(),
      body: z.string(),
    })
  ),
  vitalsMetricsMethodologyUrl: z.string(),
});

export type InsightsConfiguration = z.infer<typeof insightsConfigurationSchema>;
