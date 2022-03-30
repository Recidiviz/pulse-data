// Recidiviz - a data platform for criminal justice reform
// Copyright (C) 2021 Recidiviz, Inc.
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
import { CaseType, Gender, SupervisionLevel } from "../ClientsStore/Client";
import { OpportunityType } from "../OpportunityStore/Opportunity";

export type ScoreMinMax = [number, number | null];

export type ScoreMinMaxBySupervisionLevel = Record<
  SupervisionLevel,
  ScoreMinMax
>;

export type AssessmentScoreCutoffs = Record<
  Gender,
  ScoreMinMaxBySupervisionLevel
>;

/**
 * [X, Y] representing X contacts every Y days
 */
export type SupervisionContactFrequency = [number, number];
/**
 * Returns tuple of ["X contact(s)", "X day(s)"] for use in text.
 * Will replace "contact" in the returned string with `singularUnit` if passed
 */
export function parseContactFrequency(
  contactFrequency: SupervisionContactFrequency,
  singularUnit = "contact"
): [string, string] {
  const [contacts, days] = contactFrequency;
  const pluralized = contacts === 1 ? "" : "s";
  const daysPluralized = days === 1 ? "day" : "days";
  return [
    `${contacts} ${singularUnit}${pluralized}`,
    `${days} ${daysPluralized}`,
  ];
}

export type ContactFrequencyByRisk = Record<
  SupervisionLevel,
  SupervisionContactFrequency
>;

export type PolicyReferencesForOpportunities = Record<OpportunityType, string>;

export type SupervisionContactFrequencies = Record<
  CaseType,
  ContactFrequencyByRisk
>;

export type HomeVisitFrequencyByRisk = Record<
  SupervisionLevel,
  SupervisionContactFrequency
>;

export type SupervisionHomeVisitFrequencies = Record<
  CaseType,
  HomeVisitFrequencyByRisk
>;

export type SupervisionLevelNames = Record<SupervisionLevel, string>;

export type Policy = {
  assessmentScoreCutoffs: AssessmentScoreCutoffs;
  docShortName: string;
  omsName: string;
  policyReferencesForOpportunities: PolicyReferencesForOpportunities;
  supervisionContactFrequencies: SupervisionContactFrequencies;
  supervisionLevelNames: SupervisionLevelNames;
  supervisionHomeVisitFrequencies: SupervisionHomeVisitFrequencies;
  supervisionPolicyReference: string | null;
};
