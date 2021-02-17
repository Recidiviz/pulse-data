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

export type ScoreMinMax = [number, number | null];

export type ScoreMinMaxBySupervisionLevel = Record<
  SupervisionLevel,
  ScoreMinMax
>;

export type AssessmentScoreCutoffs = Record<
  Gender,
  ScoreMinMaxBySupervisionLevel
>;

// X contacts every Y days
export type SupervisionContactFrequency = [number, number];

export type ContactFrequencyByRisk = Record<
  SupervisionLevel,
  SupervisionContactFrequency
>;

export type SupervisionContactFrequencies = Record<
  CaseType,
  ContactFrequencyByRisk
>;

export interface Policy {
  assessmentScoreCutoffs: AssessmentScoreCutoffs;
  supervisionContactFrequencies: SupervisionContactFrequencies;
}
