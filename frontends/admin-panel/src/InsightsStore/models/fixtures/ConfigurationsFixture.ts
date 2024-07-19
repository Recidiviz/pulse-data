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

import { InsightsConfiguration } from "../InsightsConfiguration";

export const rawInsightsConfigurationFixture: Array<InsightsConfiguration> = [
  {
    featureVariant: null,
    supervisionOfficerLabel: "officer",
    supervisionDistrictLabel: "district",
    supervisionUnitLabel: "unit",
    supervisionSupervisorLabel: "supervisor",
    supervisionDistrictManagerLabel: "manager",
    supervisionJiiLabel: "client",
    supervisorHasNoOutlierOfficersLabel:
      "Nice! No officers are outliers on any metrics this month.",
    officerHasNoOutlierMetricsLabel: "Nice! No outlying metrics this month.",
    supervisorHasNoOfficersWithEligibleClientsLabel:
      "Nice! No outstanding opportunities for now.",
    officerHasNoEligibleClientsLabel:
      "Nice! No outstanding opportunities for now.",
    learnMoreUrl: "my-learn-more-url.org",
    atOrBelowRateLabel: "At or below statewide rate",
    exclusionReasonDescription: null,
    id: 1,
    noneAreOutliersLabel: "are outliers",
    slightlyWorseThanRateLabel: "slightly worse than statewide rate",
    status: "ACTIVE",
    updatedAt: "2024-01-26T13:30:00",
    updatedBy: "alexa@recidiviz.org",
    worseThanRateLabel: "Far worse than statewide rate",
    abscondersLabel: "absconders",
    atOrAboveRateLabel: "At or above statewide rate",
    outliersHover:
      "Has a rate on any metric significantly higher than peers - over 1 Interquartile Range above the statewide rate.",
    docLabel: "DOC",
  },
  {
    featureVariant: "featureVariant1",
    supervisionOfficerLabel: "agent",
    supervisionDistrictLabel: "region",
    supervisionUnitLabel: "unit",
    supervisionSupervisorLabel: "supervisor",
    supervisionDistrictManagerLabel: "manager",
    supervisionJiiLabel: "client",
    supervisorHasNoOutlierOfficersLabel:
      "Nice! No officers are outliers on any metrics this month.",
    officerHasNoOutlierMetricsLabel: "Nice! No outlying metrics this month.",
    supervisorHasNoOfficersWithEligibleClientsLabel:
      "Nice! No outstanding opportunities for now.",
    officerHasNoEligibleClientsLabel:
      "Nice! No outstanding opportunities for now.",
    learnMoreUrl: "other-learn-more-url.org",
    atOrBelowRateLabel: "At or below statewide rate",
    exclusionReasonDescription: "not eligible",
    id: 2,
    noneAreOutliersLabel: "are outliers",
    slightlyWorseThanRateLabel: "slightly worse than statewide rate",
    status: "ACTIVE",
    updatedAt: "2024-02-26T13:30:00",
    updatedBy: "jen@recidiviz.org",
    worseThanRateLabel: "Far worse than statewide rate",
    abscondersLabel: "absconders",
    atOrAboveRateLabel: "At or above statewide rate",
    outliersHover:
      "Has a rate on any metric significantly higher than peers - over 1 Interquartile Range above the statewide rate.",
    docLabel: "DOC",
  },

  {
    featureVariant: "featureVariant1",
    supervisionOfficerLabel: "agent",
    supervisionDistrictLabel: "district",
    supervisionUnitLabel: "unit",
    supervisionSupervisorLabel: "supervisor",
    supervisionDistrictManagerLabel: "manager",
    supervisionJiiLabel: "client",
    supervisorHasNoOutlierOfficersLabel:
      "Nice! No officers are outliers on any metrics this month.",
    officerHasNoOutlierMetricsLabel: "Nice! No outlying metrics this month.",
    supervisorHasNoOfficersWithEligibleClientsLabel:
      "Nice! No outstanding opportunities for now.",
    officerHasNoEligibleClientsLabel:
      "Nice! No outstanding opportunities for now.",
    learnMoreUrl: "other-learn-more-url.org",
    atOrBelowRateLabel: "At or below statewide rate",
    exclusionReasonDescription: "not eligible",
    id: 2,
    noneAreOutliersLabel: "are outliers",
    slightlyWorseThanRateLabel: "slightly worse than statewide rate",
    status: "INACTIVE",
    updatedAt: "2024-01-23T13:30:00",
    updatedBy: "jen@recidiviz.org",
    worseThanRateLabel: "Far worse than statewide rate",
    abscondersLabel: "absconders",
    atOrAboveRateLabel: "At or above statewide rate",
    outliersHover:
      "Has a rate on any metric significantly higher/lower than peers - over 1 Interquartile Range above/below the statewide rate.",
    docLabel: "DOC",
  },
];
