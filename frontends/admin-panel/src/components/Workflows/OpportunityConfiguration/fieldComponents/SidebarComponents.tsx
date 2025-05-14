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

import { Form, Select } from "antd";

import { MultiEntryChild } from "../../formUtils/MultiEntry";
import { StaticValue } from "../../formUtils/StaticValue";

const sidebarComponentNames = [
  // Opportunity-related components
  "CaseNotes",
  "EligibilityDate",
  "SentenceDates",
  "UsMiEarlyDischargeIcDetails",
  "UsMiRecommendedSupervisionLevel",
  "UsMoRestrictiveHousing",
  "UsTnCommonlyUsedOverrideCodes",
  "UsMiRestrictiveHousing",
  "UsNeORASScores",
  "UsNeSpecialConditions",
  "UsIaActionPlansAndNotes",

  // Resident-related components
  "Incarceration",
  "ResidentHousing",
  "UsMoIncarceration",
  "UsIdPastTwoYearsAlert",
  "UsIdParoleDates",
  "UsAzDates",
  "UsAzAcisInformation",

  // Client-related components
  "Supervision",
  "Contact",
  "ClientHousing",
  "FinesAndFees",
  "SpecialConditions",
  "ClientEmployer",
  "Milestones",
  "ClientProfileDetails",
  "ActiveSentences",
  "UsUtDates",
];

export const SidebarComponentsView: MultiEntryChild = ({ name, ...field }) => (
  <Form.Item {...field} noStyle name={[name]}>
    <StaticValue />
  </Form.Item>
);

export const SidebarComponentsEdit: MultiEntryChild = ({ name }) => (
  <Form.Item name={name} noStyle>
    <Select
      style={{
        width: 300,
      }}
      options={sidebarComponentNames.map((v) => ({
        value: v,
        label: v,
      }))}
    />
  </Form.Item>
);
