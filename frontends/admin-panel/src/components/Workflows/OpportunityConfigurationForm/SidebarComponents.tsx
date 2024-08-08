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

import { MultiEntry } from "./MultiEntry";

const sidebarComponentNames = [
  "CaseNotes",
  "EligibilityDate",
  "UsMiEarlyDischargeIcDetails",
  "UsMiRecommendedSupervisionLevel",
  "UsMoRestrictiveHousing",
  "UsTnCommonlyUsedOverrideCodes",
  "UsMiRestrictiveHousing",

  "Incarceration",
  "ResidentHousing",
  "UsMoIncarceration",
  "UsIdPastTwoYearsAlert",
  "UsIdParoleDates",

  "Supervision",
  "Contact",
  "ClientHousing",
  "FinesAndFees",
  "SpecialConditions",
  "ClientEmployer",
  "Milestones",
  "ClientProfileDetails",
];

export const SidebarComponents = () => (
  <MultiEntry label="Sidebar Components" name="sidebarComponents">
    {(field) => (
      <Form.Item {...field} noStyle>
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
    )}
  </MultiEntry>
);
