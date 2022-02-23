/*
 * Recidiviz - a data platform for criminal justice reform
 * Copyright (C) 2022 Recidiviz, Inc.
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <https://www.gnu.org/licenses/>.
 * =============================================================================
 */

import { H4, spacing } from "@recidiviz/design-system";
import { observer } from "mobx-react-lite";
import { rem } from "polished";
import React from "react";
import styled from "styled-components/macro";

import { useDataStore } from "../StoreProvider";
import { daysFromNow } from "../utils";

const ProfileItem = styled(H4)`
  padding: ${rem(spacing.lg)} ${rem(spacing.xl)};
  grid-order: 1;
`;

const ProfileUpcomingDischarge: React.FC = () => {
  const { caseStore } = useDataStore();

  const expectedDischargeDate =
    caseStore.activeClient?.upcomingDischargeCase?.expectedDischargeDate;

  if (!expectedDischargeDate) {
    return <div />;
  }

  const daysUntilDischarge = daysFromNow(expectedDischargeDate.toDate());

  if (daysUntilDischarge > 0) {
    return <ProfileItem>Discharge in {daysUntilDischarge} days</ProfileItem>;
  }

  return (
    <ProfileItem>
      Discharge available {Math.abs(daysUntilDischarge)} days ago!
    </ProfileItem>
  );
};

export default observer(ProfileUpcomingDischarge);
