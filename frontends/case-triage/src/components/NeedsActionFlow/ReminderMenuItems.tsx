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

import * as React from "react";
import { DropdownMenuItem, DropdownMenuLabel } from "@recidiviz/design-system";
import moment from "moment";
import { OpportunityDeferralDropdownProps } from "./types";

export const ReminderMenuItems = ({
  onDeferred,
}: OpportunityDeferralDropdownProps): JSX.Element => {
  return (
    <>
      <DropdownMenuLabel>Remind Me In</DropdownMenuLabel>
      <DropdownMenuItem
        label="1 day"
        onClick={() => onDeferred(moment.utc().add(1, "day"))}
      />
      <DropdownMenuItem
        label="7 days"
        onClick={() => onDeferred(moment.utc().add(7, "days"))}
      />
      <DropdownMenuItem
        label="30 days"
        onClick={() => onDeferred(moment.utc().add(30, "days"))}
      />
    </>
  );
};
