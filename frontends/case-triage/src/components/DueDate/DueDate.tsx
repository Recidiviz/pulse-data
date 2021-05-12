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

import moment from "moment";
import * as React from "react";
import Tooltip from "../Tooltip";
import { BaseDueDate, PastDueDate, TodayDueDate } from "./DueDate.styles";

export interface DueDateProps {
  date: moment.Moment | null;
}

export const DueDate: React.FC<DueDateProps> = ({ date }: DueDateProps) => {
  if (!date) {
    return <BaseDueDate>Contact not required</BaseDueDate>;
  }

  const beginningOfDay = moment().startOf("day");

  // Upcoming contacts are set to day boundaries. Use the beginning of today when calculating distance
  // Thus, showing a minimum unit of "In a day" rather than "In X hours"
  const timeAgo = date.from(beginningOfDay, true);

  if (date.isSame(beginningOfDay, "day")) {
    return (
      <Tooltip title="Face to Face Contact recommended today">
        <TodayDueDate>Contact today</TodayDueDate>
      </Tooltip>
    );
  }

  if (date.isAfter(beginningOfDay, "day")) {
    return (
      <Tooltip title={`Face to Face Contact recommended in ${timeAgo}`}>
        <BaseDueDate>Contact in {timeAgo}</BaseDueDate>
      </Tooltip>
    );
  }

  return (
    <Tooltip title={`Face to Face Contact recommended ${timeAgo} ago`}>
      <PastDueDate>Contact due {timeAgo} ago</PastDueDate>
    </Tooltip>
  );
};
