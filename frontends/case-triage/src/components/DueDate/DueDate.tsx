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
import { BaseDueDate, PastDueDate, TodayDueDate } from "./DueDate.styles";

export interface DueDateProps {
  date: moment.Moment | null;
}

export const DueDate: React.FC<DueDateProps> = ({ date }: DueDateProps) => {
  if (!date) {
    return <BaseDueDate>Not required</BaseDueDate>;
  }

  const timeAgo = date.fromNow(true).split("");

  const formatted = timeAgo.join("");

  if (date.isSame(moment(), "day")) {
    return <TodayDueDate>Today</TodayDueDate>;
  }

  if (date.isAfter(moment(), "day")) {
    return <BaseDueDate>In {formatted}</BaseDueDate>;
  }

  timeAgo[0] = timeAgo[0].toUpperCase();

  return <PastDueDate>{formatted} ago</PastDueDate>;
};
