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

export interface DueDateProps {
  date: moment.Moment | null;
}

type DueDateStatus = "Today" | "Past" | "Future";

export const useDueDateStatus = ({
  date,
}: DueDateProps):
  | { status: DueDateStatus; timeDifference: string }
  | undefined => {
  if (!date) return;

  let status: DueDateStatus;
  let timeDifference;

  const beginningOfDay = moment().startOf("day");

  // Upcoming contacts are set to day boundaries. Use the beginning of today when calculating distance
  // Thus, showing a minimum unit of "In a day" rather than "In X hours"
  const relativeTime = date.from(beginningOfDay, true);

  if (date.isSame(beginningOfDay, "day")) {
    status = "Today";
    timeDifference = "today";
  } else if (date.isAfter(beginningOfDay, "day")) {
    status = "Future";
    timeDifference = `in ${relativeTime}`;
  } else {
    status = "Past";
    timeDifference = `${relativeTime} ago`;
  }

  return { status, timeDifference };
};
