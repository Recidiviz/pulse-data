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

import assertNever from "assert-never";
import moment from "moment";
import * as React from "react";
import Tooltip from "../Tooltip";
import { BaseDueDate, PastDueDate, TodayDueDate } from "./DueDate.styles";
import { useDueDateStatus } from "./useDueDateStatus";

export interface DueDateProps {
  date: moment.Moment | null;
}

export const DueDate: React.FC<DueDateProps> = ({ date }: DueDateProps) => {
  const due = useDueDateStatus({ date });

  if (!due) {
    return <BaseDueDate>Contact not required</BaseDueDate>;
  }

  let DateComponent;
  const { status, timeDifference } = due;
  switch (status) {
    case "Today":
      DateComponent = TodayDueDate;
      break;
    case "Future":
      DateComponent = BaseDueDate;
      break;
    case "Past":
      DateComponent = PastDueDate;
      break;
    default:
      assertNever(status);
  }

  return (
    <Tooltip title={`Face to Face Contact recommended ${timeDifference}`}>
      <DateComponent>
        Contact {status === "Past" ? "due" : ""} {timeDifference}
      </DateComponent>
    </Tooltip>
  );
};
