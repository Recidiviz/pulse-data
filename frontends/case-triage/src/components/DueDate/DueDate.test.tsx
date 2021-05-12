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
import MockDate from "mockdate";
import moment from "moment";
import { render } from "@testing-library/react";
import { DueDate } from "./DueDate";

const MidnightFeb26 = "2021-02-26T00:00:00.000";
const EveningFeb26 = "2021-02-26T20:00:00.000";

test("DueDate", () => {
  MockDate.set(MidnightFeb26);

  // Long ago
  let { getByText } = render(<DueDate date={moment("2021-01-01")} />);
  getByText("Contact due 2 months ago");

  // Today
  ({ getByText } = render(<DueDate date={moment("2021-02-26")} />));
  getByText("Contact today");

  MockDate.set(EveningFeb26);

  // Due date is 4 hours from now, rounds to 1 day
  ({ getByText } = render(<DueDate date={moment("2021-02-27")} />));
  getByText("Contact in a day");

  // Overdue
  ({ getByText } = render(<DueDate date={moment("2021-02-20")} />));
  getByText("Contact due 6 days ago");

  // Not required
  ({ getByText } = render(<DueDate date={null} />));
  getByText("Contact not required");

  MockDate.reset();
});
