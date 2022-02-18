// Recidiviz - a data platform for criminal justice reform
// Copyright (C) 2022 Recidiviz, Inc.
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

import { formatDistanceToNow } from "date-fns";
import type { Timestamp } from "firebase/firestore";
import { lowerCase, startCase } from "lodash";

import type { UserMapping } from "./DataStores/UserStore";

export const titleCase = (input: string): string => {
  return startCase(lowerCase(input));
};

export function getUserName(email: string, users: UserMapping): string {
  const mappedUser = users[email];
  if (mappedUser) return mappedUser.name;
  if (email.endsWith("recidiviz.org")) {
    return "Recidiviz Admin";
  }
  return email;
}

export function formatTimestampRelative(ts: Timestamp): string {
  return formatDistanceToNow(ts.toDate()).replace("about ", "");
}
