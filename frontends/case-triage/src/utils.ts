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

export const titleCase = (str: string): string => {
  // Step 1. Lowercase the string
  // Step 2. Split the string into an array of strings
  const split: string[] = str.toLowerCase().replace(/_/g, " ").split(" ");
  // str = "i'm a little tea pot".split(' ');

  // Step 3. Create the FOR loop
  for (let i = 0; i < split.length; i += 1) {
    split[i] = split[i].charAt(0).toUpperCase() + split[i].slice(1);
  }
  // Step 4. Return the output
  return split.join(" "); // ["I'm", "A", "Little", "Tea", "Pot"].join(' ') => "I'm A Little Tea Pot"
};

export const caseInsensitiveIncludes = (
  containingString: string,
  substring: string
): boolean => {
  return containingString.toLowerCase().includes(substring.toLowerCase());
};

/**
 * Formats distance from now in natural language. Any time today will
 * be rendered as "today", with larger distances in days, months, etc
 * per [Moment.js](https://momentjs.com/docs/#/displaying/fromnow/) rules.
 */
export function getTimeDifference(date: moment.Moment): string {
  // `date` is generally expected to be a day boundary.
  // We use the beginning of today when calculating distance,
  // thus showing a minimum difference of "a day" rather than "X hours"
  const beginningOfDay = moment().startOf("day");

  if (date.isSame(beginningOfDay, "day")) {
    return "today";
  }

  return date.from(beginningOfDay);
}
