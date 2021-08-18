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

import { rem, remToPx } from "polished";
import { useMemo } from "react";
import { v4 as uuidv4 } from "uuid";

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

export function useUuid(): string {
  return useMemo(() => uuidv4(), []);
}

/**
 * Scales a raw value (assumed to represent pixels) by the browser's current rem size.
 * Useful when you need a pixel value but want to respect the user's change to base font size
 * (which most UI elements will scale in response to).
 */
export function remScaledPixels(val: number): string {
  return remToPx(rem(val));
}

/**
 * Preferred Moment.js format string for dates in sentence copy.
 * Produces, e.g., "July 30th, 2021"
 */
export const LONG_DATE_FORMAT = "MMMM Do, YYYY";

/**
 * Returns "day" or "days" as is numerically appropriate.
 */
export const inflectDay = (days: number): string => {
  return `day${Math.abs(days) === 1 ? "" : "s"}`;
};

/**
 * Clears information in the browser's local storage that was
 * originally set, but we want to remove entirely.
 *
 * At the moment, this includes all metadata set by Auth0, including
 * the access token, name, user roles, nickname, etc.
 */
export const redactLocalStorageCache = (): void => {
  Object.keys(localStorage).forEach((key) => {
    if (key.startsWith("@@auth0spajs@@::")) {
      localStorage.removeItem(key);
    }
  });
};
