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

/**
 * Separate multiple people on a list by comma - no comma for the last person on the list
 * @example ['Editor 1', 'Editor 2', 'Editor 3'] would print: `Editor 1, Editor 2, Editor 3`
 */
export const printCommaSeparatedList = (list: string[]): string => {
  const string = list.map((item, i) =>
    i < list.length - 1 ? `${item}, ` : `${item}`
  );
  return string.join(" ");
};

/**
 * Take a string, trim and remove all spacing, and lowercase it.
 * @example normalizeString("All Reports ") will be "allreports"
 */
export const normalizeString = (string: string): string => {
  return string.split(" ").join("").toLowerCase().trim();
};

/**
 * Take a string, replace _ with ' ' space.
 * @example "NOT_STARTED" becomes "NOT STARTED"
 */
export const removeSnakeCase = (string: string): string => {
  return string.split("_").join(" ");
};

/**
 * Concatenate two string keys by an `_` underscore (default) or a specified separator string
 * @returns a single concatenated string
 * @examples
 * combineTwoKeyNames("KEY1", "KEY2") will return "KEY1_KEY2"
 * combineTwoKeyNames("KEY1", "KEY2", "-") will return "KEY1-KEY2"
 */
export const combineTwoKeyNames = (
  key1: string,
  key2: string,
  separator?: string
) => {
  return `${key1}${separator || "_"}${key2}`;
};

/**
 * Remove commas, spaces and trim string
 *
 * @returns a trimmed string free from spaces and commas
 * @example "   1,000,00  0 " becomes "1000000"
 */

export const removeCommaSpaceAndTrim = (
  value: string | undefined
): string | undefined => {
  return value?.replaceAll(",", "").replaceAll(" ", "").trim();
};

/**
 * Sanitize by formatting and converting string input to appropriate value for backend.
 *
 * @param value input value
 * @param previousValue previously saved value retrieved from the backend
 * @returns
 * * `previousValue` from the backend if `value` is undefined
 * * `null` for empty string or no previous value
 * * number `0` for true zeros ("0", "0.000", etc.)
 * * `value` converted to number
 * * `value` itself (if it is not a number)
 */

export const sanitizeInputValue = (
  value: string | undefined,
  previousValue: string | number | boolean | null | undefined
): string | number | boolean | null | undefined => {
  const cleanValue = removeCommaSpaceAndTrim(value);

  if (value === undefined) {
    return previousValue;
  }

  if (cleanValue === "" || (Number(previousValue) !== 0 && !previousValue)) {
    return null;
  }
  if (Number(cleanValue) === 0) {
    return 0;
  }
  return Number(cleanValue) || value;
};
