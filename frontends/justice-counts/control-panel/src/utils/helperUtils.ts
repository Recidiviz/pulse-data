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

import { MetricContext } from "../shared/types";

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

export const removeCommaSpaceAndTrim = (string: string) => {
  return string?.replaceAll(",", "").replaceAll(" ", "").trim();
};

/**
 * Formats string version of numbers into string format with thousands separator
 *
 * @returns a string representation of a number with commas
 * @example "   1231223,23,23,3,3.123123123 11  " " becomes "1,231,223,232,333.12312312311"
 */

export const formatNumberInput = (
  value: string | undefined
): string | undefined => {
  if (value === undefined) {
    return undefined;
  }

  const maxNumber = 999_999_999_999_999; // 1 quadrillion
  const cleanValue = removeCommaSpaceAndTrim(value);
  const splitValues = cleanValue.split(".");

  if (Number(cleanValue) > maxNumber) {
    return Number(cleanValue.slice(0, 15)).toLocaleString();
  }

  if (splitValues && splitValues.length === 2) {
    if (cleanValue[cleanValue.length - 1] === ".") {
      return Number(splitValues[0]) !== 0 && Number(splitValues[0])
        ? `${Number(splitValues[0]).toLocaleString()}.`
        : value;
    }

    if (cleanValue.includes(".")) {
      const [wholeNumber, decimal] = cleanValue.split(".");
      return Number(wholeNumber)
        ? `${Number(wholeNumber).toLocaleString()}.${decimal}`
        : value;
    }
  }
  return Number(cleanValue) ? Number(cleanValue).toLocaleString() : value;
};

/**
 * Sanitize by formatting and converting string input to appropriate value for backend.
 *
 * @param value input value
 * @param previousValue previously saved value retrieved from the backend
 * @returns
 * * `previousValue` from the backend if `value` is undefined
 * * `null` for empty string
 * * number `0` for true zeros ("0", "0.000", etc.)
 * * `value` converted to number
 * * `value` itself (if it is not a number) or if the type is "TEXT"
 */

export const sanitizeInputValue = (
  value: string | undefined,
  previousValue: string | number | boolean | null | undefined,
  type?: MetricContext["type"]
): string | number | boolean | null | undefined => {
  if (value === undefined) {
    return previousValue;
  }
  const cleanValue = removeCommaSpaceAndTrim(value);
  if (cleanValue === "") {
    return null;
  }
  if (type === "TEXT") {
    return value;
  }
  if (Number(cleanValue) === 0) {
    return 0;
  }
  return Number(cleanValue) || value;
};

/**
 * Converts string | number | boolean | null | undefined into string equivalents that conforms to a text input
 *
 * @returns a string, "" empty string to represent null and undefined, stringified version of number and boolean
 */
export const normalizeToString = (
  value: string | number | boolean | null | undefined
): string => {
  const stringValue = value?.toString();
  return !stringValue ? "" : stringValue;
};

/**
 * Group a list of objects based on property value
 * @param arr list of objects
 * @param key name of the property on which to perform the grouping
 * @returns dictionary of property value to list of objects with that value
 */
// eslint-disable-next-line @typescript-eslint/no-explicit-any
export const groupBy = <T, K extends keyof any>(arr: T[], key: (i: T) => K) => {
  const result = {} as Record<K, T[]>;
  arr.forEach((item) => {
    if (!result[key(item)]) {
      result[key(item)] = [];
    }
    result[key(item)].push(item);
  });
  return result;
};
