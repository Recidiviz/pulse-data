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

import {
  formatNumberInput,
  isPositiveNumber,
  normalizeToString,
  sanitizeInputValue,
} from "./helperUtils";

describe("sanitizeInputValue", () => {
  test("return previous value if input value is undefined", () => {
    const undefinedInput = sanitizeInputValue(undefined, 2);
    const definedInput = sanitizeInputValue("1", 2);

    expect(undefinedInput).toBe(2);
    expect(definedInput).toBe(1);
  });

  test("return null if empty string input", () => {
    const emptyStringInput = sanitizeInputValue("", 2);
    const nonEmptyStringInput = sanitizeInputValue("text", 2);

    expect(emptyStringInput).toBeNull();
    expect(nonEmptyStringInput).not.toBeNull();
  });

  test("return the number zero for string 0 and 0.00 with decimals", () => {
    const zeroString = sanitizeInputValue("0", null);
    const zeroDecimal = sanitizeInputValue("0.00", null);

    expect(zeroString).toBe(0);
    expect(zeroDecimal).toBe(0);
  });

  test("return value converted to number if convertible", () => {
    const numberString = sanitizeInputValue("123", null);
    const numberStringWithDecimals = sanitizeInputValue("123.2341", null);
    const numberStringWithDecimalsAfterZero = sanitizeInputValue(
      "0.12341",
      null
    );

    expect(numberString).toBe(123);
    expect(numberStringWithDecimals).toBe(123.2341);
    expect(numberStringWithDecimalsAfterZero).toBe(0.12341);
  });

  test("return value as string if not convertible to number", () => {
    const nonNumber = sanitizeInputValue("0.123abc", null);
    expect(typeof nonNumber).toBe("string");
  });
});

describe("normalizeToString", () => {
  test("return string version of value", () => {
    const undefinedInput = normalizeToString(undefined);
    const nullInput = normalizeToString(null);
    const booleanInput = normalizeToString(false);
    const numberInput = normalizeToString(22);
    const stringInput = normalizeToString("Hello");

    expect(undefinedInput).toBe("");
    expect(nullInput).toBe("");
    expect(booleanInput).toBe("false");
    expect(numberInput).toBe("22");
    expect(stringInput).toBe("Hello");
  });
});

describe("formatNumberInput", () => {
  test("return formatted number with commas and decimals", () => {
    const inputWithCommasSpaces = formatNumberInput(
      "   1231223,23,23,3,3.123123123 11  "
    );
    const inputWithSeriesOfNumbers = formatNumberInput("123122323233312");

    expect(inputWithCommasSpaces).toBe("1,231,223,232,333.12312312311");
    expect(inputWithSeriesOfNumbers).toBe("123,122,323,233,312");
  });

  test("return formatted number on first decimal instance", () => {
    const inputWithDecimalAtEnd = formatNumberInput("2,32,3,23,2.");
    expect(inputWithDecimalAtEnd).toBe("2,323,232.");
  });

  test("return input value if not valid", () => {
    const invalidInput = formatNumberInput("12xyz!");
    expect(invalidInput).toBe("12xyz!");
  });
});

describe("isPositiveNumber", () => {
  test("valid positive numbers return true", () => {
    expect(isPositiveNumber("1")).toBe(true);
    expect(isPositiveNumber("12")).toBe(true);
    expect(isPositiveNumber("13")).toBe(true);
    expect(isPositiveNumber("3.4")).toBe(true);
    expect(isPositiveNumber("0")).toBe(true);
  });
  test("negative numbers return false", () => {
    expect(isPositiveNumber("-1")).toBe(false);
    expect(isPositiveNumber("-5")).toBe(false);
    expect(isPositiveNumber("-5.6")).toBe(false);
  });
  test("invalid numbers return false", () => {
    expect(isPositiveNumber("-1 ")).toBe(false);
    expect(isPositiveNumber("0.0.0")).toBe(false);
    expect(isPositiveNumber("five")).toBe(false);
    expect(isPositiveNumber(" ")).toBe(false);
    expect(isPositiveNumber("")).toBe(false);
  });
});
