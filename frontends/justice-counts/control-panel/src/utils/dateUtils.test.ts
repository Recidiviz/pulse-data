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
  printDateRangeFromMonthYear,
  printElapsedDaysMonthsYearsSinceDate,
} from "./dateUtils";

describe("printDateRangeFromMonthYear", () => {
  test("monthly", () => {
    const result1 = printDateRangeFromMonthYear(1, 2022);
    const result2 = printDateRangeFromMonthYear(2, 2022);
    const result3 = printDateRangeFromMonthYear(3, 2022);
    const result4 = printDateRangeFromMonthYear(4, 2022);
    const result5 = printDateRangeFromMonthYear(5, 2022);
    const result6 = printDateRangeFromMonthYear(6, 2022);
    const result7 = printDateRangeFromMonthYear(7, 2022);
    const result8 = printDateRangeFromMonthYear(8, 2022);
    const result9 = printDateRangeFromMonthYear(9, 2022);
    const result10 = printDateRangeFromMonthYear(10, 2022);
    const result11 = printDateRangeFromMonthYear(11, 2022);
    const result12 = printDateRangeFromMonthYear(12, 2022);
    const result13 = printDateRangeFromMonthYear(1, 2020);
    const result14 = printDateRangeFromMonthYear(12, 2020);
    expect(result1).toEqual("January 1, 2022 - January 31, 2022");
    expect(result2).toEqual("February 1, 2022 - February 28, 2022");
    expect(result3).toEqual("March 1, 2022 - March 31, 2022");
    expect(result4).toEqual("April 1, 2022 - April 30, 2022");
    expect(result5).toEqual("May 1, 2022 - May 31, 2022");
    expect(result6).toEqual("June 1, 2022 - June 30, 2022");
    expect(result7).toEqual("July 1, 2022 - July 31, 2022");
    expect(result8).toEqual("August 1, 2022 - August 31, 2022");
    expect(result9).toEqual("September 1, 2022 - September 30, 2022");
    expect(result10).toEqual("October 1, 2022 - October 31, 2022");
    expect(result11).toEqual("November 1, 2022 - November 30, 2022");
    expect(result12).toEqual("December 1, 2022 - December 31, 2022");
    expect(result13).toEqual("January 1, 2020 - January 31, 2020");
    expect(result14).toEqual("December 1, 2020 - December 31, 2020");
  });

  test("annual", () => {
    const result1 = printDateRangeFromMonthYear(1, 2022, "ANNUAL");
    const result2 = printDateRangeFromMonthYear(7, 2022, "ANNUAL");
    expect(result1).toEqual("January 1, 2022 - December 31, 2022");
    expect(result2).toEqual("July 1, 2022 - June 30, 2023");
  });
});

describe("printElapsedDaysMonthsYearsSinceDate", () => {
  const dayAsMilliseconds = 86400000;
  const zeroDaysLapsed = new Date(
    Date.now() - dayAsMilliseconds * 0
  ).toString();
  const oneDayLapsed = new Date(Date.now() - dayAsMilliseconds * 1).toString();
  const twoDaysLapsed = new Date(Date.now() - dayAsMilliseconds * 2).toString();
  const fifteenDaysLapsed = new Date(
    Date.now() - dayAsMilliseconds * 15
  ).toString();
  const fourtyDaysLapsed = new Date(
    Date.now() - dayAsMilliseconds * 40
  ).toString();
  const sixtyDaysLapsed = new Date(
    Date.now() - dayAsMilliseconds * 60
  ).toString();
  const hundredDaysLapsed = new Date(
    Date.now() - dayAsMilliseconds * 100
  ).toString();
  const fiveHundredDaysLapsed = new Date(
    Date.now() - dayAsMilliseconds * 500
  ).toString();
  const nineHundredDaysLapsed = new Date(
    Date.now() - dayAsMilliseconds * 900
  ).toString();

  test("0 days ago prints today", () => {
    const zeroDaysLapsedText =
      printElapsedDaysMonthsYearsSinceDate(zeroDaysLapsed);
    const nonZeroDaysLapsedText =
      printElapsedDaysMonthsYearsSinceDate(fifteenDaysLapsed);
    expect(zeroDaysLapsedText).toEqual("today");
    expect(nonZeroDaysLapsedText).not.toEqual("today");
  });

  test("1 day ago prints yesterday", () => {
    const oneDayLapsedText = printElapsedDaysMonthsYearsSinceDate(oneDayLapsed);
    expect(oneDayLapsedText).toEqual("yesterday");
  });

  test("less than 31 days ago prints number of days lapsed", () => {
    const twoDaysLapsedText =
      printElapsedDaysMonthsYearsSinceDate(twoDaysLapsed);
    const fifteenDaysLapsedText =
      printElapsedDaysMonthsYearsSinceDate(fifteenDaysLapsed);
    expect(twoDaysLapsedText).toEqual("2 days ago");
    expect(fifteenDaysLapsedText).toEqual("15 days ago");
  });

  test("more than 30 days prints number of months lapsed", () => {
    const fourtyDaysLapsedText =
      printElapsedDaysMonthsYearsSinceDate(fourtyDaysLapsed);
    const sixtyDaysLapsedText =
      printElapsedDaysMonthsYearsSinceDate(sixtyDaysLapsed);
    const hundredDaysLapsedText =
      printElapsedDaysMonthsYearsSinceDate(hundredDaysLapsed);
    expect(fourtyDaysLapsedText).toEqual("a month ago");
    expect(sixtyDaysLapsedText).toEqual("2 months ago");
    expect(hundredDaysLapsedText).toEqual("3 months ago");
  });

  test("more than 365 days prints number of years lapsed", () => {
    const fiveHundredDaysLapsedText = printElapsedDaysMonthsYearsSinceDate(
      fiveHundredDaysLapsed
    );
    const nineHundredDaysLapsedText = printElapsedDaysMonthsYearsSinceDate(
      nineHundredDaysLapsed
    );
    expect(fiveHundredDaysLapsedText).toEqual("a year ago");
    expect(nineHundredDaysLapsedText).toEqual("2 years ago");
  });
});
