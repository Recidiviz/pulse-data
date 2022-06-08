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

import { ReportFrequency } from "../shared/types";

export const monthsByName = [
  "January",
  "February",
  "March",
  "April",
  "May",
  "June",
  "July",
  "August",
  "September",
  "October",
  "November",
  "December",
];

/**
 * @returns the month and year as a string
 * @example "March 2022"
 */
export const printDateAsMonthYear = (month: number, year: number): string => {
  return new Intl.DateTimeFormat("en-US", {
    month: "long",
    year: "numeric",
  }).format(Date.UTC(year, month, -15));
};

/**
 * @returns either "Annual Report [YEAR]" or "[MONTH] [YEAR]" as a string depending on frequency
 * @example "Annual Report 2022" or "March 2022"
 */
export const printReportTitle = (
  month: number,
  year: number,
  frequency: ReportFrequency
): string => {
  if (frequency === "ANNUAL") {
    return `Annual Report ${year}`;
  }

  return printDateAsMonthYear(month, year);
};

/**
 * @returns elapsed number of days since a provided date as a string
 * @example 'today', 'yesterday', '2 days ago', '3 months ago', '5 years ago'
 */
export const printElapsedDaysMonthsYearsSinceDate = (date: string): string => {
  const now = +new Date(Date.now());
  const stringDateToNumber = +new Date(date);
  const daysLapsed = Math.floor(
    (now - stringDateToNumber) / (1000 * 60 * 60 * 24)
  );

  if (daysLapsed === 0) {
    return `today`;
  }

  if (daysLapsed === 1) {
    return `yesterday`;
  }

  if (daysLapsed < 31) {
    return `${daysLapsed !== 1 ? daysLapsed : "a"} day${
      daysLapsed !== 1 ? "s" : ""
    } ago`;
  }

  if (daysLapsed > 30 && daysLapsed < 365) {
    const monthsLapsed = Math.floor(daysLapsed / 30);
    return `${monthsLapsed !== 1 ? monthsLapsed : "a"} month${
      monthsLapsed !== 1 ? "s" : ""
    } ago`;
  }

  if (daysLapsed >= 365) {
    const yearsLapsed = Math.floor(daysLapsed / 365);
    return `${yearsLapsed !== 1 ? yearsLapsed : "a"} year${
      yearsLapsed !== 1 ? "s" : ""
    } ago`;
  }

  return "";
};

/**
 * Prints a human-readable date range of the provided month based on month and year
 * @returns date range of the month as a string
 * @example printDateRangeFromMonthYear(12, 2022) returns 'December 1, 2022 - December 31, 2022'
 */
export const printDateRangeFromMonthYear = (
  month: number,
  year: number,
  frequency: ReportFrequency = "MONTHLY"
): string => {
  /**
   * Note: backend sends true month number, whereas JavaScript's Date API deals with zero-indexed month numbers
   * The below method of calculating the last day (number) of a given month relies on getting the 0th day of the following month.
   * Simply providing the true month number value (from `month` param) does the + 1 (following month) calculation for us.
   */

  if (frequency === "MONTHLY") {
    const lastDayOfMonth = new Date(year, month, 0)?.getDate();
    const currentMonth = monthsByName[month - 1];
    return `${currentMonth} 1, ${year} - ${currentMonth} ${lastDayOfMonth}, ${year}`;
  }

  const currentMonth = monthsByName[month - 1];
  const prevMonthNumber = month === 1 ? 12 : month - 1;
  const prevMonth = monthsByName[prevMonthNumber - 1];
  const lastDayOfPrevMonth = new Date(year, prevMonthNumber, 0)?.getDate();
  return `${currentMonth} 1, ${year} - ${prevMonth} ${lastDayOfPrevMonth}, ${
    month === 1 ? year : year + 1
  }`;
};
