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
 * @example '1 day ago', '0 days ago', '365 days ago'
 */
export const printElapsedDaysSinceDate = (date: string): string => {
  const now = +new Date(Date.now());
  const stringDateToNumber = +new Date(date);
  const daysLapsed = Math.floor(
    (now - stringDateToNumber) / (1000 * 60 * 60 * 24)
  );

  return `${daysLapsed} day${daysLapsed !== 1 ? "s" : ""} ago`;
};
