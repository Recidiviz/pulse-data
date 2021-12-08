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

import { PresetStatusColorType } from "antd/lib/_util/colors";
import { ValidationStatusRecord } from "../../recidiviz/admin_panel/models/validation_pb";
import { RecordStatus } from "./constants";

export const replaceInfinity = <T>(x: number, replacement: T): number | T => {
  return Number.isFinite(x) ? x : replacement;
};

export const daysBetweenDates = (date1: Date, date2: Date): number => {
  const delta = Math.abs(date1.getTime() - date2.getTime());
  return Math.ceil(delta / (1000 * 3600 * 24));
};

export const getDaysActive = (record: ValidationStatusRecord): number => {
  const runDate = record.getRunDatetime()?.toDate();
  const lastSuccessDate = record.getLastBetterStatusRunDatetime()?.toDate();
  if (runDate === undefined) {
    return 0;
  }
  if (lastSuccessDate === undefined) {
    return Infinity;
  }
  return daysBetweenDates(runDate, lastSuccessDate);
};

export const formatDatetime = (date?: Date): string | undefined => {
  return date?.toLocaleString("en-US", { timeZoneName: "short" });
};

export const formatDate = (date?: Date): string | undefined => {
  return date?.toLocaleDateString();
};

export const formatStatusAmount = (
  amount: number | null | undefined,
  isPercent: boolean | undefined
): string => {
  if (amount === null || amount === undefined) {
    return "";
  }
  if (isPercent) {
    return `${(amount * 100).toFixed(2)}%`;
  }
  return amount.toString();
};

export const getRecordStatus = (
  record: ValidationStatusRecord
): RecordStatus => {
  if (record === undefined) {
    return RecordStatus.NO_RESULT;
  }
  if (record.getDidRun() === false) {
    return RecordStatus.BROKEN;
  }
  if (record.getHasData() === false) {
    return RecordStatus.NEED_DATA;
  }

  return convertResultStatus(record.getResultStatus());
};

export const convertResultStatus = (resultStatus?: number): RecordStatus => {
  switch (resultStatus) {
    case ValidationStatusRecord.ValidationResultStatus.FAIL_HARD:
      return RecordStatus.FAIL_HARD;
    case ValidationStatusRecord.ValidationResultStatus.FAIL_SOFT:
      return RecordStatus.FAIL_SOFT;
    case ValidationStatusRecord.ValidationResultStatus.SUCCESS:
      return RecordStatus.SUCCESS;
    default:
      return RecordStatus.UNKNOWN;
  }
};

export const getClassNameForRecordStatus = (
  status: RecordStatus
): string | undefined => {
  switch (status) {
    case RecordStatus.NO_RESULT:
      return undefined;
    case RecordStatus.BROKEN:
      return "broken";
    case RecordStatus.NEED_DATA:
      return undefined;
    case RecordStatus.FAIL_HARD:
      return "failed-hard";
    case RecordStatus.FAIL_SOFT:
      return "failed-soft";
    case RecordStatus.SUCCESS:
      return "success";
    case RecordStatus.UNKNOWN:
      return "broken";
    default:
      return undefined;
  }
};

export const getBadgeStatusForRecordStatus = (
  status: RecordStatus
): PresetStatusColorType => {
  switch (status) {
    case RecordStatus.NO_RESULT:
      return "default";
    case RecordStatus.BROKEN:
      return "error";
    case RecordStatus.NEED_DATA:
      return "default";
    case RecordStatus.FAIL_HARD:
      return "error";
    case RecordStatus.FAIL_SOFT:
      return "warning";
    case RecordStatus.SUCCESS:
      return "success";
    case RecordStatus.UNKNOWN:
      return "error";
    default:
      return "default";
  }
};

export const getTextForRecordStatus = (status: RecordStatus): string => {
  switch (status) {
    case RecordStatus.NO_RESULT:
      return "No Result";
    case RecordStatus.BROKEN:
      return "Broken";
    case RecordStatus.NEED_DATA:
      return "Need Data";
    case RecordStatus.FAIL_HARD:
      return "Hard Fail";
    case RecordStatus.FAIL_SOFT:
      return "Soft Fail";
    case RecordStatus.SUCCESS:
      return "Passed";
    case RecordStatus.UNKNOWN:
    default:
      return "Unknown Status Result";
  }
};

export const readableNameForCategoryId = (category: string): string => {
  switch (category) {
    case "CONSISTENCY":
      return "Consistency";
    case "EXTERNAL_AGGREGATE":
      return "External Aggregate";
    case "EXTERNAL_INDIVIDUAL":
      return "External Individual";
    case "INVARIANT":
      return "Invariant";
    case "FRESHNESS":
      return "Freshness";
    case "UNKNOWN":
      return "Unknown Category";
    default:
      return category;
  }
};

export const chooseIdNameForCategory = (
  category?: ValidationStatusRecord.ValidationCategoryMap[keyof ValidationStatusRecord.ValidationCategoryMap]
): string => {
  switch (category) {
    case ValidationStatusRecord.ValidationCategory.CONSISTENCY:
      return "CONSISTENCY";
    case ValidationStatusRecord.ValidationCategory.EXTERNAL_AGGREGATE:
      return "EXTERNAL_AGGREGATE";
    case ValidationStatusRecord.ValidationCategory.EXTERNAL_INDIVIDUAL:
      return "EXTERNAL_INDIVIDUAL";
    case ValidationStatusRecord.ValidationCategory.INVARIANT:
      return "INVARIANT";
    case ValidationStatusRecord.ValidationCategory.FRESHNESS:
      return "FRESHNESS";
    default:
      return "UNKNOWN";
  }
};
