// Recidiviz - a data platform for criminal justice reform
// Copyright (C) 2024 Recidiviz, Inc.
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

import { message } from "antd";

export function getValueIfResolved<Value>(
  result: PromiseSettledResult<Value>
): Value | undefined {
  return result.status === "fulfilled" ? result.value : undefined;
}

export const runAndCheckStatus = async (
  fn: () => Promise<Response>
): Promise<boolean> => {
  const r = await fn();
  if (r.status >= 400) {
    const text = await r.text();
    message.error(`Error: ${text}`);
    return false;
  }
  return true;
};

export const cancelReimportLockDescription =
  "Acquiring locks for canceling a secondary reimport from the admin panel";
export const flashSecondaryLockDescription =
  "Acquiring locks to flash SECONDARY to PRIMARY from the admin panel";
export const flashingLockSecondsTtl = 60 * 60 * 24; // 24 hours
