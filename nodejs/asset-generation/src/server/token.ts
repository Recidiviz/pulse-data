// Recidiviz - a data platform for criminal justice reform
// Copyright (C) 2023 Recidiviz, Inc.
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

import jwt from "jsonwebtoken";

import { isDevMode, isTestMode } from "../utils";

function getSecret() {
  if (isDevMode() || isTestMode()) {
    return "not-a-real-secret";
  }
  throw new Error("not implemented");
}

export function getAssetToken(url: string) {
  return jwt.sign({ sub: url, iat: Date.now() }, getSecret());
}

export function getUrlFromAssetToken(token: string) {
  const { sub } = jwt.verify(token, getSecret());
  if (typeof sub === "string") return sub;
  throw new Error("invalid payload; object expected");
}
