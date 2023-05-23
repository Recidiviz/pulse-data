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
import { SECRET_MANAGER_KEY_NAME } from "./constants";
import { getSecret } from "./gcp";

async function getTokenSecret() {
  if (isDevMode() || isTestMode()) {
    return "not-a-real-secret";
  }
  return getSecret(SECRET_MANAGER_KEY_NAME);
}

export async function getAssetToken(url: string) {
  const secret = await getTokenSecret();
  return jwt.sign({ sub: url, iat: Date.now() }, secret);
}

export async function getUrlFromAssetToken(token: string) {
  const secret = await getTokenSecret();
  const { sub } = jwt.verify(token, secret);
  if (typeof sub === "string") return sub;
  throw new Error("invalid payload; object expected");
}
