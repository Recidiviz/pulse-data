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

import { getAssetToken } from "./token";

/**
 * generates an asset token using a dummy URL
 */
export function getTestToken(url = "test/path/to/image") {
  return getAssetToken(url);
}

/**
 * generates an asset token using a dummy URL
 * and then injects a different dummy URL into the payload
 */
export function getTamperedTestToken() {
  const token = getTestToken();
  // inject a different URL this token
  const payload = jwt.decode(token) as jwt.JwtPayload;
  payload.sub = "test/path/to/another/image";
  const doctoredPayload = Buffer.from(JSON.stringify(payload)).toString(
    "base64url"
  );
  const tokenParts = token.split(".");
  return `${tokenParts[0]}.${doctoredPayload}.${tokenParts[2]}`;
}

/**
 * this is a 1x1 transparent png
 */
export const mockImageData = Buffer.from(
  "iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAQAAAC1HAwCAAAAC0lEQVR42mNkYAAAAAYAAjCB0C8AAAAASUVORK5CYII=",
  "base64"
);
