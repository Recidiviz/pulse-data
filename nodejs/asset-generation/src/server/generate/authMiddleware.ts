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

/* eslint-disable no-console -- these console statements are temporary for figuring out what kind of
auth token we're getting */

import { NextFunction, Request, Response } from "express";
import { OAuth2Client } from "google-auth-library";

import { isDevMode, isTestMode } from "../../utils";

async function checkAuth(req: Request) {
  if (isDevMode() || isTestMode()) {
    return;
  }
  const authHeader = req.get("Authorization");
  if (!authHeader) {
    console.log("Auth header is empty");
    return;
  }

  const split = authHeader.split(" ");
  if (split.length !== 2) {
    console.log(`Unexpected auth header ${authHeader}`);
    return;
  }

  const token = split[1];
  console.log(`Received auth token ${token}`);

  // TODO(Recidiviz/recidiviz-dashboards#3298): Figure out which approach we're actually using and
  // use it to configure auth checks
  const oAuth2Client = new OAuth2Client();
  try {
    // https://github.com/googleapis/google-auth-library-nodejs/blob/main/samples/verifyGoogleIdToken.js
    const verifyResult = await oAuth2Client.verifyIdToken({ idToken: token });
    console.log(`verifyIdToken success: ${JSON.stringify(verifyResult)}, `);
  } catch (e) {
    console.log(`received error in verifyIdToken: ${e}`);
  }

  try {
    // https://github.com/googleapis/google-auth-library-nodejs/blob/main/samples/verifyIdToken-iap.js
    const iapResponse = await oAuth2Client.getIapPublicKeys();
    const verifyResult = await oAuth2Client.verifySignedJwtWithCertsAsync(
      token,
      iapResponse.pubkeys
    );
    console.log(
      `verifySignedJwtWithCertsAsync success: ${JSON.stringify(verifyResult)}`
    );
  } catch (e) {
    console.log(`received error in verifySignedJwtWithCertsAsync: ${e}`);
  }
}

export async function authMiddleware(
  req: Request,
  res: Response,
  next: NextFunction
) {
  checkAuth(req);
  next();
}
