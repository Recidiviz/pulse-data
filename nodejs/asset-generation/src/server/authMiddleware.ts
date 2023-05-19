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

import { NextFunction, Request, Response } from "express";
import { OAuth2Client } from "google-auth-library";

import { isDevMode, isTestMode } from "../utils";
import { HttpError } from "./errors";

async function checkAuth(req: Request) {
  if (isDevMode() || isTestMode()) {
    return;
  }
  const authHeader = req.get("Authorization");
  if (!authHeader) {
    throw new HttpError(HttpError.UNAUTHORIZED, "Auth header is empty");
  }

  const split = authHeader.split(" ");
  if (split.length !== 2) {
    throw new HttpError(HttpError.UNAUTHORIZED, "Auth header is misformatted");
  }
  const token = split[1];

  let verifyResult;
  try {
    // Make sure Google signed the token
    verifyResult = await new OAuth2Client().verifyIdToken({
      idToken: token,
    });
  } catch (e) {
    throw new HttpError(
      HttpError.UNAUTHORIZED,
      `Auth header was not verified: ${e}`
    );
  }
  const payload = verifyResult.getPayload();
  if (!payload?.email_verified) {
    throw new HttpError(HttpError.FORBIDDEN, "Email not verified");
  }
  if (
    payload?.email !== process.env.GAE_SERVICE_ACCOUNT &&
    // hd represents the G Suite domain of the user
    payload?.hd !== "recidiviz.org"
  ) {
    throw new HttpError(
      HttpError.FORBIDDEN,
      `Unauthorized email ${payload?.email}`
    );
  }
  // If the JWT came from the GAE service account, we also want to check that it's a JWT that was
  // created to talk specifically to this API. Allow recidiviz users to make calls directly:
  // gcloud doesn't allow you to set an audience in these ID tokens.
  const { cloudRunUrl } = req.app.locals;
  if (
    payload?.email === process.env.GAE_SERVICE_ACCOUNT &&
    payload?.aud !== cloudRunUrl
  ) {
    throw new HttpError(
      HttpError.FORBIDDEN,
      "Payload audience != expected audience"
    );
  }
}

export async function authMiddleware(
  req: Request,
  res: Response,
  next: NextFunction
) {
  try {
    await checkAuth(req);
  } catch (e) {
    if (e instanceof HttpError) {
      // eslint-disable-next-line no-console
      console.error(`auth error: ${e.code} ${e.message}`);
      res.sendStatus(e.code);
      return;
    }
    // eslint-disable-next-line no-console
    console.error(`auth error: ${e}`);
    res.sendStatus(HttpError.INTERNAL_SERVER_ERROR);
    return;
  }
  next();
}
