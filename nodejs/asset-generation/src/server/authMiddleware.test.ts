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

import express, { Express, Request, Response } from "express";
import { OAuth2Client } from "google-auth-library";
import supertest from "supertest";
import { afterEach, beforeEach, Mock, test, vi } from "vitest";

import { isTestMode } from "../utils";
import { authMiddleware } from "./authMiddleware";
import { HttpError } from "./errors";

vi.mock("../utils");
vi.mock("google-auth-library");

const AUTH_ENDPOINT = "/test-auth";
const CLOUD_RUN_URL = "test-service.run.app";

let testApp: Express;

beforeEach(() => {
  (isTestMode as Mock).mockReturnValue(false);
  testApp = express();
  testApp.locals.cloudRunUrl = CLOUD_RUN_URL;
  testApp.post(
    AUTH_ENDPOINT,
    authMiddleware,
    async (_req: Request, resp: Response) => {
      resp.sendStatus(200);
    }
  );
});

afterEach(() => {
  vi.restoreAllMocks();
});

test("missing auth", async () => {
  await supertest(testApp).post(AUTH_ENDPOINT).expect(HttpError.UNAUTHORIZED);
});

test("misformatted auth", async () => {
  await supertest(testApp)
    .post(AUTH_ENDPOINT)
    .set("Authorization", "notabearertoken")
    .expect(HttpError.UNAUTHORIZED);
});

test("unverified", async () => {
  (OAuth2Client as unknown as Mock).mockReturnValue({
    verifyIdToken: async () => {
      throw new Error("failed verifying token");
    },
  });

  await supertest(testApp)
    .post(AUTH_ENDPOINT)
    .set("Authorization", "Bearer xyz")
    .expect(HttpError.UNAUTHORIZED);
});

test("unverified email", async () => {
  (OAuth2Client as unknown as Mock).mockReturnValue({
    verifyIdToken: async () => {
      return {
        getPayload: () => {
          return {
            email_verified: false,
          };
        },
      };
    },
  });

  await supertest(testApp)
    .post(AUTH_ENDPOINT)
    .set("Authorization", "Bearer xyz")
    .expect(HttpError.FORBIDDEN);
});

test("unauthorized email", async () => {
  (OAuth2Client as unknown as Mock).mockReturnValue({
    verifyIdToken: async () => {
      return {
        getPayload: () => {
          return {
            email_verified: true,
            hd: "example.com",
          };
        },
      };
    },
  });

  await supertest(testApp)
    .post(AUTH_ENDPOINT)
    .set("Authorization", "Bearer xyz")
    .expect(HttpError.FORBIDDEN);
});

test("no audience", async () => {
  const gaeServiceAcct = "my-service-account@example.com";
  vi.stubEnv("GAE_SERVICE_ACCOUNT", gaeServiceAcct);

  (OAuth2Client as unknown as Mock).mockReturnValue({
    verifyIdToken: async () => {
      return {
        getPayload: () => {
          return {
            email_verified: true,
            email: gaeServiceAcct,
          };
        },
      };
    },
  });

  await supertest(testApp)
    .post(AUTH_ENDPOINT)
    .set("Authorization", "Bearer xyz")
    .expect(HttpError.FORBIDDEN);
});

test("successful service account auth", async () => {
  const gaeServiceAcct = "my-service-account@example.com";
  vi.stubEnv("GAE_SERVICE_ACCOUNT", gaeServiceAcct);

  (OAuth2Client as unknown as Mock).mockReturnValue({
    verifyIdToken: async () => {
      return {
        getPayload: () => {
          return {
            email_verified: true,
            email: gaeServiceAcct,
            aud: CLOUD_RUN_URL,
          };
        },
      };
    },
  });

  await supertest(testApp)
    .post(AUTH_ENDPOINT)
    .set("Authorization", "Bearer xyz")
    .expect(200);
});

test("successful user auth", async () => {
  const gaeServiceAcct = "my-service-account@example.com";
  vi.stubEnv("GAE_SERVICE_ACCOUNT", gaeServiceAcct);

  (OAuth2Client as unknown as Mock).mockReturnValue({
    verifyIdToken: async () => {
      return {
        getPayload: () => {
          return {
            email_verified: true,
            hd: "recidiviz.org",
            email: "test-user@recidiviz.org",
          };
        },
      };
    },
  });

  await supertest(testApp)
    .post(AUTH_ENDPOINT)
    .set("Authorization", "Bearer xyz")
    .expect(200);
});
