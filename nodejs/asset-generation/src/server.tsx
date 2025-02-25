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

import express from "express";
import rateLimit from "express-rate-limit";

import { RETRIEVE_PATH } from "./server/constants";
import { getCloudRunUrl } from "./server/gcp";
import { routes as generateRoutes } from "./server/generate";
import { routes as retrieveRoutes } from "./server/retrieve";

async function createServer() {
  const app = express();
  const port = process.env.PORT || 5174; // default vite port + 1

  app.locals.cloudRunUrl = await getCloudRunUrl(port);

  const generateLimiter = rateLimit({
    windowMs: 1000, // 1 second = 1000ms
    max: 30, // each IP address gets 30 requests per 1 second
    standardHeaders: true, // return rate limit info in the `RateLimit-*` headers
    legacyHeaders: false, // disabling the `X-RateLimit-*` headers
  });
  app.use("/generate", generateRoutes);
  app.use("/generate", generateLimiter);

  const retrieveLimiter = rateLimit({
    windowMs: 1000, // 1 second = 1000ms
    max: 300, // each IP address gets X requests per 1 second
    standardHeaders: true, // return rate limit info in the `RateLimit-*` headers
    legacyHeaders: false, // disabling the `X-RateLimit-*` headers
  });
  app.use(RETRIEVE_PATH, retrieveRoutes);
  app.use(RETRIEVE_PATH, retrieveLimiter);

  const server = app.listen(port, () => {
    // eslint-disable-next-line no-console
    console.log(
      `Server in ${import.meta.env.MODE} mode, listening on port ${port}`
    );
  });

  // https://github.com/vitest-dev/vitest/issues/2334
  if (import.meta.hot) {
    import.meta.hot.on("vite:beforeFullReload", () => {
      server.close();
    });
  }
}

createServer();
