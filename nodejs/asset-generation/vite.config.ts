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

// this lets us add a `test` property to the config object for vitest
/// <reference types="vitest" />

import react from "@vitejs/plugin-react-swc";
import path from "path";
import { defineConfig } from "vite";

// https://vitejs.dev/config/
export default defineConfig({
  plugins: [react()],
  resolve: {
    alias: {
      // https://github.com/vitest-dev/vitest/issues/2794
      clsx: path.resolve(__dirname, "./node_modules/clsx/dist/clsx.js"),
    },
  },
  test: {
    globalSetup: ["./globalTestSetup.ts"],
    setupFiles: ["./testSetup.ts"],
    mockReset: true,
    // Vitest resolves packages like Node does, which picks the design system's
    // CJS entry; that entry requires .css files Node cannot load. Point tests at
    // the ES build instead and let Vite transform it.
    alias: {
      "@recidiviz/design-system": path.resolve(
        __dirname,
        "./node_modules/@recidiviz/design-system/dist/index.es.js"
      ),
    },
    server: {
      deps: {
        inline: ["@recidiviz/design-system"],
      },
    },
  },
});
