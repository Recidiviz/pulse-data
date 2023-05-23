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

import { readFile as fsReadFile } from "fs/promises";
import { MockStorage } from "mock-gcs";
import { join } from "path";
import { expect, Mock, test, vi } from "vitest";

import { isDevMode, isProductionMode } from "../../utils";
import { LOCAL_FILE_DIR } from "../constants";
import { gcsClient } from "../gcp";
import { readFile } from "./readFile";

vi.mock("../../utils");
vi.mock("fs/promises");
vi.mock("../gcp", async () => {
  const mod = await vi.importActual<typeof import("../gcp")>("../gcp");
  return {
    ...mod,
    gcsClient: new MockStorage(),
  };
});

test("local dev", async () => {
  (isDevMode as Mock).mockReturnValue(true);

  const fn = "/path/to/file";
  await readFile(fn);

  expect(fsReadFile).toHaveBeenCalledWith(join(LOCAL_FILE_DIR, fn));
});

test("unsupported mode", async () => {
  expect(async () => readFile("whatever")).rejects.toThrow();
});

test("production", async () => {
  (isProductionMode as Mock).mockReturnValue(true);
  const bucket = "test";
  vi.stubEnv("GCS_ASSET_BUCKET_NAME", bucket);
  const fn = "/path/to/file";
  const contents = "contents";
  gcsClient.bucket(bucket).file(fn).save(contents);
  expect(await readFile(fn)).toEqual(Buffer.from(contents));
});
