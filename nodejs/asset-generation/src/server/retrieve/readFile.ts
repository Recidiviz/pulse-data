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
import { join } from "path";

import { isDevMode, isProductionMode, isTestMode } from "../../utils";
import { LOCAL_FILE_DIR } from "../constants";
import { HttpError } from "../errors";
import { gcsClient } from "../gcp";

async function readLocalFile(filename: string) {
  const filepath = join(LOCAL_FILE_DIR, filename);
  return fsReadFile(filepath);
}

async function readGcsFile(filename: string) {
  const bucket = process.env.GCS_ASSET_BUCKET_NAME;
  if (!bucket) {
    throw new HttpError(
      HttpError.INTERNAL_SERVER_ERROR,
      "Missing GCS_ASSET_BUCKET_NAME environment variable"
    );
  }
  const [file] = await gcsClient.bucket(bucket).file(filename).download();
  return file;
}

export async function readFile(filename: string) {
  if (isDevMode() || isTestMode()) {
    return readLocalFile(filename);
  }
  if (isProductionMode()) {
    return readGcsFile(filename);
  }
  throw new Error("unsupported mode");
}
