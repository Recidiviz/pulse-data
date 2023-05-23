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

import { mkdir, writeFile as fsWriteFile } from "fs/promises";
import { dirname, join } from "path";

import { isDevMode, isProductionMode, isTestMode } from "../../utils";
import { LOCAL_FILE_DIR } from "../constants";
import { gcsClient } from "../gcp";

type WriteFunction = (filename: string, filedata: Buffer) => Promise<void>;

const writeLocalFile: WriteFunction = async (filename, filedata) => {
  const filepath = join(LOCAL_FILE_DIR, filename);
  await mkdir(dirname(filepath), { recursive: true });
  await fsWriteFile(filepath, filedata);
};

const writeToGcs: WriteFunction = async (filename, filedata) => {
  const bucket = process.env.GCS_ASSET_BUCKET_NAME;
  if (!bucket) {
    throw new Error("Missing GCS_ASSET_BUCKET_NAME environment variable");
  }
  await gcsClient.bucket(bucket).file(filename).save(filedata);
};

export const writeFile: WriteFunction = async (...args) => {
  if (isDevMode() || isTestMode()) {
    await writeLocalFile(...args);
  } else if (isProductionMode()) {
    await writeToGcs(...args);
  } else {
    throw new Error("unsupported mode");
  }
};
