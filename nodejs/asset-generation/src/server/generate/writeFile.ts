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

const mockLocalDir = join(__dirname, "../../../local/gcs/");

type WriteFunction = (filename: string, filedata: Buffer) => Promise<void>;

const writeLocalFile: WriteFunction = async (filename, filedata) => {
  const filepath = join(mockLocalDir, filename);
  await mkdir(dirname(filepath), { recursive: true });
  await fsWriteFile(filepath, filedata);
};

export const writeFile: WriteFunction = async (...args) => {
  // TODO(Recidiviz/recidiviz-dashboards#3298): support write to GCS
  await writeLocalFile(...args);
};
