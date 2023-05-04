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

import { isDevMode } from "../../utils";
import { LOCAL_FILE_DIR } from "../constants";

async function readLocalFile(filename: string) {
  const filepath = join(LOCAL_FILE_DIR, filename);
  return fsReadFile(filepath);
}

export async function readFile(filename: string) {
  if (isDevMode()) {
    return readLocalFile(filename);
  }
  throw new Error("not implemented");
}
