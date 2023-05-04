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

import { Router } from "express";
import { fileTypeFromBuffer } from "file-type";

import { getUrlFromAssetToken } from "../token";
import { readFile } from "./readFile";

export const routes = Router();

routes.get("/:token", async (req, res) => {
  try {
    const url = getUrlFromAssetToken(req.params.token);
    const file = await readFile(url);

    const { mime } = (await fileTypeFromBuffer(file)) ?? {};
    if (!mime) throw new Error("unknown file type");

    res.type(mime);
    res.send(file);
  } catch (e) {
    // eslint-disable-next-line no-console
    console.error(e);
    res.sendStatus(404);
  }
});
