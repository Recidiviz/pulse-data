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

import { Request, Response } from "express";
import fs from "fs/promises";
import { kebabCase } from "lodash";
import { join } from "path";

import { RETRIEVE_PATH } from "../../constants";
import { HttpError } from "../../errors";
import { getAssetToken } from "../../token";
import { AssetResponse, ValidatedInput } from "../types";
import { writeFile } from "../writeFile";
import { OutliersAggregatedChartInputTransformed } from "./types";

export const outliersAggregatedChartRoute = async (
  req: Request,
  res: Response<
    AssetResponse,
    ValidatedInput<OutliersAggregatedChartInputTransformed>
  >
) => {
  const { stateCode, id, aggregationType } = res.locals.data;

  const today = new Date().toISOString().split("T")[0];
  const fileUrl = `outliers-aggregated-${kebabCase(
    aggregationType
  )}-chart/${stateCode}/${today}/${id}.png`;
  try {
    // TODO(#21174): real image instead of a fixture
    const img = await fs.readFile(
      join(__dirname, "fixtures/temp-image-placeholder.png")
    );
    await writeFile(fileUrl, img);
    const token = await getAssetToken(fileUrl);
    res.json({
      url: `${RETRIEVE_PATH}/${token}`,
    });
  } catch (e) {
    // eslint-disable-next-line no-console
    console.error(e);
    res.sendStatus(HttpError.INTERNAL_SERVER_ERROR);
  }
};
