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

import { getRenderedChartSvg } from "../../../components/OutliersMetricChart/OutliersMetricChart";
import { RETRIEVE_PATH } from "../../constants";
import { HttpError } from "../../errors";
import { getAssetToken } from "../../token";
import { convertToImage } from "../convertToImage";
import { AssetResponse, ValidatedInput } from "../types";
import { writeFile } from "../writeFile";
import { OutliersMetricChartInput } from "./types";

export const outliersMetricChartRoute = async (
  req: Request,
  res: Response<
    AssetResponse & { height: number },
    ValidatedInput<OutliersMetricChartInput>
  >
) => {
  const { width, entityLabel, stateCode, id, data } = res.locals.data;

  const { svg, height } = getRenderedChartSvg({
    width,
    entityLabel,
    data,
  });
  const img = await convertToImage(svg);
  const today = new Date().toISOString().split("T")[0];
  const fileUrl = `outliers-metric-chart/${stateCode}/${today}/${id}.png`;
  try {
    await writeFile(fileUrl, img);
    const token = await getAssetToken(fileUrl);
    res.json({
      url: `${RETRIEVE_PATH}/${token}`,
      height,
    });
  } catch (e) {
    // eslint-disable-next-line no-console
    console.error(e);
    res.sendStatus(HttpError.INTERNAL_SERVER_ERROR);
  }
};
