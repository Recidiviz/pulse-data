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
  const { width, entityLabel, id, data } = res.locals.data;

  const { svg, height } = getRenderedChartSvg({
    width,
    entityLabel,
    data,
  });
  const img = await convertToImage(svg);
  const fileUrl = `outliers-metric-chart/${id}-${Date.now()}.png`;
  await writeFile(fileUrl, img);
  res.json({
    url: `${RETRIEVE_PATH}/${getAssetToken(fileUrl)}`,
    height,
  });
};
