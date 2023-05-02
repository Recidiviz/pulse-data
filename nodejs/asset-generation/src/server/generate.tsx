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

import { NextFunction, Request, Response } from "express";
import { writeFile } from "fs";

import { officerData } from "../components/OutliersMetricChart/fixtures";
import { OutliersMetricChart } from "../components/OutliersMetricChart/OutliersMetricChart";
import { renderToStaticSvg } from "../components/utils";
import { convertToImage } from "./convertToImage";

function TestCmp() {
  return (
    <OutliersMetricChart
      data={officerData}
      width={570}
      entityLabel="Officers"
    />
  );
}

export async function generate(
  _req: Request,
  resp: Response,
  next: NextFunction
) {
  const svg = renderToStaticSvg(TestCmp);
  const img = await convertToImage(svg);
  writeFile("tmp.png", img, next);
  resp.sendStatus(200);
}
