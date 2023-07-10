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

import sharp from "sharp";

/**
 * @param source assumed to be a string of SVG markup.
 * SVG dimensions will be doubled to support high-resolution screens.
 * @returns a Buffer containing PNG data
 */
export async function convertSvgToPng(source: string) {
  const baseImage = sharp(Buffer.from(source));

  const baseWidth = (await baseImage.metadata()).width;
  const outputWidth = baseWidth ? baseWidth * 2 : baseWidth;
  return baseImage.resize({ width: outputWidth }).png().toBuffer();
}
