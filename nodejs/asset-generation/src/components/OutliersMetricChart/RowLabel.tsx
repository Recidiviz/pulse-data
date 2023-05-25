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

import { palette, typography } from "@recidiviz/design-system";
import styled from "styled-components";

import { wrapText } from "../utils";
import { ROW_LABEL_WIDTH } from "./constants";

const FONT_SIZE = 18;
const Text = styled.text`
  ${typography[`Sans${FONT_SIZE}`]}
  fill: ${palette.pine1};
`;

// line-height ratio matches design system
const LINE_HEIGHT = FONT_SIZE * 1.2;

type RowLabelProps = {
  className?: string;
  children: string;
  x?: number;
  y?: number;
};

export function RowLabel({ className, children, x, y }: RowLabelProps) {
  const lines = wrapText(children, ROW_LABEL_WIDTH, FONT_SIZE);

  // we only have room for two lines max
  const linesToDisplay = lines.slice(0, 2);
  const labelTruncated = lines.length > linesToDisplay.length;

  const labelHeight = LINE_HEIGHT * linesToDisplay.length;

  // goal is for the label to be centered on y, but by default it is anchored to the text bottom;
  // we have to calculate the difference and then translate
  const yOffset = FONT_SIZE - labelHeight / 2;

  return (
    <Text
      className={className}
      {...{ x, y }}
      transform={`translate(0 ${yOffset})`}
    >
      {linesToDisplay.map((line, i) => (
        <tspan key={line} x={x} dy={LINE_HEIGHT * i}>
          {line}
          {labelTruncated && i === 1 && " …"}
        </tspan>
      ))}
    </Text>
  );
}
