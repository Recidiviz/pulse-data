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
import { Text } from "@visx/text";
import styled from "styled-components";

import { HIGHLIGHT_DOT_RADIUS, TICK_WIDTH } from "./constants";

export const AxisLabel = styled(Text)`
  ${typography.Sans12}
  fill: ${palette.slate60};
`;

export const SwarmLabel = styled(Text).attrs({ verticalAnchor: "middle" })`
  ${typography.Sans16}
`;

export const AxisSpine = styled.line`
  stroke: ${palette.slate30};
  stroke-width: 1px;
`;

export const TargetLine = styled.line`
  stroke: ${palette.slate60};
  stroke-width: ${TICK_WIDTH}px;
  stroke-dasharray: 4 4;
`;

const HIGHLIGHT_MARK_STROKE = { width: 2, color: palette.white };

export const RateHighlightChangeBar = styled.rect`
  height: ${HIGHLIGHT_DOT_RADIUS * 2}px;
  /* this color is not in the design system, it's a non-transparent variant of slate */
  fill: #becbd1;
  stroke: ${HIGHLIGHT_MARK_STROKE.color};
  stroke-width: ${HIGHLIGHT_MARK_STROKE.width}px;
`;

export const RateHighlightMark = styled.circle`
  stroke: ${HIGHLIGHT_MARK_STROKE.color};
  stroke-width: ${HIGHLIGHT_MARK_STROKE.width}px;
`;
