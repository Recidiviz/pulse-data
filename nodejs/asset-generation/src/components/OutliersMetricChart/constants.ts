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

import { palette, spacing } from "@recidiviz/design-system";
import { lighten } from "polished";

export const DOT_RADIUS = 6;

export const X_AXIS_HEIGHT = 22;

export const MARGIN = {
  top: spacing.xxs,
  right: DOT_RADIUS,
  bottom: 0,
  left: spacing.xxs,
};

export const AXIS_OFFSET = 165;

export const ROWS_OFFSET = MARGIN.top + X_AXIS_HEIGHT;

export const ROW_LABEL_WIDTH = AXIS_OFFSET - MARGIN.left - DOT_RADIUS;

export const TICK_WIDTH = 2;

export const ROW_HEIGHT = 48;

export const MARK_STROKE_WIDTH = 2;

export const GOAL_COLORS = {
  far: palette.data.crimson1,
  near: palette.data.gold1,
  met: lighten(0.45)(palette.slate),
};
