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

import { spacing } from "@recidiviz/design-system";

export const HIGHLIGHT_DOT_RADIUS = 6;
export const SWARM_DOT_RADIUS = 3;
export const TICK_WIDTH = 2;

export const X_AXIS_HEIGHT = 22;
export const ROW_HEIGHT = 42;

export const MARGIN = {
  top: spacing.xxs,
  right: HIGHLIGHT_DOT_RADIUS + spacing.xxs,
  bottom: SWARM_DOT_RADIUS + spacing.xxs,
  left: HIGHLIGHT_DOT_RADIUS + spacing.xxs,
};

export const CONTENT_AREA_TOP_OFFSET = MARGIN.top + X_AXIS_HEIGHT;

export const LABEL_X_BASE = HIGHLIGHT_DOT_RADIUS + spacing.sm;
