// Recidiviz - a data platform for criminal justice reform
// Copyright (C) 2021 Recidiviz, Inc.
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
import styled from "styled-components/macro";
import { rem } from "polished";
import { Button, palette, spacing } from "@recidiviz/design-system";
import { animated } from "@react-spring/web";

export const CarouselItem = styled(animated.div)`
  position: absolute;
  // Absolutely positioned elements are positioned relative to the parent's padding box, not content box
  // Inherit any padding from the parent to maintain equal content box widths.
  padding: inherit;
  left: 0;
  top: 0;
  display: flex;
  will-change: transform, opacity;
  touch-action: none;
`;

export const CarouselContainer = styled.div`
  position: absolute;
  padding: inherit;
  touch-action: none;
  width: 100%;
  height: 100%;
  left: 0;
`;

export const CarouselControl = styled(Button).attrs({ kind: "secondary" })`
  border: 1px solid ${palette.slate30};
  border-radius: ${rem(72)};
  padding: 0;
  height: ${rem(72)};
  min-width: ${rem(72)};
  max-width: ${rem(72)};
  margin-right: ${rem(spacing.sm)};
`;

export const CarouselControlsContainer = styled.div`
  display: flex;
`;

export interface CarouselIndicatorProps {
  on?: boolean;
}

export const CarouselIndicatorOff = styled.span<CarouselIndicatorProps>`
  border-radius: 50%;
  height: ${rem(spacing.sm)};
  width: ${rem(spacing.sm)};
  margin-right: ${rem(spacing.sm)};
  border: 1px solid ${palette.pine4};
`;

export const CarouselIndicatorOn = styled(CarouselIndicatorOff)`
  background-color: ${palette.pine4};
`;
