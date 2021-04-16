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
import { rem } from "polished";
import styled from "styled-components/macro";
import { palette, spacing } from "@recidiviz/design-system";

const StateStyles = {
  entering: `opacity: 1;`,
  exiting: `opacity: 0;`,
};

export type TooltipState = "entering" | "exiting" | null;

const TooltipElement = styled.div<{
  state: TooltipState;
}>`
  display: block;
  position: fixed;
  font-size: ${rem("14px")};
  padding: ${rem(spacing.sm)};
  border-radius: 4px;
  color: ${palette.white};
  background-color: ${palette.signal.tooltip};
  pointer-events: none;
  opacity: 0;
  transition: opacity 0.3s ease-in-out;

  ${(props) => props.state && StateStyles[props.state]}
`;

export default TooltipElement;
