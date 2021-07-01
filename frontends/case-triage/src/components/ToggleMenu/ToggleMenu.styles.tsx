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

import { spacing } from "@recidiviz/design-system";
import { rem } from "polished";
import styled from "styled-components/macro";

export const ToggleLabelWrapper = styled.span`
  align-items: center;
  display: inline-flex;
  justify-content: space-between;
  width: 100%;
`;

export const ToggleLabelIndicator = styled.span`
  background: currentColor;
  display: inline-block;
  height: ${rem(2)};
  margin-left: ${rem(spacing.sm)};
  width: ${rem(8)};
`;
