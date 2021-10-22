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
import { Button, fonts, palette, spacing } from "@recidiviz/design-system";
import { rem } from "polished";
import styled from "styled-components/macro";

export const UserFlex = styled.div`
  display: flex;
  align-items: center;

  position: relative;

  text-align: right;
  letter-spacing: -0.01em;
  font-feature-settings: "ss04" on;
  font-family: ${fonts.body};
  color: ${palette.text.normal};
`;

export const UserName = styled.span`
  font-size: ${rem(15)};
  margin-right: ${rem(spacing.md)};
  cursor: pointer;
`;

export const UserAvatar = styled.span`
  background: ${palette.signal.notification};
  border-radius: 50%;
  color: white;
  display: inline-block;
  font-size: ${rem(11)};
  line-height: 32px;
  height: 32px;
  text-align: center;
  text-transform: uppercase;
  width: 32px;
  cursor: pointer;
`;

export const ToolbarButton = styled(Button)`
  margin: 0 ${rem(spacing.md)};
`;
