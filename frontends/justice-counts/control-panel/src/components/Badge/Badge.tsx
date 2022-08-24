// Recidiviz - a data platform for criminal justice reform
// Copyright (C) 2022 Recidiviz, Inc.
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
import React from "react";
import styled from "styled-components/macro";

import { palette } from "../GlobalStyles";
import { MiniLoader } from "../Loading/MiniLoader";

export type BadgeColors = "RED" | "GREEN" | "ORANGE" | "GREY";

export type BadgeColorMapping = { [key: string]: BadgeColors };

export type BadgeProps = {
  color: BadgeColors;
  disabled?: boolean;
  loading?: boolean;
};

export const BadgeElement = styled.div<{
  color?: BadgeColors;
  disabled?: boolean;
}>`
  height: 24px;
  display: flex;
  justify-content: center;
  align-items: center;
  background: ${({ color, disabled }) => {
    if (color === "GREY" || disabled) {
      return palette.highlight.grey9;
    }
    if (color === "RED") {
      return palette.solid.red;
    }
    if (color === "GREEN") {
      return palette.solid.green;
    }
    if (color === "ORANGE") {
      return palette.solid.orange;
    }
    return palette.highlight.grey5;
  }};
  color: ${palette.solid.white};
  padding: 4px 8px;
  margin-left: 10px;
  font-size: 0.65rem;
  font-weight: 600;
  white-space: nowrap;
  text-transform: capitalize;
`;

export const Badge: React.FC<BadgeProps> = ({
  color,
  disabled,
  loading,
  children,
}) => {
  return (
    <BadgeElement color={color} disabled={disabled}>
      {children}
      {loading && <MiniLoader />}
    </BadgeElement>
  );
};
