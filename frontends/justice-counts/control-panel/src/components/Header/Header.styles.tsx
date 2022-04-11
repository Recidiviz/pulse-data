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

import styled from "styled-components/macro";

type HeaderCellProps = {
  textAlign?: "right" | "left";
};

export const HeaderRow = styled.header`
  width: 100%;
  height: 50px;
  display: flex;
  justify-content: space-between;
  align-items: center;
  padding: 16px 22px;
`;

export const HeaderCell = styled.div<HeaderCellProps>`
  width: 300px;
  display: flex;
  justify-content: ${({ textAlign }) => {
    if (!textAlign) {
      // defaults as 'flex-start' if 'textAlign' is not defined.
      return "flex-start";
    }
    return textAlign === "right" ? "flex-end" : "flex-start";
  }};

  &:nth-child(1) {
    width: 400px;
  }

  &:nth-child(2) {
    width: 330px;
  }

  &:nth-child(3) {
    width: 400px;
  }

  &:nth-child(4) {
    width: 200px;
  }

  @media only screen and (max-width: 1150px) {
    // hides extra empty cell
    &:nth-child(3) {
      display: none;
    }
  }
`;
