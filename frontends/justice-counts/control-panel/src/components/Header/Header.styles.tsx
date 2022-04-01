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

export const HeaderWrapper = styled.header`
  width: 100%;
  height: 50px;
  display: flex;
  justify-content: space-between;
  align-items: center;
  padding: 16px 22px;
`;

type HeaderItemProps = {
  textAlign?: "right" | "left";
};

export const HeaderItem = styled.div<HeaderItemProps>`
  width: calc(100% / 3);
  display: flex;
  justify-content: ${({ textAlign }) => {
    if (!textAlign) {
      return "flex-start";
    }
    return textAlign === "right" ? "flex-end" : "flex-start";
  }};
`;
