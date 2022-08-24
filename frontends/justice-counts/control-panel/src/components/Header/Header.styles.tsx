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

import { palette } from "../GlobalStyles/Palette";

export const HeaderBar = styled.header`
  width: 100%;
  height: 64px;
  display: flex;
  justify-content: space-between;
  align-items: center;
  position: fixed;
  top: 0;
  z-index: 3;
  background: ${palette.solid.white};
  padding: 16px 0;
`;

export const LogoContainer = styled.div`
  height: 64px;
  width: 64px;
  display: flex;
  justify-content: center;
  align-items: center;
  background: ${palette.solid.green};
  transition: 0.3s ease;

  &:hover {
    cursor: pointer;
    opacity: 0.9;
  }
`;

export const Logo = styled.img`
  width: 48px;
  height: 48px;
`;
