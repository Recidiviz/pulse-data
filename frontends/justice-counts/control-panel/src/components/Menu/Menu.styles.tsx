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
import { DropdownMenuItem, DropdownToggle } from "@recidiviz/design-system";
import styled from "styled-components/macro";

import { HEADER_BAR_HEIGHT, palette, typography } from "../GlobalStyles";
import { ONE_PANEL_MAX_WIDTH } from "../Reports/ReportDataEntry.styles";

export const MenuContainer = styled.nav`
  font-family: ${typography.family};
  ${typography.sizeCSS.normal}
  display: flex;
  align-items: center;
  padding: 0 24px;
  gap: 24px;
`;

export const MenuItem = styled.div<{
  active?: boolean;
  highlight?: boolean;
  buttonPadding?: boolean;
}>`
  height: ${HEADER_BAR_HEIGHT}px;
  padding-top: ${({ buttonPadding }) => (buttonPadding ? `5px` : `14px`)};
  border-top: 6px solid
    ${({ active }) => (active ? palette.solid.blue : "transparent")};
  transition: 0.2s ease;
  color: ${({ highlight }) =>
    highlight ? palette.solid.red : palette.solid.darkgrey};

  a,
  a:visited {
    color: ${palette.solid.darkgrey};
    text-decoration: none;
    transition: 0.2s ease;
  }

  &:hover,
  a:hover {
    cursor: pointer;
    color: ${palette.solid.blue};
  }
`;

export const WelcomeUser = styled.div`
  color: ${palette.highlight.grey8};
  border-right: 1px solid black;
  padding-right: 24px;

  @media only screen and (max-width: ${ONE_PANEL_MAX_WIDTH}px) {
    display: none;
  }
`;

export const ExtendedDropdownToggle = styled(DropdownToggle)<{
  noMargin?: boolean;
}>`
  font-family: ${typography.family};
  ${typography.sizeCSS.normal}
  padding: 0;
  min-height: unset;
  line-height: 0;
  margin-bottom: ${({ noMargin }) => (noMargin ? "0" : "22px")};
  color: ${palette.solid.darkgrey};

  &[aria-expanded="true"] {
    color: ${palette.solid.blue};
  }

  &:hover {
    color: ${palette.solid.blue};
  }

  &:focus {
    color: ${palette.solid.darkgrey};
  }
`;

export const ExtendedDropdownMenuItem = styled(DropdownMenuItem)<{
  highlight?: boolean;
  noPadding?: boolean;
}>`
  min-width: 264px;
  display: flex;
  align-items: center;
  font-family: ${typography.family};
  ${typography.sizeCSS.normal}
  color: ${({ highlight }) =>
    highlight ? palette.solid.red : palette.solid.darkgrey};
  height: auto;
  padding: 0;
  gap: 8px;

  ${({ noPadding }) =>
    !noPadding &&
    `  
      padding: 16px;
      
      &:first-child {
        padding: 10px 16px 16px 16px;
      }
      
      &:last-child {
        padding: 16px 16px 10px 16px;
      }
    `}

  &:not(:last-child) {
    border-bottom: 1px solid ${palette.solid.offwhite};
  }

  &:focus {
    background-color: transparent;
    color: ${({ highlight }) =>
      highlight ? palette.solid.red : palette.solid.darkgrey};
  }

  &:hover {
    color: ${palette.solid.blue};
    background-color: transparent;

    svg path {
      stroke: ${palette.solid.blue};
    }
  }
`;
