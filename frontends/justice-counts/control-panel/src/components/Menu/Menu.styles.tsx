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

import { palette, typography } from "../GlobalStyles";

export const MenuContainer = styled.nav`
  font-family: ${typography.family};
  ${typography.sizeCSS.normal}
  display: flex;
  align-items: center;
  padding: 20px 24px;
`;

export const MenuItem = styled.div<{ active?: boolean; highlight?: boolean }>`
  height: 64px;
  padding-top: 14px;
  margin-left: 24px;
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
`;

export const ExtendedDropdownToggle = styled(DropdownToggle)`
  font-family: ${typography.family};
  ${typography.sizeCSS.normal}
  padding: 0;
  min-height: unset;
  line-height: 0;
  margin-bottom: 22px;
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
}>`
  min-width: 264px;
  display: flex;
  font-family: ${typography.family};
  ${typography.sizeCSS.normal}
  color: ${({ highlight }) =>
    highlight ? palette.solid.red : palette.solid.darkgrey};
  padding: 16px;
  height: auto;

  &:first-child {
    padding: 10px 16px 16px 16px;
  }

  &:last-child {
    padding: 16px 16px 10px 16px;
  }

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
  }
`;
