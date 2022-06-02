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

import React, { SelectHTMLAttributes } from "react";
import styled from "styled-components/macro";

import { palette, typography } from "../GlobalStyles";

const DropdownContainer = styled.div`
  position: relative;
  width: 100%;
  &:first-child {
    margin-right: 10px;
  }
`;

const DropdownSelection = styled.select`
  ${typography.sizeCSS.large}
  height: 71px;
  width: 100%;
  margin: 10px 0;
  background: ${palette.highlight.lightblue1};
  color: ${palette.solid.darkgrey};
  caret-color: ${palette.solid.blue};
  border: none;
  border-bottom: 1px solid ${palette.solid.blue};

  padding-left: 16px;
  margin-top: 15px;

  appearance: none;

  &:hover {
    cursor: pointer;
  }
`;

const DropdownArrowContainer = styled.div`
  pointer-events: none;
  width: 30px;
  height: 30px;
  border-radius: 15px;
  background: ${palette.highlight.lightblue2};
  position: absolute;
  top: 35px;
  right: 16px;
  display: flex;
  align-items: center;
  justify-content: center;
`;

const DropdownArrow = styled.div`
  width: 12px;
  height: 12px;
  border: none;
  border-bottom: 4px solid ${palette.solid.blue};
  border-right: 4px solid ${palette.solid.blue};
  transform: rotate(45deg) translate(-1px, -1px);
`;

interface DropdownProps extends SelectHTMLAttributes<HTMLSelectElement> {
  children: JSX.Element[];
}

export const Dropdown: React.FC<DropdownProps> = ({
  value,
  onChange,
  children,
}) => (
  <DropdownContainer>
    <DropdownSelection onChange={onChange} value={value}>
      {children}
    </DropdownSelection>
    <DropdownArrowContainer>
      <DropdownArrow />
    </DropdownArrowContainer>
  </DropdownContainer>
);
