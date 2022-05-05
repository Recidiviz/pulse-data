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

import React, { InputHTMLAttributes, useState } from "react";
import styled from "styled-components/macro";

import { rem } from "../../utils";
import { palette } from "../GlobalStyles";

export const DisaggregationContent = styled.div`
  border-left: 1px solid black;
  padding-left: 25px;
`;

export const DisaggregationContentHelperText = styled.div`
  font-size: ${rem("15px")};
  line-height: 24px;
  margin-top: 15px;
`;

export const ToggleSwitchContainer = styled.div`
  display: block;
`;

export const ToggleSwitchWrapper = styled.div`
  display: flex;
  align-items: center;
  border-bottom: 1px solid black;
  padding: 8px 0;
`;

export const ToggleSwitch = styled.label`
  position: relative;
  display: inline-block;
  width: 20px;
  height: 12px;
`;

export const ToggleSwitchInput = styled.input`
  opacity: 0;
  width: 0;
  height: 0;

  &:checked + span {
    background-color: ${palette.solid.blue};
  }

  &:checked + span:before {
    transform: translateX(7px);
  }
`;

export const Slider = styled.span`
  position: absolute;
  cursor: pointer;
  top: 0;
  left: 0;
  right: 0;
  bottom: 0;
  background-color: ${palette.highlight.grey5};
  border-radius: 34px;
  transition: 0.3s;

  &:before {
    content: "";
    height: 7px;
    width: 7px;
    position: absolute;
    left: 3px;
    bottom: 2.5px;
    background-color: ${palette.solid.white};
    border-radius: 50%;
    transition: 0.3s;
  }
`;

const ToggleSwitchLabel = styled.span`
  font-size: ${rem("15px")};
  line-height: 24px;
  margin-left: 10px;
`;

interface DisaggregationToggleProps
  extends InputHTMLAttributes<HTMLInputElement> {
  description?: string;
}

/**
 * Wraps input form elements and delivers a switch to toggle the visibility of those elements.
 */
export const DisaggregationToggle: React.FC<DisaggregationToggleProps> = ({
  description,
  children,
}): JSX.Element => {
  const [open, setOpen] = useState(true);

  const toggleOpen = () => setOpen(!open);

  return (
    <ToggleSwitchContainer>
      {/* Toggle Switch */}
      <ToggleSwitchWrapper>
        <ToggleSwitch>
          <ToggleSwitchInput
            onChange={toggleOpen}
            type="checkbox"
            checked={open}
          />
          <Slider />
        </ToggleSwitch>

        {/* Toggle Switch Label */}
        <ToggleSwitchLabel>{description}</ToggleSwitchLabel>
      </ToggleSwitchWrapper>

      {/* Additional Context Content (form elements) */}
      <DisaggregationContent>{open && children}</DisaggregationContent>
    </ToggleSwitchContainer>
  );
};
