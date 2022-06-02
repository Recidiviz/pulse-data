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

import React, { InputHTMLAttributes } from "react";
import styled from "styled-components/macro";

import { palette, typography } from "../GlobalStyles";

export const BinaryRadioGroupContainer = styled.div`
  display: flex;
  flex-direction: column;
`;

export const BinaryRadioGroupWrapper = styled.div`
  width: 100%;
  display: flex;
  justify-content: space-between;
  align-items: center;
`;

export const BinaryRadioGroupQuestion = styled.div`
  ${typography.sizeCSS.medium}
  margin-top: 22px;
  color: ${palette.solid.darkgrey};
`;

export const RadioButtonWrapper = styled.div`
  display: flex;
  flex: 1 1 0;
  margin: 15px 0 0 0;

  &:first-child {
    margin: 15px 10px 0 0;
  }
`;

export const RadioButtonElement = styled.input`
  width: 0;
  position: fixed;
  opacity: 0;

  &:focus + label {
    border: 1px solid ${palette.highlight.grey9};
  }

  &:checked + label {
    background-color: ${palette.solid.blue};
    border-color: ${palette.solid.blue};
    color: ${palette.solid.white};
  }

  &:checked + label:hover {
    background-color: ${palette.solid.darkblue};
  }
`;

export const RadioButtonLabel = styled.label`
  ${typography.sizeCSS.medium}
  width: 100%;
  height: 56px;
  display: flex;
  justify-content: center;
  align-items: center;
  background: ${palette.highlight.grey1};
  padding: 16px 24px;
  border: 1px solid ${palette.highlight.grey3};
  border-radius: 2px;
  transition: 0.2s ease;

  &:hover {
    cursor: pointer;
    background: ${palette.highlight.grey2};
  }
`;

export const BinaryRadioGroupClearButton = styled.div`
  ${typography.sizeCSS.small}
  margin-top: 8px;
  color: ${palette.solid.blue};
  text-decoration: underline;

  &:hover {
    cursor: pointer;
  }
`;

interface RadioButtonProps extends InputHTMLAttributes<HTMLInputElement> {
  label: string;
  context?: string;
}

/** Single radio button in the style of a regular button */
export const BinaryRadioButton: React.FC<RadioButtonProps> = ({
  label,
  context,
  ...props
}): JSX.Element => {
  return (
    <RadioButtonWrapper>
      <RadioButtonElement {...props} />
      <RadioButtonLabel htmlFor={props.id}>{label}</RadioButtonLabel>
    </RadioButtonWrapper>
  );
};
