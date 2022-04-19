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

import { palette } from "../GlobalStyles";

export const BinaryRadioGroupContainer = styled.div`
  width: 100%;
  display: flex;
  justify-content: space-between;
  align-items: center;
`;

export const RadioButtonWrapper = styled.div`
  width: 100%;

  &:first-child {
    margin: 0 10px 0 0;
  }
`;

export const RadioButtonElement = styled.input`
  width: 0;
  position: fixed;
  opacity: 0;

  &:focus + label {
    border: 1px solid ${palette.highlight.grey3};
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
  display: flex;
  justify-content: center;
  align-items: center;
  width: 100%;
  height: 56px;

  background: ${palette.highlight.grey1};
  padding: 10px 20px;
  font-size: 16px;
  border: 1px solid ${palette.highlight.grey3};
  border-radius: 2px;
  transition: 0.2s ease;

  margin: 10px 0;

  &:hover {
    cursor: pointer;
    background: ${palette.highlight.grey2};
  }
`;

interface RadioButtonProps extends InputHTMLAttributes<HTMLInputElement> {
  label: string;
  context?: string;
}

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
