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

import { rem } from "../../utils";
import { palette } from "../GlobalStyles";

export const InputWrapper = styled.div`
  position: relative;
  display: flex;
  flex-direction: column;
  margin-top: 20px;
  margin-bottom: 10px;
`;

type InputProps = {
  required?: boolean;
  error?: string;
  additionalContext?: boolean;
};

export const Input = styled.input<InputProps>`
  height: ${({ additionalContext }) => (additionalContext ? "46px" : "71px")};
  padding: ${({ additionalContext }) =>
    additionalContext ? "16px 11px 10px 16px" : "42px 90px 16px 16px"};
  font-size: ${rem("24px")};
  line-height: 24px;

  background: ${({ value, error }) => {
    if (error) {
      return palette.highlight.red;
    }
    return value ? palette.highlight.lightblue : palette.highlight.grey1;
  }};

  border: none;
  border-bottom: 1px solid
    ${({ value, error, disabled }) => {
      if (error) {
        return palette.solid.red;
      }
      if (disabled) {
        return palette.highlight.grey4;
      }
      return value !== "" ? palette.solid.blue : palette.highlight.grey6;
    }};

  &:focus ~ label {
    ${({ additionalContext }) => additionalContext && "display: none"};
    top: 12px;
    font-size: ${rem("12px")};
    line-height: 16px;
    color: ${({ required, error }) => {
      if (error) {
        return palette.solid.red;
      }
      return required ? palette.solid.blue : palette.solid.darkgrey;
    }};
  }

  &::placeholder {
    opacity: 0;
    transition: 0.2s;
    color: ${palette.highlight.grey4};
  }

  &:focus::placeholder {
    opacity: 1;
    transition: 0.2s;
  }
`;

type InputLabelProps = {
  required?: boolean;
  inputHasValue?: boolean;
  isDisabled?: boolean;
  additionalContext?: boolean;
  error?: string;
};

export const InputLabel = styled.label<InputLabelProps>`
  /* For Additional Context input: we only need this label visible when the input is not focused and has no input */
  ${({ additionalContext, inputHasValue }) =>
    additionalContext && inputHasValue && "display: none;"}

  position: absolute;
  top: ${({ inputHasValue }) => (inputHasValue ? "12px" : "26px")};
  ${({ additionalContext }) =>
    additionalContext &&
    "top: 14px"}; /* Override if this is an Additional Context input */
  left: 16px;
  z-index: -1;
  transition: 0.2s ease;

  font-size: ${({ inputHasValue }) =>
    inputHasValue ? rem("12px") : rem("20px")};
  line-height: ${({ inputHasValue }) => (inputHasValue ? "16px" : "20px")};
  color: ${({ error, isDisabled, required, inputHasValue }) => {
    if (error) {
      return palette.solid.red;
    }
    if (isDisabled) {
      return palette.highlight.grey4;
    }
    return required || inputHasValue ? palette.solid.blue : palette.solid.grey;
  }};
`;

type InputContextLabelProps = {
  isDisabled?: boolean;
};

export const InputContextLabel = styled.span<InputContextLabelProps>`
  ${({ isDisabled }) => isDisabled && `color: ${palette.highlight.grey4};`}
  font-size: ${rem("12px")};
  line-height: 16px;
  margin-top: 8px;
`;

type NotificationLabelProps = {
  error?: string;
  required?: boolean;
  valueLabel?: string;
};

export const NotificationLabel = styled.span<NotificationLabelProps>`
  background-color: ${({ required, error }) => {
    if (error) {
      return palette.solid.red;
    }
    return required ? palette.solid.blue : palette.highlight.grey4;
  }};
  display: ${({ valueLabel }) => (valueLabel ? `flex` : `none`)};
  justify-content: center;
  align-items: center;
  height: 32px;
  position: absolute;
  top: 20px;
  right: 16px;
  padding: 4px 8px;
  font-size: ${rem("12px")};
  line-height: 24px;
  text-transform: uppercase;
  border-radius: 2px;
  color: ${palette.solid.white};
`;

export const AdditionalContextLabel = styled.div`
  font-size: ${rem("20px")};
  line-height: 24px;
  margin-top: 37px;
`;

interface TextInputProps extends InputHTMLAttributes<HTMLInputElement> {
  label: string;
  context?: string;
  error?: string;
  valueLabel?: string;
  additionalContext?: boolean;
  resetField?: (name: string) => void;
}

export const TextInput: React.FC<TextInputProps> = ({
  label,
  context,
  error,
  valueLabel,
  additionalContext,
  ...props
}): JSX.Element => {
  const { name, required, value, disabled } = props;

  return (
    <InputWrapper>
      {/* Text Input */}
      <Input {...props} error={error} additionalContext={additionalContext} />

      {/* Label (appears inside of text input) */}
      <InputLabel
        htmlFor={name}
        required={required}
        inputHasValue={value !== ""}
        isDisabled={disabled}
        additionalContext={additionalContext}
        error={error}
      >
        {label}
      </InputLabel>

      {/* Context Description (appears below text input) */}
      <InputContextLabel isDisabled={disabled}>{context}</InputContextLabel>

      {/* Value Label Chip (appears inside of text input) */}
      <NotificationLabel
        error={error}
        valueLabel={valueLabel}
        required={required}
      >
        {valueLabel}
      </NotificationLabel>
    </InputWrapper>
  );
};
