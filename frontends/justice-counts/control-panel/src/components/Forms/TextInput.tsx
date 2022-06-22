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
import statusCheckIcon from "../assets/status-check-icon.png";
import statusErrorIcon from "../assets/status-error-icon.png";
import { palette, typography } from "../GlobalStyles";

export const InputWrapper = styled.div`
  position: relative;
  display: flex;
  flex-direction: column;
  margin-bottom: 32px;
`;

type InputProps = {
  error?: string;
  placeholder?: string;
  multiline?: boolean;
  persistLabel?: boolean;
};

export const Input = styled.input<InputProps>`
  ${typography.sizeCSS.large}
  line-height: ${rem("30px")};
  resize: none;
  height: ${({ multiline }) => (multiline ? "200px;" : "71px;")};
  padding: ${({ persistLabel }) =>
    persistLabel ? "42px 16px 16px 16px" : "16px 55px 10px 16px"}};
  background: ${({ value, error }) => {
    if (error) {
      return palette.highlight.red;
    }
    return value || value === 0
      ? palette.highlight.lightblue1
      : palette.highlight.grey1;
  }};
  caret-color: ${({ error }) => {
    if (error) {
      return palette.solid.red;
    }
    return palette.solid.blue;
  }};

  border: none;
  border-bottom: 1px solid
    ${({ value, error, disabled }) => {
      if (error) {
        return palette.solid.red;
      }
      if (disabled) {
        return palette.highlight.grey8;
      }
      return value || value === 0
        ? palette.solid.blue
        : palette.highlight.grey9;
    }};

  &:hover {
    border-bottom: 1px solid ${palette.solid.blue};
  }

  &:focus ~ label {
    ${typography.sizeCSS.small}
    ${({ persistLabel }) => !persistLabel && "display: none"};
    top: 12px;
    color: ${({ error }) => {
      if (error) {
        return palette.solid.red;
      }
      return palette.solid.blue;
    }};
  }

  &:hover ~ label {
    color: ${({ error }) => !error && palette.solid.blue};
  }

  &::placeholder {
    opacity: 0;
    transition: 0.2s;
  }

  &:focus::placeholder {
    opacity: 1;
    transition: 0.2s;
    color: ${palette.highlight.grey6};
  }
`;

type InputLabelProps = {
  inputHasValue?: boolean;
  isDisabled?: boolean;
  error?: string;
  persistLabel?: boolean;
};

export const InputLabel = styled.label<InputLabelProps>`
  ${({ inputHasValue }) =>
    inputHasValue ? typography.sizeCSS.small : typography.sizeCSS.large}

  /* If persistLabel is false, the label is visible only when the input has no value
   * If persistLabel is true, when the input has value, show the label above the value
   */
  ${({ persistLabel, inputHasValue }) =>
    !persistLabel && inputHasValue && "display: none;"}

  min-height: 50px;
  position: absolute;
  top: ${({ inputHasValue }) => (inputHasValue ? "12px" : "26px")};
  left: 16px;
  z-index: -1;
  transition: 0.2s ease;

  color: ${({ error, isDisabled, inputHasValue }) => {
    if (error) {
      return palette.solid.red;
    }
    if (isDisabled) {
      return palette.highlight.grey6;
    }
    return inputHasValue ? palette.solid.blue : palette.highlight.grey8;
  }};
`;

type ErrorLabelProps = {
  isDisabled?: boolean;
  error?: string;
  multiline?: boolean;
};

export const ErrorLabel = styled.span<ErrorLabelProps>`
  ${typography.sizeCSS.small}
  ${({ error }) => error && `color: ${palette.solid.red};`};
  ${({ isDisabled }) => isDisabled && `color: ${palette.highlight.grey8};`}
  margin-top: 8px;
  position: absolute;
  ${({ multiline }) => `top: ${multiline ? "200" : "71"}px;`};
`;

export const LabelChipPosition = styled.span`
  position: absolute;
  top: 24px;
  right: 16px;
`;

export const RequiredChip = styled.span`
  ${typography.sizeCSS.small}
  background-color: ${palette.solid.blue};
  justify-content: center;
  align-items: center;
  height: 24px;
  padding: 4px 8px;
  color: ${palette.solid.white};

  &::after {
    content: "Required";
  }
`;

export const InputTooltip = styled.div`
  position: absolute;
  top: 72px;
  border-radius: 4px;
  padding: 16px;
  background-color: ${palette.solid.darkgrey};
  color: ${palette.solid.white};
`;

interface TextInputProps extends InputHTMLAttributes<HTMLInputElement> {
  label: string;
  error?: string;
  valueLabel?: string;
  multiline?: boolean;
  persistLabel?: boolean;
}

export const TextInput: React.FC<TextInputProps> = ({
  label,
  error,
  valueLabel,
  multiline,
  placeholder,
  persistLabel,
  ...props
}): JSX.Element => {
  const [showTooltip, setShowTooltip] = useState<boolean>();
  const { name, required, value, disabled } = props;

  const showTooltipIfTruncated = (
    e: React.MouseEvent<HTMLInputElement, MouseEvent>
  ) => {
    const labelElement = e.currentTarget.lastElementChild as HTMLElement;
    if (labelElement.offsetWidth < labelElement.scrollWidth) {
      setShowTooltip(true);
    }
  };
  const clearTooltip = () => setShowTooltip(false);

  return (
    <InputWrapper
      onMouseEnter={showTooltipIfTruncated}
      onMouseLeave={clearTooltip}
      onFocus={clearTooltip}
    >
      {/* Text Input */}
      <Input
        {...props}
        disabled={disabled}
        as={multiline ? "textarea" : "input"}
        multiline={multiline}
        error={error}
        id={`input-${name}`}
        placeholder={placeholder}
        persistLabel={persistLabel}
      />

      {/* Text Input Label (appears inside of text input) */}
      <InputLabel
        htmlFor={`input-${name}`}
        inputHasValue={Boolean(value)}
        isDisabled={disabled}
        persistLabel={persistLabel}
        error={error}
      >
        {label}
      </InputLabel>

      {showTooltip && <InputTooltip>{name}</InputTooltip>}

      {/* Error Description (appears below text input) */}
      {error && (
        <ErrorLabel isDisabled={disabled} error={error} multiline={multiline}>
          {error}
        </ErrorLabel>
      )}

      {/* Label Chip (appears inside of text input on the right) */}

      {/* Chip: Required */}
      {required && !error && !value && (
        <LabelChipPosition>
          <RequiredChip />
        </LabelChipPosition>
      )}

      {/* Chip: Error Status */}
      {error && (
        <LabelChipPosition>
          <img src={statusErrorIcon} alt="" width="24px" height="24px" />
        </LabelChipPosition>
      )}

      {/* Chip: Validated Successfully Status */}
      {!error && value && (
        <LabelChipPosition>
          <img src={statusCheckIcon} alt="" width="24px" height="24px" />
        </LabelChipPosition>
      )}
    </InputWrapper>
  );
};
