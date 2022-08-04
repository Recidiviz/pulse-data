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

import { FormError } from "../../shared/types";
import { rem } from "../../utils";
import infoRedIcon from "../assets/info-red-icon.png";
import statusCheckIcon from "../assets/status-check-icon.png";
import statusErrorIcon from "../assets/status-error-icon.png";
import { palette, typography } from "../GlobalStyles";
import { NotReportedIcon } from ".";

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
  notReporting?: boolean;
};

export const Input = styled.input<InputProps>`
  ${typography.sizeCSS.large}
  line-height: ${rem("30px")};
  resize: none;
  height: ${({ multiline }) => (multiline ? "200px;" : "71px;")};
  padding: ${({ persistLabel }) =>
    persistLabel ? "42px 16px 16px 16px" : "16px 55px 10px 16px"}};
  background: ${({ value, error, notReporting }) => {
    if (error) {
      return palette.highlight.red;
    }
    if (notReporting) {
      return palette.highlight.grey1;
    }
    return value || value === 0
      ? palette.highlight.lightblue1
      : palette.highlight.grey1;
  }};
  ${({ notReporting }) => notReporting && `color: ${palette.highlight.grey6}`};
  
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
    border-bottom: ${({ disabled }) =>
      disabled ? undefined : `1px solid ${palette.solid.blue}`};
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
    color: ${({ error, disabled }) =>
      !error && !disabled && palette.solid.blue};
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
  notReporting?: boolean;
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

  color: ${({ error, isDisabled, inputHasValue, notReporting }) => {
    if (error) {
      return palette.solid.red;
    }
    if (isDisabled || notReporting) {
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
  display: flex;
  align-items: flex-start;
  justify-content: flex-start;
  width: 100%;
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

export const ErrorIconContainer = styled.span`
  transform: translate(1px, -1px);
  flex-grow: 1;
  z-index: 1;
`;

export const ErrorInfo = styled.div`
  position: absolute;
  background-color: ${palette.solid.red};
  color: ${palette.solid.white};
  border-radius: 4px;
  z-index: 1;
  padding: 16px;
  max-width: 300px;
  bottom: 24px;
`;

interface ErrorWithTooltipProps {
  error: FormError;
  disabled?: boolean;
  multiline?: boolean;
}

export const ErrorWithTooltip: React.FC<ErrorWithTooltipProps> = ({
  error,
  disabled,
  multiline,
}): JSX.Element => {
  const [showErrorInfo, setShowErrorInfo] = useState<boolean>();
  return (
    <ErrorLabel
      isDisabled={disabled}
      error={error.message}
      multiline={multiline}
    >
      {error.message}
      {error?.info && (
        <ErrorIconContainer>
          <img
            src={infoRedIcon}
            alt=""
            width="16px"
            height="16px"
            onMouseEnter={() => setShowErrorInfo(true)}
            onMouseLeave={() => setShowErrorInfo(false)}
          />
          {showErrorInfo && <ErrorInfo>{error.info}</ErrorInfo>}
        </ErrorIconContainer>
      )}
    </ErrorLabel>
  );
};

interface TextInputProps extends InputHTMLAttributes<HTMLInputElement> {
  label: string;
  error?: FormError;
  valueLabel?: string;
  multiline?: boolean;
  persistLabel?: boolean;
  metricKey?: string;
  notReporting?: boolean;
}

export const TextInput: React.FC<TextInputProps> = ({
  label,
  error,
  valueLabel,
  multiline,
  placeholder,
  persistLabel,
  metricKey,
  notReporting,
  ...props
}): JSX.Element => {
  const [showTooltip, setShowTooltip] = useState<boolean>();
  const { name, value, disabled } = props;

  const showTooltipIfTruncated = (
    e: React.MouseEvent<HTMLInputElement, MouseEvent>
  ) => {
    const labelElement = e.currentTarget.querySelector("label") as HTMLElement;
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
        data-metric-key={metricKey}
        disabled={disabled}
        as={multiline ? "textarea" : "input"}
        multiline={multiline}
        error={error?.message}
        id={`input-${name}`}
        placeholder={placeholder}
        persistLabel={persistLabel}
        notReporting={notReporting}
      />

      {/* Text Input Label (appears inside of text input) */}
      <InputLabel
        htmlFor={`input-${name}`}
        inputHasValue={Boolean(value)}
        isDisabled={disabled}
        persistLabel={persistLabel}
        error={error?.message}
        notReporting={notReporting}
      >
        {label}
      </InputLabel>

      {showTooltip && <InputTooltip>{name}</InputTooltip>}

      {/* Error Description (appears below text input) */}
      {error && (
        <ErrorWithTooltip
          error={error}
          disabled={disabled}
          multiline={multiline}
        />
      )}

      {/* Label Chip (appears inside of text input on the right) */}

      {/* Chip: Required */}
      {/* Disable the Required Chip for now. Refer to https://github.com/Recidiviz/pulse-data/pull/13849 for more information */}
      {/* {required && !error && !value && (
        <LabelChipPosition>
          <RequiredChip />
        </LabelChipPosition>
      )} */}
      {/* Chip: Not Reporting Status */}
      {notReporting && (
        <LabelChipPosition>
          <NotReportedIcon lighter />
        </LabelChipPosition>
      )}

      {/* Chip: Error Status */}
      {error && !notReporting && (
        <LabelChipPosition>
          <img src={statusErrorIcon} alt="" width="24px" height="24px" />
        </LabelChipPosition>
      )}

      {/* Chip: Validated Successfully Status */}
      {!error && !notReporting && value && (
        <LabelChipPosition>
          <img src={statusCheckIcon} alt="" width="24px" height="24px" />
        </LabelChipPosition>
      )}
    </InputWrapper>
  );
};
