// Recidiviz - a data platform for criminal justice reform
// Copyright (C) 2021 Recidiviz, Inc.
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

import { animation, fonts, palette, spacing } from "@recidiviz/design-system";
import { rem } from "polished";
import styled, { css } from "styled-components/macro";

export const InputWrapper = styled.div`
  display: flex;
  justify-content: space-between;
  width: 100%;
`;

const noteTextAlignmentStyles = css`
  margin: 0;
  margin-right: ${rem(spacing.md)};
  margin-top: ${rem(spacing.xs)};
`;

export const NoteText = styled.div`
  ${noteTextAlignmentStyles}
`;

export const Input = styled.input`
  ${noteTextAlignmentStyles}

  background: transparent;
  border: none;
  color: currentColor;
  flex: 1 1 auto;
  font-family: ${fonts.body};
  font-size: ${rem(16)};
  height: ${rem(24)};
  letter-spacing: -0.01em;
  line-height: 1.5;
  padding: 0;

  &:focus {
    outline: none;
  }
`;

export const KeyHint = styled.div`
  background: ${palette.slate10};
  color: ${palette.slate70};
  flex: 0 0 auto;
  font-size: ${rem(12)};
  height: ${rem(32)};
  padding: ${rem(spacing.sm)};
`;

export const AddNoteButton = styled.button.attrs({ type: "button" })`
  background: none;
  border: none;
  color: ${palette.signal.links};
  cursor: pointer;
  font-family: ${fonts.body};
  font-size: ${rem(16)};
  font-weight: 500;
  margin: 0;
  outline-offset: -${rem(spacing.xxs)};
  padding: ${rem(24)} ${rem(32)};
  text-align: left;
  transition: color ${animation.defaultDurationMs}ms;
  width: 100%;

  &:hover,
  &:focus {
    color: ${palette.pine4};
  }

  &:disabled {
    color: ${palette.slate60};
    cursor: not-allowed;
  }
`;
