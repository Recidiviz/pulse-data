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
import styled from "styled-components/macro";
import { rem } from "polished";
import { Button, fonts, palette, spacing } from "@recidiviz/design-system";
import { Form } from "formik";

export const Input = styled.textarea`
  background: ${palette.marble3};
  border-width: 0;
  border-radius: ${rem(4)};
  color: ${palette.text.normal};
  font-size: ${rem(16)};
  font-family ${fonts.body};
  padding: ${rem(spacing.md)};
  width: 100%;
  height: ${rem(117)};
  resize: none;

  &::placeholder {
    color: ${palette.text.caption};
    font-family: ${fonts.body};
  }
`;

export const Description = styled.label`
  font-size: ${rem(17)};
  margin: ${rem(spacing.xl)} 0;
  display: block;
`;

export const ReasonsContainer = styled.div`
  margin-top: ${rem(spacing.xl)};
  margin-bottom: ${rem(spacing.lg)};
`;

export const Reason = styled.span`
  margin-bottom: ${rem(spacing.sm)};
  font-size: ${rem("16px")};
  color: ${palette.text.links};
`;

export const SubmitContainer = styled.div`
  margin-top: ${rem(spacing.xl)};
  text-align: center;
`;

export const SubmitButton = styled.button`
  cursor: pointer;
  display: flex;
  flex-direction: row;
  justify-content: center;
  align-items: center;
  padding: ${rem(spacing.md)} ${rem(spacing.xl)};
  background: ${palette.signal.links};
  border: none;
  color: white;

  width: ${rem(262)};
  height: ${rem(48)};

  border-radius: ${rem(100)};

  margin: 0 auto;
  margin-bottom: ${rem(spacing.md)};

  font-size: ${rem(15)};

  &:disabled {
    cursor: not-allowed;
    background-color: ${palette.slate10};
    color: ${palette.slate80};
  }
`;

export const CancelButton = styled(Button).attrs({
  kind: "link",
})`
  font-size: ${rem(17)};
`;

export const CloseButton = styled(Button).attrs({ kind: "link" })`
  position: absolute;
  top: ${rem(spacing.xl)};
  right: ${rem(spacing.xl)};

  height: ${rem(16)};
  width: ${rem(16)};
`;

export const PaddedForm = styled(Form)`
  padding: ${rem(40)};
`;

export const ExtraPaddedForm = styled(Form)`
  padding: ${rem(40 + spacing.xl)} ${rem(40)} ${rem(40)} ${rem(40)};
`;
