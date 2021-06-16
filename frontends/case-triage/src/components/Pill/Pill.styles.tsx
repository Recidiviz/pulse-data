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

import { palette, spacing } from "@recidiviz/design-system";
import { rem } from "polished";
import styled from "styled-components/macro";

type PillKind = "highlight" | "info" | "warn" | "error" | "neutral" | "muted";

const pillColors: Record<PillKind, string> = {
  highlight: palette.signal.highlight,
  info: palette.data.teal1,
  warn: palette.data.gold1,
  muted: palette.slate10,
  neutral: palette.slate60,
  error: palette.signal.error,
};

type PillProps = {
  kind: PillKind;
  filled: boolean;
};

const pillPropsToStyles = ({ kind, filled }: PillProps): string => {
  const color = pillColors[kind];
  const property = filled ? "background-color" : "border-color";

  return `
    ${property}: ${color};
    ${filled && kind !== "muted" ? `color: ${palette.white};` : ""}
  `;
};

export const Pill = styled.span<PillProps>`
  align-items: center;
  border-radius: ${rem(16)};
  border: 1px solid transparent;
  box-sizing: border-box;
  color: ${palette.text.caption};
  display: inline-flex;
  font-size: ${rem(14)};
  height: ${rem(32)};
  justify-content: center;
  margin-right: ${rem(spacing.xs)};
  padding: ${rem(spacing.xs)} ${rem(12)};
  white-space: nowrap;

  ${pillPropsToStyles}
`;
