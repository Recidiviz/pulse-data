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
// =============================================================================import styled from "styled-components/macro";
import styled from "styled-components/macro";
import {
  Icon,
  IconSVG,
  palette,
  spacing,
} from "@recidiviz/case-triage-components";

interface CheckboxProps {
  checked?: boolean;
}

export const CheckedIcon = styled(Icon).attrs({
  kind: IconSVG.Check,
  fill: palette.white.main,
  height: 10,
  width: 10,
})`
  display: none;
  position: absolute;
  top: 2px;
  left: 2px;
`;

export const Checkbox = styled.div<CheckboxProps>`
  display: inline-block;
  margin: 0;
  margin-right: ${spacing.sm};
  height: 16px;
  width: 16px;
  border: 1px solid ${palette.tints["5"]};
  border-radius: 4px;
  vertical-align: middle;
  position: relative;
  ${(props) =>
    props.checked
      ? `background: ${palette.pine.main}; border-color: ${palette.pine.main};`
      : null}

  ${CheckedIcon} {
    display: ${(props) => (props.checked ? `block` : `none`)};
  }
`;

export const CheckboxContainer = styled.div.attrs({
  tabIndex: 0,
  role: "checkbox",
})`
  cursor: pointer;
`;
