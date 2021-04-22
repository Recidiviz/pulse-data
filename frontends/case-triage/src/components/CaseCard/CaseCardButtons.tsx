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
import * as React from "react";
import styled from "styled-components/macro";
import { rem } from "polished";
import {
  Button,
  Icon,
  IconSVG,
  palette,
  spacing,
} from "@recidiviz/design-system";
import Tooltip from "../Tooltip";

const BaseCheckboxButton = styled.div`
  cursor: pointer;
  padding: ${rem(spacing.xs)} ${rem(12)};
  display: flex;
  align-items: center;
  justify-content: center;

  font-size: ${rem("14px")};
  line-height: ${rem(24)};

  box-sizing: border-box;
  border-radius: 4px;
`;

const CheckedButton = styled(BaseCheckboxButton)`
  color: ${palette.white};
  background-color: ${palette.slate60};
  border: 0px;

  border-radius: 16px;

  &:hover {
    background-color: ${palette.slate70};
  }
`;

export const UncheckedButton = styled(BaseCheckboxButton)`
  background-color: transparent;

  color: ${palette.pine4};
  border: 1px solid ${palette.slate30};

  &:hover {
    color: ${palette.pine2};
    border: 1px solid ${palette.signal.links};
  }
`;

const CloseButton = styled(Button).attrs({ kind: "link" })`
  margin-left: ${rem(spacing.sm)};
  height: 16px;
  width: 16px;
`;

export interface NeedsCheckboxButtonProps {
  title: React.ReactNode;

  checked?: boolean;
  onToggleCheck?: (checked: boolean) => void;
}

export const NeedsCheckboxButton: React.FC<NeedsCheckboxButtonProps> = ({
  title,
  checked,
  onToggleCheck,
}) => {
  const [checkedState, setCheckedState] = React.useState(false);
  React.useEffect(() => {
    if (checked !== undefined) {
      setCheckedState(checked);
    }
  }, [checked]);

  const Component = checkedState ? CheckedButton : UncheckedButton;
  const componentClick = checked
    ? undefined
    : () => {
        setCheckedState(true);
        if (onToggleCheck) {
          onToggleCheck(true);
        }
      };

  return (
    <Component onClick={componentClick}>
      {title}
      {checked ? (
        <CloseButton
          onClick={() => {
            setCheckedState(false);
            if (onToggleCheck) {
              onToggleCheck(false);
            }
          }}
        >
          <Tooltip title="Remove">
            <Icon kind={IconSVG.CloseOutlined} fill={palette.white} size={16} />
          </Tooltip>
        </CloseButton>
      ) : null}
    </Component>
  );
};
