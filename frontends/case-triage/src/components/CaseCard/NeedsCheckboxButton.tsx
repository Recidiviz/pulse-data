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
import { palette } from "@recidiviz/case-triage-components";

const BaseCheckboxButton = styled.button.attrs({
  type: "button",
})`
  cursor: pointer;
  min-height: 40px;
  padding: 12px 16px;
  display: flex;
  align-items: center;
  justify-content: center;

  font-size: ${rem("14px")};
  line-height: ${rem("16px")};

  box-sizing: border-box;
  border-radius: 4px;
`;

const CheckedCheckboxButton = styled(BaseCheckboxButton)`
  color: ${palette.white};
  background-color: ${palette.signal.links};
  border: 1px solid ${palette.signal.links};

  &:hover {
    background-color: ${palette.signal.links};
  }
`;

const UncheckedCheckboxButton = styled(
  BaseCheckboxButton
)<NeedsCheckboxButtonProps>`
  background-color: transparent;

  ${(props) =>
    props.disabled
      ? `
  color: ${palette.text.caption};
  border: 1px solid ${palette.marble5};
`
      : `
  color: ${palette.signal.links};
  border: 1px solid rgba(0, 108, 103, 0.3);

  &:hover {
    color: ${palette.text.normal};
  }
`}
`;

export interface NeedsCheckboxButtonProps {
  children: React.ReactChild | React.ReactChild[];

  inProgress?: boolean;
  checked?: boolean;
  onToggleCheck?: (checked: boolean) => void;
}

export const NeedsCheckboxButton: React.FC<NeedsCheckboxButtonProps> = ({
  children,
  inProgress,
  checked,
  onToggleCheck,
}) => {
  const [checkedState, setCheckedState] = React.useState(false);
  React.useEffect(() => {
    if (checked !== undefined) {
      setCheckedState(checked);
    }
  }, [checked]);

  const Component =
    checkedState && !inProgress
      ? CheckedCheckboxButton
      : UncheckedCheckboxButton;
  const onClick = () => {
    const nextCheck = !checkedState;
    setCheckedState(nextCheck);
    if (onToggleCheck) {
      onToggleCheck(nextCheck);
    }
  };

  return (
    <Component disabled={inProgress} onClick={onClick}>
      {!inProgress ? children : "In Progress"}
    </Component>
  );
};
