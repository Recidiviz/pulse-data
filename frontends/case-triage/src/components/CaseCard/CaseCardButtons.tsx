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
import { Button, Icon, IconSVG, spacing } from "@recidiviz/design-system";
import Tooltip from "../Tooltip";
import { Pill } from "../Pill";

export const UncheckedButton = styled(Button).attrs({
  kind: "secondary",
  shape: "block",
})`
  margin-right: ${rem(spacing.xs)};
`;

const CloseButton = styled(Button).attrs({ kind: "link" })`
  color: white;
  margin-left: ${rem(spacing.sm)};
  height: 16px;
  width: 16px;
`;

export interface NeedsCheckboxButtonProps {
  title: React.ReactNode;
  tooltip?: string;

  checked?: boolean;
  onToggleCheck?: (checked: boolean) => void;
}

export const NeedsCheckboxButton: React.FC<NeedsCheckboxButtonProps> = ({
  title,
  tooltip,
  checked,
  onToggleCheck,
}) => {
  if (checked) {
    return (
      <Pill filled kind="neutral">
        {tooltip ? <Tooltip title={tooltip}>{title}</Tooltip> : title}
        <CloseButton
          onClick={() => {
            if (onToggleCheck) {
              onToggleCheck(false);
            }
          }}
          aria-label="Remove"
        >
          <Tooltip title="Remove">
            <Icon kind={IconSVG.CloseOutlined} size={16} />
          </Tooltip>
        </CloseButton>
      </Pill>
    );
  }

  const componentClick = () => {
    if (onToggleCheck) {
      onToggleCheck(true);
    }
  };

  return <UncheckedButton onClick={componentClick}>{title}</UncheckedButton>;
};
