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

import { DropdownMenu, DropdownMenuItem } from "@recidiviz/design-system";
import React from "react";
import { ToggleLabelIndicator, ToggleLabelWrapper } from "./ToggleMenu.styles";

export type ToggleMenuProps<OptionId> = {
  className?: string;
  currentValue?: OptionId;
  options: { id: OptionId; label: string }[];
  setCurrentValue: (val?: OptionId) => void;
};

const ToggleItemLabel = ({
  label,
  active,
}: {
  label: string;
  active: boolean;
}) => {
  return (
    <ToggleLabelWrapper>
      {label}
      {active && <ToggleLabelIndicator />}
    </ToggleLabelWrapper>
  );
};

export const ToggleMenu = <OptionId extends string>({
  className,
  currentValue,
  options,
  setCurrentValue,
}: ToggleMenuProps<OptionId>): JSX.Element => {
  return (
    <DropdownMenu className={className}>
      {options.map(({ id, label }) => (
        <DropdownMenuItem
          key={id}
          onClick={() => {
            if (id === currentValue) {
              setCurrentValue(undefined);
            } else {
              setCurrentValue(id);
            }
          }}
        >
          <ToggleItemLabel label={label} active={id === currentValue} />
        </DropdownMenuItem>
      ))}
    </DropdownMenu>
  );
};
