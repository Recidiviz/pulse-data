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

import {
  Dropdown,
  DropdownMenu,
  DropdownMenuProps,
  DropdownToggle,
} from "@recidiviz/design-system";
import React from "react";
import { ActionRow, ActionRowProps } from "../ActionRow";
import { Caption } from "../CaseCard.styles";
import { AlertContents } from "./Alert.styles";

type AlertProps = ActionRowProps & {
  title: React.ReactNode;
  body: React.ReactNode;
  /**
   * Should render `DropdownMenuItem`s and `DropdownMenuLabel`s only
   */
  menuItems: NonNullable<DropdownMenuProps["children"]>;
};

export const Alert = ({
  bullet,
  title,
  body,
  menuItems,
}: AlertProps): JSX.Element => {
  return (
    <ActionRow {...{ bullet }}>
      <AlertContents>
        <div>
          <strong>{title}</strong>
        </div>
        <Caption>{body}</Caption>
      </AlertContents>
      <Dropdown>
        <DropdownToggle kind="borderless" icon="TripleDot" shape="block" />
        <DropdownMenu alignment="right">{menuItems}</DropdownMenu>
      </Dropdown>
    </ActionRow>
  );
};
