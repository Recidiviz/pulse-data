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

import React from "react";

import { AgencySystems } from "../../shared/types";
import { removeSnakeCase } from "../../utils";
import { ReactComponent as CheckIcon } from "../assets/check-icon.svg";
import {
  FileName,
  SelectSystemOptions,
  SystemName,
  UserPromptContainer,
  UserPromptTitle,
  UserPromptWrapper,
} from ".";

type SystemSelectionProps = {
  selectedFile: File;
  userSystems: AgencySystems[];
  handleSystemSelection: (file: File, system: AgencySystems) => void;
};

export const SystemSelection: React.FC<SystemSelectionProps> = ({
  selectedFile,
  userSystems,
  handleSystemSelection,
}) => {
  return (
    <UserPromptContainer>
      <UserPromptWrapper>
        <FileName>
          <CheckIcon />
          {selectedFile.name}
        </FileName>
        <UserPromptTitle>Which system is this data for?</UserPromptTitle>

        <SelectSystemOptions>
          {userSystems.map((system) => (
            <SystemName
              key={system}
              onClick={() => handleSystemSelection(selectedFile, system)}
            >
              {removeSnakeCase(system)}
              <span>→</span>
            </SystemName>
          ))}
        </SelectSystemOptions>
      </UserPromptWrapper>
    </UserPromptContainer>
  );
};
