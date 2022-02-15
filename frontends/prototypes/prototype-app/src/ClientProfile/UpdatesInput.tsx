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

import { Button } from "@recidiviz/design-system";
import React from "react";
import styled from "styled-components/macro";

import { useDataStore } from "../StoreProvider";

const InputWrapper = styled.div`
  grid-area: 2/1;
  height: 80px;
  width: 100%;
  padding: 32px;
  display: flex;
`;

const Input = styled.input.attrs({ type: "text" })`
  height: 32px;
  margin-right: 16px;
  flex: 1 1 auto;
`;

const UpdatesInput: React.FC = () => {
  const { caseStore } = useDataStore();
  const [updateText, setUpdateText] = React.useState<string>("");

  const saveUpdate = async () => {
    if (updateText) {
      await caseStore.sendCompliantReportingUpdate(updateText);
      setUpdateText("");
    }
  };

  return (
    <InputWrapper>
      <Input
        type="text"
        placeholder="Add an update"
        value={updateText || ""}
        onChange={(e) => setUpdateText(e.currentTarget.value)}
        onKeyDown={(e) => {
          e.stopPropagation();
          if (e.key === "Escape") {
            setUpdateText("");
          } else if (e.key === "Enter") {
            saveUpdate();
          }
        }}
      />
      <Button kind="secondary" shape="block" onClick={() => saveUpdate()}>
        Send
      </Button>
    </InputWrapper>
  );
};

export default UpdatesInput;
