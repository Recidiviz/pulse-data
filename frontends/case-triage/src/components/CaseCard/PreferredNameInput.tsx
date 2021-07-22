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
import { Button, Icon, IconSVG } from "@recidiviz/design-system";
import * as React from "react";
import { useRootStore } from "../../stores";
import { Client } from "../../stores/ClientsStore";
import { TextInput } from "./PreferredNameInput.styles";
import { useToggleFocus } from "./useToggleFocus";

export interface PreferredNameInputProps {
  client: Client;
}

export const PreferredNameInput: React.FC<PreferredNameInputProps> = ({
  client,
}) => {
  const { clientsStore, errorMessageStore } = useRootStore();
  const [isToggled, setIsToggled, toggleRef, toggledRef] =
    useToggleFocus<HTMLButtonElement, HTMLInputElement>();

  const handleNameChange = async (name: string) => {
    setIsToggled(false);
    clientsStore
      .updateClientPreferredName(client, name)
      .catch(() =>
        errorMessageStore.pushErrorMessage("Failed to update preferred name.")
      );
  };

  if (isToggled) {
    return (
      <TextInput
        onKeyPress={async (event) => {
          if (event.key === "Enter") {
            return handleNameChange(event.currentTarget.value);
          }
        }}
        onBlur={async (event) => {
          // Focus should not be captured on blur, reset toggled to default state
          setIsToggled(undefined);
          return handleNameChange(event.currentTarget.value);
        }}
        onKeyDown={(event) => {
          // Pressing escape should un-toggle the text input
          if (event.key === "Esc" || event.key === "Escape") {
            // Stop propagation of the event so `react-modal` does not close the profile card
            event.stopPropagation();

            setIsToggled(false);
          }
        }}
        defaultValue={client.preferredName}
        ref={toggledRef}
      />
    );
  }

  return (
    <Button kind="link" onClick={() => setIsToggled(true)} ref={toggleRef}>
      {`${client.preferredName || "Name"} `}
      <Icon kind={IconSVG.Edit} size={9} aria-label="Edit" />
    </Button>
  );
};
