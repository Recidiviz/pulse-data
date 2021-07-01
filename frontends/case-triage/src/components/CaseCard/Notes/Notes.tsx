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

import { Button, Icon } from "@recidiviz/design-system";
import React from "react";
import { observer } from "mobx-react-lite";
import {
  AddNoteButton,
  Input,
  InputWrapper,
  KeyHint,
  NoteText,
  NoteWrapper,
} from "./Notes.styles";
import { Note } from "../../../stores/ClientsStore";
import { ActionRow } from "../ActionRow";

type InputProps = {
  onCommit: (text: string) => void;
  requestClose: () => void;
};

export const NoteInput = ({
  onCommit,
  requestClose,
}: InputProps): JSX.Element => {
  return (
    <InputWrapper>
      <Input
        type="text"
        // this component is mounted in response to a user action, so no accessibility concerns
        // eslint-disable-next-line jsx-a11y/no-autofocus
        autoFocus
        placeholder="Start typing …"
        aria-label="add note"
        onKeyDown={(e) => {
          e.stopPropagation();
          if (e.key === "Escape") {
            e.currentTarget.value = "";
            requestClose();
          } else if (e.key === "Enter") {
            const noteText = e.currentTarget.value.trim();
            if (noteText) {
              onCommit(noteText);
            }
          }
        }}
      />
      <KeyHint>
        Enter <Icon kind="Return" width={18} height={8} />
      </KeyHint>
    </InputWrapper>
  );
};

export const NoteRow = observer(({ note }: { note: Note }) => (
  <NoteWrapper>
    <NoteText>{note.text}</NoteText>
    <Button
      kind="secondary"
      shape="block"
      icon="Check"
      iconSize={14}
      onClick={() => {
        note.setResolution(true);
      }}
    />
  </NoteWrapper>
));

export const AddNote = ({
  disabled,
  startNote,
}: {
  disabled?: boolean;
  startNote: () => void;
}): JSX.Element => (
  <AddNoteButton disabled={disabled} onClick={() => startNote()}>
    <ActionRow bullet={<Icon kind="AddFilled" size={24} />}>Add Task</ActionRow>
  </AddNoteButton>
);
