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
import { observer } from "mobx-react-lite";
import React, { useState } from "react";
import { ActionRow } from "./ActionRow";
import { CaseCardProps } from "./CaseCard.types";
import {
  FooterItem,
  Item,
  ItemsWrapper,
  NewItemsWrapper,
  PlainBullet,
} from "./NewItems.styles";
import { OpportunityAlert } from "./Alert/OpportunityAlert";
import { AddNote, NoteInput, NoteRow } from "./Notes";
import { CaseloadRemovalAlert } from "./Alert/CaseloadRemovalAlert";

export const NewItems = observer(({ client }: CaseCardProps): JSX.Element => {
  const [noteInProgress, setNoteInProgress] = useState(false);

  return (
    <NewItemsWrapper>
      <ItemsWrapper>
        {client.pendingCaseloadRemoval ? (
          <Item>
            <CaseloadRemovalAlert
              client={client}
              pendingUpdate={client.pendingCaseloadRemoval}
            />
          </Item>
        ) : (
          client.activeOpportunities.map((opp) => (
            <Item key={opp.opportunityType}>
              <OpportunityAlert client={client} opportunity={opp} />
            </Item>
          ))
        )}
        {client.activeNotes.map((note) => (
          <Item key={note.noteId}>
            <ActionRow bullet={<PlainBullet />}>
              <NoteRow note={note} />
            </ActionRow>
          </Item>
        ))}
        {noteInProgress && (
          <Item>
            <ActionRow bullet={<PlainBullet />}>
              <NoteInput
                onCommit={(text) => {
                  client.createNote({ text });
                  setNoteInProgress(false);
                }}
                requestClose={() => setNoteInProgress(false)}
              />
            </ActionRow>
          </Item>
        )}
      </ItemsWrapper>
      <FooterItem>
        <AddNote
          disabled={noteInProgress}
          startNote={() => setNoteInProgress(true)}
        />
      </FooterItem>
    </NewItemsWrapper>
  );
});
