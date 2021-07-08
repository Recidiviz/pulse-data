# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2021 Recidiviz, Inc.
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program.  If not, see <https://www.gnu.org/licenses/>.
# =============================================================================
"""Defines the interface for interacting with notes."""
from datetime import datetime

import pytz
import sqlalchemy.orm.exc
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.orm import Session

from recidiviz.case_triage.permissions_checker import PermissionsChecker
from recidiviz.case_triage.user_context import UserContext
from recidiviz.persistence.database.schema.case_triage.schema import (
    ETLClient,
    OfficerNote,
)


class OfficerNoteDoesNotExistError(ValueError):
    pass


class OfficerNotesInterface:
    """Defines the interface for interacting with notes."""

    @staticmethod
    def create_note(
        session: Session, user_context: UserContext, client: ETLClient, text: str
    ) -> OfficerNote:
        """Creates a new officer note in postgres for a given client."""
        officer_id = user_context.officer_id
        client_id = user_context.person_id(client)
        state_code = user_context.client_state_code(client)
        insert_statement = insert(OfficerNote).values(
            state_code=state_code,
            officer_external_id=officer_id,
            person_external_id=client_id,
            text=text,
        )
        result = session.execute(insert_statement)
        session.commit()

        return session.query(OfficerNote).get(result.inserted_primary_key)

    @staticmethod
    def resolve_note(
        session: Session, user_context: UserContext, note_id: str, is_resolved: bool
    ) -> None:
        """Resolves a new officer note in postgres for a given client with a timestamp."""
        try:
            note = (
                session.query(OfficerNote).filter(OfficerNote.note_id == note_id).one()
            )
        except sqlalchemy.orm.exc.NoResultFound as e:
            raise OfficerNoteDoesNotExistError from e

        if not PermissionsChecker.note_belongs_to(note, user_context):
            raise OfficerNoteDoesNotExistError

        note.resolved_datetime = datetime.now(tz=pytz.UTC) if is_resolved else None
        session.commit()

    @staticmethod
    def update_note(
        session: Session, user_context: UserContext, note_id: str, new_text: str
    ) -> OfficerNote:
        """Updates an existing officer note in postgres for a given client."""
        try:
            note = (
                session.query(OfficerNote).filter(OfficerNote.note_id == note_id).one()
            )
        except sqlalchemy.orm.exc.NoResultFound as e:
            raise OfficerNoteDoesNotExistError from e

        if not PermissionsChecker.note_belongs_to(note, user_context):
            raise OfficerNoteDoesNotExistError

        note.text = new_text
        session.commit()

        return note
