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

from recidiviz.case_triage.demo_helpers import (
    DEMO_STATE_CODE,
    fake_officer_id_for_demo_user,
    fake_person_id_for_demo_user,
)
from recidiviz.persistence.database.schema.case_triage.schema import (
    ETLClient,
    ETLOfficer,
    OfficerNote,
)


class OfficerNoteDoesNotExistError(ValueError):
    pass


def _create_note(
    session: Session, officer_id: str, client_id: str, state_code: str, text: str
) -> OfficerNote:
    insert_statement = insert(OfficerNote).values(
        state_code=state_code,
        officer_external_id=officer_id,
        person_external_id=client_id,
        text=text,
    )
    session.execute(insert_statement)
    session.commit()

    return (
        session.query(OfficerNote)
        .filter(
            OfficerNote.state_code == state_code,
            OfficerNote.officer_external_id == officer_id,
            OfficerNote.person_external_id == client_id,
        )
        .one()
    )


def _resolve_note(
    session: Session, officer_id: str, state_code: str, note_id: str, is_resolved: bool
) -> None:
    try:
        note = session.query(OfficerNote).filter(OfficerNote.note_id == note_id).one()
    except sqlalchemy.orm.exc.NoResultFound as e:
        raise OfficerNoteDoesNotExistError from e

    if note.officer_external_id != officer_id or note.state_code != state_code:
        raise OfficerNoteDoesNotExistError

    note.resolved_datetime = datetime.now(tz=pytz.UTC) if is_resolved else None
    session.commit()


def _update_note(
    session: Session, officer_id: str, state_code: str, note_id: str, new_text: str
) -> OfficerNote:
    try:
        note = session.query(OfficerNote).filter(OfficerNote.note_id == note_id).one()
    except sqlalchemy.orm.exc.NoResultFound as e:
        raise OfficerNoteDoesNotExistError from e

    if note.officer_external_id != officer_id or note.state_code != state_code:
        raise OfficerNoteDoesNotExistError

    note.text = new_text
    session.commit()

    return note


class OfficerNotesInterface:
    @staticmethod
    def create_note(
        session: Session, officer: ETLOfficer, client: ETLClient, text: str
    ) -> OfficerNote:
        return _create_note(
            session,
            officer.external_id,
            client.person_external_id,
            client.state_code,
            text,
        )

    @staticmethod
    def resolve_note(
        session: Session, officer: ETLOfficer, note_id: str, is_resolved: bool
    ) -> None:
        _resolve_note(
            session, officer.external_id, officer.state_code, note_id, is_resolved
        )

    @staticmethod
    def update_note(
        session: Session, officer: ETLOfficer, note_id: str, new_text: str
    ) -> OfficerNote:
        return _update_note(
            session, officer.external_id, officer.state_code, note_id, new_text
        )


class DemoOfficerNotesInterface:
    @staticmethod
    def create_note(
        session: Session, user_email: str, client: ETLClient, text: str
    ) -> OfficerNote:
        return _create_note(
            session,
            fake_officer_id_for_demo_user(user_email),
            fake_person_id_for_demo_user(user_email, client.person_external_id),
            client.state_code,
            text,
        )

    @staticmethod
    def resolve_note(
        session: Session, user_email: str, note_id: str, is_resolved: bool
    ) -> None:
        _resolve_note(
            session,
            fake_officer_id_for_demo_user(user_email),
            DEMO_STATE_CODE,
            note_id,
            is_resolved,
        )

    @staticmethod
    def update_note(
        session: Session, user_email: str, note_id: str, new_text: str
    ) -> OfficerNote:
        return _update_note(
            session,
            fake_officer_id_for_demo_user(user_email),
            DEMO_STATE_CODE,
            note_id,
            new_text,
        )
