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
"""Implements interface for querying case_updates."""
from datetime import datetime
from typing import List, Optional

from sqlalchemy import delete
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.orm import Session

from recidiviz.case_triage.case_updates.types import CaseUpdateActionType
from recidiviz.case_triage.case_updates.serializers import serialize_last_version_info
from recidiviz.case_triage.demo_helpers import (
    fake_officer_id_for_demo_user,
)
from recidiviz.persistence.database.schema.case_triage.schema import (
    CaseUpdate,
    ETLClient,
    ETLOfficer,
)


def _update_case_for_person(
    session: Session,
    officer_id: str,
    client: ETLClient,
    actions: List[CaseUpdateActionType],
    other_text: Optional[str] = None,
) -> None:
    """This method updates the case_updates table with the newly provided actions.

    This private method can be used liberally without regards for foreign key constraints,
    as it creates a series of CaseUpdate objects (one for each action) associated with the
    given officer_id and client. If other_text is provided, it's stored on the comment field.
    """
    # First, we delete all old actions before uploading en masse the list of new ones
    delete_statement = delete(CaseUpdate).where(
        (CaseUpdate.person_external_id == client.person_external_id)
        & (CaseUpdate.officer_external_id == officer_id)
        & (CaseUpdate.state_code == client.state_code)
    )
    session.execute(delete_statement)

    now = datetime.now()
    for action_type in actions:
        last_version = serialize_last_version_info(action_type, client).to_json()
        insert_statement = insert(CaseUpdate).values(
            person_external_id=client.person_external_id,
            officer_external_id=officer_id,
            state_code=client.state_code,
            action_type=action_type.value,
            action_ts=now,
            last_version=last_version,
            comment=other_text,
        )
        session.execute(insert_statement)
    session.commit()


class CaseUpdatesInterface:
    """Implements interface for querying case_updates."""

    @staticmethod
    def update_case_for_person(
        session: Session,
        officer: ETLOfficer,
        client: ETLClient,
        actions: List[CaseUpdateActionType],
        other_text: Optional[str] = None,
    ) -> None:
        """This method updates the case_updates table with the newly provided actions.

        Because the underlying table does not have foreign key constraints, independent
        validation must be provided before calling this method.
        """
        _update_case_for_person(
            session,
            officer.external_id,
            client,
            actions,
            other_text,
        )


class DemoCaseUpdatesInterface:
    """Implements interface for updating demo users."""

    @staticmethod
    def update_case_for_person(
        session: Session,
        user_email: str,
        client: ETLClient,
        actions: List[CaseUpdateActionType],
        other_text: Optional[str] = None,
    ) -> None:
        """This method updates the case_updates table for demo users.

        No checking is provided to ensure that the provided officer ids or person ids map
        back to anything.
        """
        _update_case_for_person(
            session,
            fake_officer_id_for_demo_user(user_email),
            client,
            actions,
            other_text,
        )
