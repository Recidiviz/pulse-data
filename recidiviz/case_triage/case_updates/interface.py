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
from typing import Optional

from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.orm import Session

from recidiviz.case_triage.case_updates.serializers import serialize_client_case_version
from recidiviz.case_triage.case_updates.types import CaseUpdateActionType
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
    action_type: CaseUpdateActionType,
    comment: Optional[str] = None,
    action_ts: Optional[datetime] = None,
) -> None:
    """This method updates the case_updates table with the newly provided actions.
    This private method can be used liberally without regards for foreign key constraints,
    as it creates a series of CaseUpdate objects (one for each action) associated with the
    given officer_id and client. If other_text is provided, it's stored on the comment field.
    """
    action_ts = datetime.now() if action_ts is None else action_ts
    last_version = serialize_client_case_version(action_type, client).to_json()
    insert_statement = (
        insert(CaseUpdate)
        .values(
            person_external_id=client.person_external_id,
            officer_external_id=officer_id,
            state_code=client.state_code,
            action_type=action_type.value,
            action_ts=action_ts,
            last_version=last_version,
            comment=comment,
        )
        .on_conflict_do_update(
            constraint="unique_person_officer_action_triple",
            set_={
                "last_version": last_version,
                "action_ts": action_ts,
                "comment": comment,
            },
        )
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
        action: CaseUpdateActionType,
        comment: Optional[str] = None,
    ) -> None:
        """This method updates the case_updates table with the newly provided actions.

        Because the underlying table does not have foreign key constraints, independent
        validation must be provided before calling this method.
        """
        _update_case_for_person(
            session,
            officer.external_id,
            client,
            action,
            comment,
        )


class DemoCaseUpdatesInterface:
    """Implements interface for updating demo users."""

    @staticmethod
    def update_case_for_person(
        session: Session,
        user_email: str,
        client: ETLClient,
        action: CaseUpdateActionType,
        comment: Optional[str] = None,
        action_ts: Optional[datetime] = None,
    ) -> None:
        """This method updates the case_updates table for demo users.

        No checking is provided to ensure that the provided officer ids or person ids map
        back to anything.
        """
        _update_case_for_person(
            session,
            fake_officer_id_for_demo_user(user_email),
            client,
            action,
            comment,
            action_ts,
        )
