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
from recidiviz.persistence.database.schema.case_triage.schema import (
    CaseUpdate,
    ETLClient,
    ETLOfficer,
)


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

        # First, we delete all old actions before uploading en masse the list of new ones
        delete_statement = delete(CaseUpdate).where(
            (CaseUpdate.person_external_id == client.person_external_id)
            & (CaseUpdate.officer_external_id == officer.external_id)
            & (CaseUpdate.state_code == officer.state_code)
        )
        session.execute(delete_statement)

        now = datetime.now()
        for action_type in actions:
            last_version = serialize_last_version_info(action_type, client).to_json()
            insert_statement = insert(CaseUpdate).values(
                person_external_id=client.person_external_id,
                officer_external_id=officer.external_id,
                state_code=officer.state_code,
                action_type=action_type.value,
                action_ts=now,
                last_version=last_version,
                comment=other_text,
            )
            session.execute(insert_statement)
        session.commit()
