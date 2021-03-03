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
from typing import Any, Dict, List, Optional

from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.orm import Session

from recidiviz.case_triage.case_updates.types import CaseUpdateActionType
from recidiviz.case_triage.case_updates.serializers import serialize_case_update_action
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
        update_metadata: Dict[str, Any] = {
            "actions": CaseUpdatesInterface.serialize_actions(client, actions)
        }
        if other_text:
            update_metadata["otherText"] = other_text

        insert_statement = (
            insert(CaseUpdate)
            .values(
                person_external_id=client.person_external_id,
                officer_external_id=officer.external_id,
                state_code=officer.state_code,
                update_metadata=update_metadata,
            )
            .on_conflict_do_update(
                index_elements=[
                    "person_external_id",
                    "officer_external_id",
                    "state_code",
                ],
                set_={
                    "update_metadata": update_metadata,
                },
            )
        )
        session.execute(insert_statement)
        session.commit()

    @staticmethod
    def serialize_actions(
        client: ETLClient, actions: List[CaseUpdateActionType]
    ) -> List[Dict[str, Any]]:
        """Serializes a list of actions for a client into the necessary metadata needed to eventually
        retrieve it."""
        now = datetime.now()
        return [
            serialize_case_update_action(action, client, now).to_json()
            for action in actions
        ]
