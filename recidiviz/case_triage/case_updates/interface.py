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
from typing import Optional

import sqlalchemy.orm.exc
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.orm import Session

from recidiviz.case_triage.case_updates.serializers import serialize_client_case_version
from recidiviz.case_triage.case_updates.types import CaseUpdateActionType
from recidiviz.case_triage.user_context import UserContext
from recidiviz.persistence.database.schema.case_triage.schema import (
    CaseUpdate,
    ETLClient,
)


class CaseUpdateDoesNotExistError(ValueError):
    pass


class CaseUpdatesInterface:
    """Implements interface for querying case_updates."""

    @staticmethod
    def update_case_for_person(
        session: Session,
        user_context: UserContext,
        client: ETLClient,
        action: CaseUpdateActionType,
        comment: Optional[str] = None,
    ) -> CaseUpdate:
        """This method updates the case_updates table with the newly provided actions.

        Because the underlying table does not have foreign key constraints, independent
        validation must be provided before calling this method.
        """
        action_ts = user_context.now()
        last_version = serialize_client_case_version(action, client).to_json()
        officer_id = user_context.officer_id
        person_external_id = user_context.person_id(client)
        insert_statement = (
            insert(CaseUpdate)
            .values(
                person_external_id=person_external_id,
                officer_external_id=officer_id,
                state_code=client.state_code,
                action_type=action.value,
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

        return (
            session.query(CaseUpdate)
            .filter(
                CaseUpdate.person_external_id == person_external_id,
                CaseUpdate.officer_external_id == officer_id,
                CaseUpdate.action_type == action.value,
            )
            .one()
        )

    @staticmethod
    def delete_case_update(
        session: Session, user_context: UserContext, update_id: str
    ) -> CaseUpdate:
        officer_external_id = user_context.officer_id
        try:
            case_update = (
                session.query(CaseUpdate)
                .filter(
                    CaseUpdate.officer_external_id == officer_external_id,
                    CaseUpdate.update_id == update_id,
                )
                .one()
            )
        except sqlalchemy.orm.exc.NoResultFound as e:
            raise CaseUpdateDoesNotExistError(
                f"Could not find update for officer: {officer_external_id} update_id: {update_id}"
            ) from e

        session.delete(case_update)
        session.commit()

        return case_update
