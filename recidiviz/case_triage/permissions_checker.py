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
"""Implements basic permissions safeguards."""
from recidiviz.case_triage.user_context import UserContext
from recidiviz.persistence.database.schema.case_triage.schema import (
    ETLClient,
    OfficerNote,
)


class PermissionsChecker:
    @staticmethod
    def is_on_caseload(client: ETLClient, user_context: UserContext) -> bool:
        return user_context.should_see_demo or (
            user_context.current_user is not None
            and client.state_code == user_context.client_state_code(client)
            and client.supervising_officer_external_id == user_context.officer_id
        )

    @staticmethod
    def note_belongs_to(note: OfficerNote, user_context: UserContext) -> bool:
        return user_context.should_see_demo or (
            user_context.current_user is not None
            and note.state_code == user_context.officer_state_code
            and note.officer_external_id == user_context.officer_id
        )
