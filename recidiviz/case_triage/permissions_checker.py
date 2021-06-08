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


from recidiviz.persistence.database.schema.case_triage.schema import (
    ETLClient,
    ETLOfficer,
    OfficerNote,
)


class PermissionsChecker:
    @staticmethod
    def is_on_caseload(client: ETLClient, officer: ETLOfficer) -> bool:
        return (
            client.state_code == officer.state_code
            and client.supervising_officer_external_id == officer.external_id
        )

    @staticmethod
    def note_belongs_to(note: OfficerNote, officer: ETLOfficer) -> bool:
        return (
            note.state_code == officer.state_code
            and note.officer_external_id == officer.external_id
        )
