# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2026 Recidiviz, Inc.
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
"""Shared request data model for contact note writebacks."""
from datetime import datetime

from recidiviz.case_triage.workflows.writeback.base import WritebackRequestData


class ContactNoteRequestData(WritebackRequestData):
    """Common fields shared across all state contact note writebacks.

    State-specific subclasses extend this with a ``state_code`` Literal field
    (used as the discriminator) and additional fields for the note content.
    """

    state_code: str
    person_external_id: str
    person_external_id_type: str
    staff_id: str
    staff_id_type: str
    contact_note_date_time: datetime
