# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2023 Recidiviz, Inc.
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
""" Contains Marshmallow schemas for Workflows API """
from typing import Dict, List

from marshmallow import ValidationError, fields
from marshmallow_enum import EnumField

from recidiviz.case_triage.api_schemas_utils import CamelOrSnakeCaseSchema
from recidiviz.case_triage.workflows.constants import WorkflowsUsTnVotersRightsCode


def valid_us_tn_contact_note(data: Dict[int, List[str]]) -> None:
    """
    Validates the shape of the contact note for US_TN.

    The note should be a dictionary, where the key is an integer representing the page number and
    the value is a list of strings (each string is a line in the page).

    External system requirements to be met:
    - Pages must be numbered 1-10.
    - Each page can have a maximum of 10 lines.
    - Each line must be <= 70 characters.

    Since we might receive a subset of pages for a note, i.e. if some pages failed and others succeeded, the
    dictionary structure enables us to specify the page number.
    """
    if not data:
        raise ValidationError("Note must be non-empty")

    page_numbers = list(data.keys())
    page_numbers.sort()

    if max(page_numbers) > 10 or min(page_numbers) < 1:
        raise ValidationError("Page number provided is outside the 1-10 range.")

    for page_num in page_numbers:
        lines = data[page_num]
        if len(lines) > 10:
            raise ValidationError(f"Page {page_num} has too many lines, maximum is 10")

        line_too_long = any(len(line) > 70 for line in lines)
        if line_too_long:
            raise ValidationError(
                f"Line in page {page_num} has too many characters, maximum is 70"
            )


class WorkflowsUsTnInsertTEPEContactNoteSchema(CamelOrSnakeCaseSchema):
    """
    The schema expected by the /workflows/US_TN/insert_tepe_contact_note.
    Camel-cased keys are expected since the request is coming from the dashboards app
    """

    person_external_id = fields.Str(required=True)
    user_id = fields.Str(required=True)
    contact_note_date_time = fields.DateTime(required=True)
    contact_note = fields.Dict(
        keys=fields.Integer(),
        values=fields.List(fields.Str),
        validate=valid_us_tn_contact_note,
        required=True,
    )
    voters_rights_code = EnumField(WorkflowsUsTnVotersRightsCode, by_value=True)
    should_queue_task = fields.Boolean(load_default=True, load_only=True)
