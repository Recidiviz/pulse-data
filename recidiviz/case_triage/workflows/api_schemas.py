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
import re
from typing import Dict, List

from marshmallow import Schema, ValidationError, fields
from marshmallow_enum import EnumField

from recidiviz.case_triage.api_schemas_utils import (
    CamelCaseSchema,
    CamelOrSnakeCaseSchema,
)
from recidiviz.case_triage.workflows.constants import WorkflowsUsTnVotersRightsCode
from recidiviz.common.constants.states import StateCode


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
    staff_id = fields.Str(required=True)
    contact_note_date_time = fields.DateTime(required=True)
    contact_note = fields.Dict(
        keys=fields.Integer(),
        values=fields.List(fields.Str),
        validate=valid_us_tn_contact_note,
        required=True,
    )
    voters_rights_code = EnumField(WorkflowsUsTnVotersRightsCode, by_value=True)
    should_queue_task = fields.Boolean(load_default=True, load_only=True)


class ProxySchema(Schema):
    url_secret = fields.Str(required=True)
    method = fields.Str(required=True)
    headers = fields.Dict()
    json = fields.Dict()
    timeout = fields.Integer(load_default=360)


def validate_phone_number(phone_number: str) -> None:
    """
    Validates that a string is a 10-digit number without an area code that starts with a 0 or 1
    e.g. 2223334444 is allowed and 1112223333 or 0112223333 are not
    """
    if not re.match(r"[2-9]\d{9}", phone_number):
        raise ValidationError(f"{phone_number} is not a valid US phone number")


class WorkflowsEnqueueSmsRequestSchema(CamelOrSnakeCaseSchema):
    """
    The schema expected by the /workflows/external_request/<state>/enqueue_sms_request
    Camel-cased keys are expected since the request is coming from the dashboards app

    message: The string text message that will be sent through twilio
    recipient_phone_number: The 10-digit phone number for the recipient
    recipient_external_id: The pseudonymized ID for the recipient
    sender_id: The email address of the user who is sending the message
    """

    message = fields.Str(required=True)
    recipient_phone_number = fields.Str(required=True, validate=validate_phone_number)
    recipient_external_id = fields.Str(required=True)
    sender_id = fields.Str(required=True)
    user_hash = fields.Str(required=True)


def validate_e164_phone_number(phone_number: str) -> None:
    """
    Validates that a string matches E.164 schema for a US phone number
    e.g. +12223334444
    """
    if not re.match(r"\+1\d{10}", phone_number):
        raise ValidationError(
            f"{phone_number} is not a valid US phone number using E.164"
        )


class WorkflowsSendSmsRequestSchema(CamelOrSnakeCaseSchema):
    """
    The schema expected by the /workflows/external_request/<state>/send_sms_request.
    """

    message = fields.Str(required=True)
    recipient_external_id = fields.Str(required=True)
    recipient = fields.Str(required=True, validate=validate_e164_phone_number)
    client_firestore_id = fields.Str(required=True)


class WorkflowsUsNdUpdateDocstarsEarlyTerminationDateSchema(CamelOrSnakeCaseSchema):
    """
    The schema expected by the /workflows/external_request/US_ND/update_docstars_early_termination_date.
    Camel-cased keys are expected since the request is coming from the dashboards app
    """

    class JustificationReasonSchema(Schema):
        code = fields.Str(required=True)
        description = fields.Str(required=True)

    person_external_id = fields.Integer(required=True)
    user_email = fields.Str(required=True)
    early_termination_date = fields.Date(required=True)
    justification_reasons = fields.List(fields.Nested(JustificationReasonSchema))
    should_queue_task = fields.Boolean(load_default=True, load_only=True)


class WorkflowsConfigurationsResponseSchema(CamelCaseSchema):
    """
    The schema returned by the /workflows/<state>/opportunities endpoint
    """

    class ConfigSchema(CamelCaseSchema):
        class SnoozeConfigSchema(CamelCaseSchema):
            default_snooze_days = fields.Integer()
            max_snooze_days = fields.Integer()

        class CriteriaCopySchema(CamelCaseSchema):
            text = fields.Str()
            tooltip = fields.Str(required=False)

        state_code = fields.Enum(StateCode)
        system_type = fields.Str()
        url_section = fields.Str()
        display_name = fields.Str()
        feature_variant = fields.Str(required=False)
        dynamic_eligibility_text = fields.Str()
        call_to_action = fields.Str()
        firestore_collection = fields.Str()
        methodology_url = fields.Str()
        snooze = fields.Nested(SnoozeConfigSchema(), required=False)
        denial_reasons = fields.Dict(fields.Str(), fields.Str())
        denial_text = fields.Str(required=False)
        initial_header = fields.Str()
        eligible_criteria_copy = fields.Dict(
            fields.Str(), fields.Nested(CriteriaCopySchema())
        )
        ineligible_criteria_copy = fields.Dict(
            fields.Str(), fields.Nested(CriteriaCopySchema())
        )
        sidebar_components = fields.List(fields.Str())

    # TODO(#27835): Make opportunity types top-level instead of nested one deep
    enabled_configs = fields.Dict(fields.Str(), fields.Nested(ConfigSchema()))
