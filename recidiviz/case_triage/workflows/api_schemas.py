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
from typing import Any, Dict, List

from marshmallow import Schema, ValidationError, fields, validates_schema

from recidiviz.case_triage.api_schemas_utils import (
    CamelCaseSchema,
    CamelOrSnakeCaseSchema,
)
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
    staff_id = fields.Str(required=True)
    contact_note_date_time = fields.DateTime(required=True)
    contact_note = fields.Dict(
        keys=fields.Integer(),
        values=fields.List(fields.Str),
        validate=valid_us_tn_contact_note,
        required=True,
    )
    voters_rights_code = fields.Enum(WorkflowsUsTnVotersRightsCode, by_value=True)
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


class WorkflowsConfigSchema(CamelCaseSchema):
    """
    Schema used to represent a Workflows opportunity config. Also used for the admin panel
    """

    class SnoozeConfigSchema(CamelCaseSchema):
        class AutoSnoozeParamsSchema(CamelCaseSchema):
            type = fields.Str(required=True)
            params = fields.Dict(fields.Str(), fields.Raw(), required=True)

        default_snooze_days = fields.Integer(required=False)
        max_snooze_days = fields.Integer(required=False)
        auto_snooze_params = fields.Nested(AutoSnoozeParamsSchema(), required=False)

        @validates_schema
        def validate_union(self, data: Dict, **_kwargs: Any) -> None:
            if "auto_snooze_params" in data:
                print(data)
                if "default_snooze_days" in data or "max_snooze_days" in data:
                    raise ValidationError("Snooze contains both manual and auto fields")
            else:
                if "default_snooze_days" not in data or "max_snooze_days" not in data:
                    raise ValidationError("Snooze is missing a required field")

    class CriteriaCopySchema(CamelCaseSchema):
        key = fields.Str()
        text = fields.Str()
        tooltip = fields.Str(required=False)

    class KeylessCriteriaCopySchema(CamelCaseSchema):
        text = fields.Str()
        tooltip = fields.Str(required=False)

    class DenialReasonsSchema(CamelCaseSchema):
        key = fields.Str()
        text = fields.Str()

    class TabGroupsSchema(CamelCaseSchema):
        key = fields.Str()
        tabs = fields.List(fields.Str())

    class SortParamSchema(CamelCaseSchema):
        field = fields.Str()
        sort_direction = fields.Str(required=False)
        undefined_behavior = fields.Str(required=False)

    class NotificationSchema(CamelCaseSchema):
        id = fields.Str()
        title = fields.Str(required=False)
        body = fields.Str()
        cta = fields.Str(required=False)

    class SubcategoryHeadingSchema(CamelCaseSchema):
        subcategory = fields.Str()
        text = fields.Str()

    class TabTextSchema(CamelCaseSchema):
        tab = fields.Str()
        text = fields.Str()

    class TabTextListSchema(CamelCaseSchema):
        tab = fields.Str()
        texts = fields.List(fields.Str())

    state_code = fields.Str()
    display_name = fields.Str()
    feature_variant = fields.Str(required=False)
    dynamic_eligibility_text = fields.Str()
    eligibility_date_text = fields.Str(required=False)
    hide_denial_revert = fields.Bool()
    tooltip_eligibility_text = fields.Str(required=False)
    call_to_action = fields.Str(required=False)
    subheading = fields.Str(required=False)
    snooze = fields.Nested(SnoozeConfigSchema(), required=False)
    denial_reasons = fields.List(fields.Nested(DenialReasonsSchema()))
    denial_text = fields.Str(required=False)
    initial_header = fields.Str(required=False)
    eligible_criteria_copy = fields.List(fields.Nested(CriteriaCopySchema()))
    ineligible_criteria_copy = fields.List(fields.Nested(CriteriaCopySchema()))
    sidebar_components = fields.List(fields.Str())
    methodology_url = fields.Str()
    is_alert = fields.Bool()
    priority = fields.Str()
    tab_groups = fields.List(fields.Nested(TabGroupsSchema()), required=False)
    compare_by = fields.List(fields.Nested(SortParamSchema()), required=False)
    notifications = fields.List(fields.Nested(NotificationSchema()))
    zero_grants_tooltip = fields.Str(required=False)
    denied_tab_title = fields.Str(required=False)
    denial_adjective = fields.Str(required=False)
    denial_noun = fields.Str(required=False)
    supports_submitted = fields.Bool()
    submitted_tab_title = fields.Str(required=False)
    empty_tab_copy = fields.List(fields.Nested(TabTextSchema))
    tab_preface_copy = fields.List(fields.Nested(TabTextSchema))
    subcategory_headings = fields.List(fields.Nested(SubcategoryHeadingSchema))
    subcategory_orderings = fields.List(fields.Nested(TabTextListSchema))
    mark_submitted_options_by_tab = fields.List(fields.Nested(TabTextListSchema))
    oms_criteria_header = fields.Str(required=False)
    non_oms_criteria_header = fields.Str(required=False)
    non_oms_criteria = fields.List(fields.Nested(KeylessCriteriaCopySchema()))
    highlight_cases_on_homepage = fields.Bool()
    highlighted_case_cta_copy = fields.Str(required=False)
    overdue_opportunity_callout_copy = fields.Str(required=False)
    snooze_companion_opportunity_types = fields.List(fields.Str(required=False))
    case_notes_title = fields.Str(required=False)


class WorkflowsFullConfigSchema(WorkflowsConfigSchema):
    """
    Configuration schema with base opportunity information added
    """

    system_type = fields.Str()
    url_section = fields.Str()
    firestore_collection = fields.Str()
    homepage_position = fields.Int()


class WorkflowsConfigurationsResponseSchema(CamelCaseSchema):
    """
    The schema returned by the /workflows/<state>/opportunities endpoint
    """

    # TODO(#27835): Make opportunity types top-level instead of nested one deep
    enabled_configs = fields.Dict(
        fields.Str(), fields.Nested(WorkflowsFullConfigSchema())
    )
