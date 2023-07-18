#  Recidiviz - a data platform for criminal justice reform
#  Copyright (C) 2023 Recidiviz, Inc.
#
#  This program is free software: you can redistribute it and/or modify
#  it under the terms of the GNU General Public License as published by
#  the Free Software Foundation, either version 3 of the License, or
#  (at your option) any later version.
#
#  This program is distributed in the hope that it will be useful,
#  but WITHOUT ANY WARRANTY; without even the implied warranty of
#  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
#  GNU General Public License for more details.
#
#  You should have received a copy of the GNU General Public License
#  along with this program.  If not, see <https://www.gnu.org/licenses/>.
#  =============================================================================
"""Implements tests for the Workflows API schemas."""
from recidiviz.case_triage.workflows.api_schemas import (
    WorkflowsEnqueueSmsRequestSchema,
    WorkflowsSendSmsRequestSchema,
    WorkflowsUsTnInsertTEPEContactNoteSchema,
)
from recidiviz.tests.case_triage.api_schemas_test_utils import (
    SchemaTestCase,
    invalid_schema_test,
    valid_schema_test,
)

PERSON_EXTERNAL_ID = "123"
STAFF_ID = "456"
CONTACT_NOTE_DATE_TIME = "2000-12-30T00:00:00"


class WorkflowsUsTnInsertTEPEContactNoteSchemaTest(SchemaTestCase):
    """Tests for WorkflowsUsTnInsertTEPEContactNoteSchema"""

    camel_case = False
    schema = WorkflowsUsTnInsertTEPEContactNoteSchema

    test_valid_data = valid_schema_test(
        {
            "personExternalId": PERSON_EXTERNAL_ID,
            "staffId": STAFF_ID,
            "contactNoteDateTime": CONTACT_NOTE_DATE_TIME,
            "contactNote": {1: ["line 1", "line 2"]},
            "votersRightsCode": "VRRE",
        }
    )

    test_valid_data_missing_voters_rights_code = valid_schema_test(
        {
            "personExternalId": PERSON_EXTERNAL_ID,
            "staffId": STAFF_ID,
            "contactNoteDateTime": CONTACT_NOTE_DATE_TIME,
            "contactNote": {1: ["line 1", "line 2"]},
        }
    )

    test_valid_data_snake_case = valid_schema_test(
        {
            "person_external_id": PERSON_EXTERNAL_ID,
            "staff_id": STAFF_ID,
            "contact_note_date_time": CONTACT_NOTE_DATE_TIME,
            "contact_note": {1: ["line 1", "line 2"]},
            "voters_rights_code": "VRRE",
        }
    )

    test_valid_data_snake_case_missing_voters_rights_code = valid_schema_test(
        {
            "person_external_id": PERSON_EXTERNAL_ID,
            "staff_id": STAFF_ID,
            "contact_note_date_time": CONTACT_NOTE_DATE_TIME,
            "contact_note": {1: ["line 1", "line 2"]},
        }
    )

    test_incorrect_voters_rights_code = invalid_schema_test(
        {
            "personExternalId": PERSON_EXTERNAL_ID,
            "staffId": STAFF_ID,
            "contactNoteDateTime": CONTACT_NOTE_DATE_TIME,
            "contactNote": {1: ["line 1", "line 2"]},
            "votersRightsCode": "VVVV",
        },
        ["voters_rights_code"],
    )

    test_missing_contact_note = invalid_schema_test(
        {
            "personExternalId": PERSON_EXTERNAL_ID,
            "staffId": STAFF_ID,
            "contactNoteDateTime": CONTACT_NOTE_DATE_TIME,
        },
        ["contact_note"],
    )

    test_missing_person_id = invalid_schema_test(
        {
            "staffId": STAFF_ID,
            "contactNoteDateTime": CONTACT_NOTE_DATE_TIME,
            "contactNote": {1: ["line 1", "line 2"]},
        },
        ["person_external_id"],
    )

    test_missing_staff_id = invalid_schema_test(
        {
            "personExternalId": PERSON_EXTERNAL_ID,
            "contactNoteDateTime": CONTACT_NOTE_DATE_TIME,
            "contactNote": {1: ["line 1", "line 2"]},
            "votersRightsCode": "VRRE",
        },
        ["staff_id"],
    )

    test_invalid_contact_note = invalid_schema_test(
        {
            "personExternalId": PERSON_EXTERNAL_ID,
            "staffId": STAFF_ID,
            "contactNoteDateTime": CONTACT_NOTE_DATE_TIME,
            "contactNote": {"1": "line 1"},
            "votersRightsCode": "VRRE",
        },
        ["contact_note"],
    )

    test_too_many_lines_contact_note = invalid_schema_test(
        {
            "personExternalId": PERSON_EXTERNAL_ID,
            "staffId": STAFF_ID,
            "contactNoteDateTime": CONTACT_NOTE_DATE_TIME,
            "contactNote": {
                1: [
                    "line 1",
                    "line 2",
                    "line 3",
                    "line 4",
                    "line 5",
                    "line 6",
                    "line 7",
                    "line 8",
                    "line 9",
                    "line 10",
                    "line 11",
                ]
            },
        },
        ["contact_note"],
    )

    test_invalid_page_num_contact_note = invalid_schema_test(
        {
            "personExternalId": PERSON_EXTERNAL_ID,
            "staffId": STAFF_ID,
            "contactNoteDateTime": CONTACT_NOTE_DATE_TIME,
            "contactNote": {
                11: [
                    "line 1",
                    "line 2",
                    "line 3",
                    "line 4",
                    "line 5",
                    "line 6",
                    "line 7",
                    "line 8",
                    "line 9",
                    "line 10",
                    "line 11",
                ]
            },
        },
        ["contact_note"],
    )


class WorkflowsSendSmsRequestSchemaTest(SchemaTestCase):
    """Tests for WorkflowsSendSmsRequestSchema"""

    camel_case = False
    schema = WorkflowsSendSmsRequestSchema

    VALID_NUMBER = "+12223334444"
    MID = "AAA-BBBB-XX-DD"

    test_valid_data = valid_schema_test(
        {
            "recipient": VALID_NUMBER,
            "mid": MID,
            "message": "Some pig!",
            "recipient_external_id": "123",
        }
    )

    test_missing_mid = invalid_schema_test(
        {
            "recipient": VALID_NUMBER,
            "message": "Baa Ram Ewe",
        }
    )

    test_invalid_phone_number = invalid_schema_test(
        {
            "recipient": "(555) 666-7777",
            "mid": MID,
            "message": "That'll do, pig. That'll do.",
        }
    )

    test_another_invalid_phone_number = invalid_schema_test(
        {
            "recipient": "15556667777",
            "mid": MID,
            "message": "That'll do, pig. That'll do.",
        }
    )

    test_missing_recipient_external_id = invalid_schema_test(
        {
            "recipient": VALID_NUMBER,
            "mid": MID,
            "message": "1112223333",
        }
    )


class WorkflowsEnqueueSmsRequestSchemaTest(SchemaTestCase):
    """Tests for WorkflowsEnqueueSmsRequestSchema"""

    camel_case = True
    schema = WorkflowsEnqueueSmsRequestSchema

    VALID_NUMBER = "2223334444"
    INVALID_NUMBER_PREFIXED_BY_1 = "12223334444"
    INVALID_NUMBER_AREA_CODE_STARTS_WITH_1 = "1223334444"
    INVALID_NUMBER_AREA_CODE_STARTS_WITH_0 = "0223334444"
    INVALID_NUMBER_FORMATTED = "(222) 333-4444"
    INVALID_NUMBER_E164 = "+12223334444"

    test_valid_data = valid_schema_test(
        {
            "recipient_phone_number": VALID_NUMBER,
            "message": "I must not fear",
            "recipient_external_id": "paul.atreides",
            "sender_id": "rev.mum.mohiam",
        }
    )

    test_invalid_number_prefixed_by_1 = invalid_schema_test(
        {
            "recipient_phone_number": INVALID_NUMBER_PREFIXED_BY_1,
            "message": "Fear is the mind-killer",
            "recipient_external_id": "paul.atreides",
            "sender_id": "rev.mum.mohiam",
        }
    )

    test_invalid_number_area_code_starts_with_1 = invalid_schema_test(
        {
            "recipient_phone_number": INVALID_NUMBER_AREA_CODE_STARTS_WITH_1,
            "message": "Fear is the little-death that brings total obliteration",
            "recipient_external_id": "paul.atreides",
            "sender_id": "rev.mum.mohiam",
        }
    )

    test_invalid_number_area_code_starts_with_0 = invalid_schema_test(
        {
            "recipient_phone_number": INVALID_NUMBER_AREA_CODE_STARTS_WITH_0,
            "message": "I will face my fear",
            "recipient_external_id": "paul.atreides",
            "sender_id": "rev.mum.mohiam",
        }
    )

    test_invalid_number_formatted = invalid_schema_test(
        {
            "recipient_phone_number": INVALID_NUMBER_FORMATTED,
            "message": "I will permit it to pass over me and through me",
            "recipient_external_id": "paul.atreides",
            "sender_id": "rev.mum.mohiam",
        }
    )

    test_invalid_number_e164 = invalid_schema_test(
        {
            "recipient_phone_number": INVALID_NUMBER_E164,
            "message": "And when it has gone past, I will turn the inner eye to see its path",
            "recipient_external_id": "paul.atreides",
            "sender_id": "rev.mum.mohiam",
        }
    )

    test_invalid_missing_sender_id = invalid_schema_test(
        {
            "recipient_phone_number": INVALID_NUMBER_AREA_CODE_STARTS_WITH_1,
            "message": "Where the fear has gone there will be nothing",
            "recipient_external_id": "paul.atreides",
        }
    )

    test_invalid_missing_recipient_id = invalid_schema_test(
        {
            "recipient_phone_number": INVALID_NUMBER_AREA_CODE_STARTS_WITH_1,
            "message": "Only I will remain",
            "sender_id": "rev.mum.mohiam",
        }
    )
