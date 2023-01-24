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
    WorkflowsUsTnHandleInsertTEPEContactNoteSchema,
    WorkflowsUsTnInsertTEPEContactNoteSchema,
)
from recidiviz.tests.case_triage.api_schemas_test import (
    SchemaTestCase,
    invalid_schema_test,
    valid_schema_test,
)

PERSON_EXTERNAL_ID = "123"
USER_ID = "456"
CONTACT_NOTE_DATE_TIME = "2000-12-30T00:00:00"


class WorkflowsUsTnInsertTEPEContactNoteSchemaTest(SchemaTestCase):
    """Tests for WorkflowsUsTnInsertTEPEContactNoteSchema"""

    schema = WorkflowsUsTnInsertTEPEContactNoteSchema

    test_valid_data = valid_schema_test(
        {
            "personExternalId": PERSON_EXTERNAL_ID,
            "userId": USER_ID,
            "contactNoteDateTime": CONTACT_NOTE_DATE_TIME,
            "contactNote": {1: ["line 1", "line 2"]},
            "votersRightsCode": "VRRE",
        }
    )

    test_incorrect_voters_rights_code = invalid_schema_test(
        {
            "personExternalId": PERSON_EXTERNAL_ID,
            "userId": USER_ID,
            "contactNoteDateTime": CONTACT_NOTE_DATE_TIME,
            "contactNote": {1: ["line 1", "line 2"]},
            "votersRightsCode": "VVVV",
        },
        ["voters_rights_code"],
    )

    test_missing_contact_note = invalid_schema_test(
        {
            "personExternalId": PERSON_EXTERNAL_ID,
            "userId": USER_ID,
            "contactNoteDateTime": CONTACT_NOTE_DATE_TIME,
        },
        ["contact_note"],
    )

    test_missing_person_id = invalid_schema_test(
        {
            "userId": USER_ID,
            "contactNoteDateTime": CONTACT_NOTE_DATE_TIME,
            "contactNote": {1: ["line 1", "line 2"]},
        },
        ["person_external_id"],
    )

    test_missing_user_id = invalid_schema_test(
        {
            "personExternalId": PERSON_EXTERNAL_ID,
            "contactNoteDateTime": CONTACT_NOTE_DATE_TIME,
            "contactNote": {1: ["line 1", "line 2"]},
            "votersRightsCode": "VRRE",
        },
        ["user_id"],
    )

    test_invalid_contact_note = invalid_schema_test(
        {
            "personExternalId": PERSON_EXTERNAL_ID,
            "userId": USER_ID,
            "contactNoteDateTime": CONTACT_NOTE_DATE_TIME,
            "contactNote": {"1": "line 1"},
            "votersRightsCode": "VRRE",
        },
        ["contact_note"],
    )

    test_too_many_lines_contact_note = invalid_schema_test(
        {
            "personExternalId": PERSON_EXTERNAL_ID,
            "userId": USER_ID,
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
            "userId": USER_ID,
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


class WorkflowsUsTnHandleInsertTEPEContactNoteSchemaTest(SchemaTestCase):
    """
    Tests for WorkflowsUsTnHandleInsertTEPEContactNoteSchema.
    Similar to the WorkflowsUsTnInsertTEPEContactNoteSchema, however, the handlers data is not in camel case.
    """

    camel_case = False
    schema = WorkflowsUsTnHandleInsertTEPEContactNoteSchema

    test_valid_data = valid_schema_test(
        {
            "person_external_id": PERSON_EXTERNAL_ID,
            "user_id": USER_ID,
            "contact_note_date_time": CONTACT_NOTE_DATE_TIME,
            "contact_note": {1: ["line 1", "line 2"]},
            "voters_rights_code": "VRRE",
        }
    )

    test_incorrect_voters_rights_code = invalid_schema_test(
        {
            "person_external_id": PERSON_EXTERNAL_ID,
            "user_id": USER_ID,
            "contact_note_date_time": CONTACT_NOTE_DATE_TIME,
            "contact_note": {1: ["line 1", "line 2"]},
            "voters_rights_code": "VVVV",
        },
        ["voters_rights_code"],
    )
