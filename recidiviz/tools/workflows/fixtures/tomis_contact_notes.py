# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2022 Recidiviz, Inc.
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
"""
Predefined request bodies to use in test requests to TOMIS via recidiviz.tools.workflows.request_api
"""

from datetime import datetime, timedelta
from typing import Any, List, Optional

complete_request_basic_obj = {
    "ContactTypeCode1": "TEPE",
    "ContactTypeCode2": "",
    "ContactTypeCode3": "",
    "ContactTypeCode4": "",
    "ContactTypeCode5": "",
    "ContactTypeCode6": "",
    "ContactTypeCode7": "",
    "ContactTypeCode8": "",
    "ContactTypeCode9": "",
    "ContactTypeCode10": "",
    "ContactSequenceNumber": 1,
    "Comment1": "Test 1",
    "Comment2": "Test 2",
    "Comment3": "Test 3",
    "Comment4": "Test 4",
    "Comment5": "Test 5",
    "Comment6": "Test 6",
    "Comment7": "Test 7",
    "Comment8": "Test 8",
    "Comment9": "Test 9",
    "Comment10": "Test 10",
}

complete_request_full_obj = {
    "ContactTypeCode1": "TEPE",
    "ContactTypeCode2": "",
    "ContactTypeCode3": "",
    "ContactTypeCode4": "",
    "ContactTypeCode5": "",
    "ContactTypeCode6": "",
    "ContactTypeCode7": "",
    "ContactTypeCode8": "",
    "ContactTypeCode9": "",
    "ContactTypeCode10": "",
    "ContactSequenceNumber": 1,
    "Comment1": "TEXT TO FILL 70 CHARACTERS. TEXT TO FILL 70 CHARACTERS. TEXT TO FILL 7",
    "Comment2": "TEXT TO FILL 70 CHARACTERS. TEXT TO FILL 70 CHARACTERS. TEXT TO FILL 7",
    "Comment3": "TEXT TO FILL 70 CHARACTERS. TEXT TO FILL 70 CHARACTERS. TEXT TO FILL 7",
    "Comment4": "TEXT TO FILL 70 CHARACTERS. TEXT TO FILL 70 CHARACTERS. TEXT TO FILL 7",
    "Comment5": "TEXT TO FILL 70 CHARACTERS. TEXT TO FILL 70 CHARACTERS. TEXT TO FILL 7",
    "Comment6": "TEXT TO FILL 70 CHARACTERS. TEXT TO FILL 70 CHARACTERS. TEXT TO FILL 7",
    "Comment7": "TEXT TO FILL 70 CHARACTERS. TEXT TO FILL 70 CHARACTERS. TEXT TO FILL 7",
    "Comment8": "TEXT TO FILL 70 CHARACTERS. TEXT TO FILL 70 CHARACTERS. TEXT TO FILL 7",
    "Comment9": "TEXT TO FILL 70 CHARACTERS. TEXT TO FILL 70 CHARACTERS. TEXT TO FILL 7",
    "Comment10": "TEXT TO FILL 70 CHARACTERS. TEXT TO FILL 70 CHARACTERS. TEXT TO FILL 7",
}

incomplete_request_obj = {
    "ContactTypeCode1": "TEPE",
    "ContactSequenceNumber": 1,
    "Comment1": "Test 1",
}


empty_comments_request_obj = {
    "ContactTypeCode1": "TEPE",
    "ContactTypeCode2": "",
    "ContactTypeCode3": "",
    "ContactTypeCode4": "",
    "ContactTypeCode5": "",
    "ContactTypeCode6": "",
    "ContactTypeCode7": "",
    "ContactTypeCode8": "",
    "ContactTypeCode9": "",
    "ContactTypeCode10": "",
    "ContactSequenceNumber": 1,
    "Comment1": "Test 1",
    "Comment2": "",
    "Comment3": "",
    "Comment4": "",
    "Comment5": "",
    "Comment6": "",
    "Comment7": "",
    "Comment8": "",
    "Comment9": "",
    "Comment10": "",
}


def with_sequence_number(note: dict[str, Any], sequence_number: int) -> dict[str, Any]:
    cp = note.copy()
    cp["ContactSequenceNumber"] = sequence_number
    return cp


note_name_to_objs = {
    "complete_request": [complete_request_basic_obj],
    "complete_request_full": [complete_request_full_obj],
    "incomplete_request": [incomplete_request_obj],
    "empty_comments_request": [empty_comments_request_obj],
    "two_page_request": [
        complete_request_full_obj,
        with_sequence_number(complete_request_full_obj, 2),
    ],
    "ten_page_request": [
        with_sequence_number(complete_request_full_obj, i) for i in range(1, 11)
    ],
}

_START_DATE = datetime(2022, 10, 1)


def build_notes(
    notes: List[dict[str, Any]],
    test_case_number: int,
    offender_id: Optional[str],
    user_id: Optional[str],
) -> List[dict[str, Any]]:
    transformed_notes: List[dict[str, Any]] = []
    for note in notes:
        transformed_note = note.copy()
        transformed_note["ContactNoteDateTime"] = (
            _START_DATE + timedelta(days=test_case_number)
        ).isoformat()
        if offender_id:
            transformed_note["OffenderId"] = offender_id
        if user_id:
            transformed_note["UserId"] = user_id
        transformed_notes.append(transformed_note)

    return transformed_notes
