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

from datetime import datetime
from typing import Any, List, Optional

ten_lines_page = [
    "Line 1",
    "Line 2",
    "Line 3",
    "Line 4",
    "Line 5",
    "Line 6",
    "Line 7",
    "Line 8",
    "Line 9",
    "Line 10",
]

one_line_page = ["Line 1"]

empty_line_page = [""]

empty_page: List[str] = []

partial_empty_lines_page = ["Line 1", ""]

full_lines_page = [
    "TEXT TO FILL 70 CHARACTERS. TEXT TO FILL 70 CHARACTERS. TEXT TO FILL 7",
    "TEXT TO FILL 70 CHARACTERS. TEXT TO FILL 70 CHARACTERS. TEXT TO FILL 7",
    "TEXT TO FILL 70 CHARACTERS. TEXT TO FILL 70 CHARACTERS. TEXT TO FILL 7",
    "TEXT TO FILL 70 CHARACTERS. TEXT TO FILL 70 CHARACTERS. TEXT TO FILL 7",
    "TEXT TO FILL 70 CHARACTERS. TEXT TO FILL 70 CHARACTERS. TEXT TO FILL 7",
    "TEXT TO FILL 70 CHARACTERS. TEXT TO FILL 70 CHARACTERS. TEXT TO FILL 7",
    "TEXT TO FILL 70 CHARACTERS. TEXT TO FILL 70 CHARACTERS. TEXT TO FILL 7",
    "TEXT TO FILL 70 CHARACTERS. TEXT TO FILL 70 CHARACTERS. TEXT TO FILL 7",
    "TEXT TO FILL 70 CHARACTERS. TEXT TO FILL 70 CHARACTERS. TEXT TO FILL 7",
    "TEXT TO FILL 70 CHARACTERS. TEXT TO FILL 70 CHARACTERS. TEXT TO FILL 7",
]

too_many_lines_page = [
    "TEXT TO FILL 70 CHARACTERS. TEXT TO FILL 70 CHARACTERS. TEXT TO FILL 7",
    "TEXT TO FILL 70 CHARACTERS. TEXT TO FILL 70 CHARACTERS. TEXT TO FILL 7",
    "TEXT TO FILL 70 CHARACTERS. TEXT TO FILL 70 CHARACTERS. TEXT TO FILL 7",
    "TEXT TO FILL 70 CHARACTERS. TEXT TO FILL 70 CHARACTERS. TEXT TO FILL 7",
    "TEXT TO FILL 70 CHARACTERS. TEXT TO FILL 70 CHARACTERS. TEXT TO FILL 7",
    "TEXT TO FILL 70 CHARACTERS. TEXT TO FILL 70 CHARACTERS. TEXT TO FILL 7",
    "TEXT TO FILL 70 CHARACTERS. TEXT TO FILL 70 CHARACTERS. TEXT TO FILL 7",
    "TEXT TO FILL 70 CHARACTERS. TEXT TO FILL 70 CHARACTERS. TEXT TO FILL 7",
    "TEXT TO FILL 70 CHARACTERS. TEXT TO FILL 70 CHARACTERS. TEXT TO FILL 7",
    "TEXT TO FILL 70 CHARACTERS. TEXT TO FILL 70 CHARACTERS. TEXT TO FILL 7",
    "TEXT TO FILL 70 CHARACTERS. TEXT TO FILL 70 CHARACTERS. TEXT TO FILL 7",
]

line_too_long_page = [
    "Line 1 is good.",
    "TEXT TO FILL 71 CHARACTERS. TEXT TO FILL 71 CHARACTERS. TEXT TO FILL 71",
    "Line 3 is good.",
]

invalid_characters_page = [
    "`Line 1~",
]

page_name_to_page = {
    "ten_lines": ten_lines_page,
    "one_line": one_line_page,
    "empty_line": empty_line_page,
    "empty_page": empty_page,
    "partial_empty_lines": partial_empty_lines_page,
    "full_lines": full_lines_page,
    "too_many_lines": too_many_lines_page,
    "invalid_characters_page_2_request": line_too_long_page,
    "line_too_long": line_too_long_page,
    "invalid_characters": invalid_characters_page,
}

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


invalid_characters_request_obj = {
    "ContactTypeCode1": "TEPE",
    "ContactSequenceNumber": 1,
    "Comment1": "~Test~",
}

line_too_long_request_obj = {
    "ContactTypeCode1": "TEPE",
    "ContactSequenceNumber": 1,
    "Comment1": "TEXT TO FILL 71 CHARACTERS. TEXT TO FILL 71 CHARACTERS. TEXT TO FILL 71",
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
    "invalid_characters_request": [invalid_characters_request_obj],
    "invalid_characters_page_2_request": [
        incomplete_request_obj,
        with_sequence_number(invalid_characters_request_obj, 2),
    ],
    "line_too_long_request": [line_too_long_request_obj],
    "out_of_order_request": [
        incomplete_request_obj,
        with_sequence_number(incomplete_request_obj, 3),
        with_sequence_number(incomplete_request_obj, 2),
    ],
    "missing_offenderid_request": [incomplete_request_obj],
    "missing_userid_request": [incomplete_request_obj],
}


def build_notes(
    notes: List[dict[str, Any]],
    request_datetime: datetime,
    offender_id: Optional[str],
    user_id: Optional[str],
) -> List[dict[str, Any]]:
    transformed_notes: List[dict[str, Any]] = []
    for note in notes:
        transformed_note = note.copy()
        transformed_note["ContactNoteDateTime"] = request_datetime.isoformat()
        if offender_id:
            transformed_note["OffenderId"] = offender_id
        if user_id:
            transformed_note["UserId"] = user_id
        transformed_notes.append(transformed_note)

    return transformed_notes
