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

complete_request_obj = {
    "ContactNoteDateTime": "2022-10-01T13:31:34.558Z",
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

incomplete_request_obj = {
    "ContactNoteDateTime": "2022-10-02T13:31:34.558Z",
    "ContactTypeCode1": "TEPE",
    "ContactSequenceNumber": 1,
    "Comment1": "Test 1",
}


empty_comments_request_obj = {
    "ContactNoteDateTime": "2022-10-03T13:31:34.558Z",
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

note_name_to_obj = {
    "complete_request": complete_request_obj,
    "incomplete_request": incomplete_request_obj,
    "empty_comments_request": empty_comments_request_obj,
}
