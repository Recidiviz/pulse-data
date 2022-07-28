# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2019 Recidiviz, Inc.
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
"""Tests for validating ingest_info protos."""

import unittest

from recidiviz.ingest.models.ingest_info_pb2 import IngestInfo, StatePerson
from recidiviz.persistence.ingest_info_validator import ingest_info_validator
from recidiviz.persistence.ingest_info_validator.ingest_info_validator import (
    ValidationError,
)

PERSON_1 = "PERSON_ID_1"
EXTRA_PERSON = "EXTRA_PERSON_ID"


class TestValidator(unittest.TestCase):
    """Test ingest_info validation."""

    def test_empty_ingest_info(self) -> None:
        ingest_info_validator.validate(IngestInfo())

    def test_non_empty_ingest_info(self) -> None:
        ingest_info = IngestInfo()
        ingest_info.state_people.extend(
            [
                StatePerson(state_person_id=PERSON_1),
                StatePerson(state_person_id=EXTRA_PERSON),
            ]
        )
        ingest_info_validator.validate(ingest_info)

    def test_duplicate_ids(self) -> None:
        ingest_info = IngestInfo()
        ingest_info.state_people.extend(
            [
                StatePerson(state_person_id=PERSON_1),
                StatePerson(state_person_id=PERSON_1),
            ]
        )

        # Act
        with self.assertRaises(ValidationError) as e:
            ingest_info_validator.validate(ingest_info)
        result = e.exception.errors

        # Assert
        expected_result = {
            "state_people": {"duplicate_ids": {PERSON_1}},
        }

        self.assertEqual(result, expected_result)
