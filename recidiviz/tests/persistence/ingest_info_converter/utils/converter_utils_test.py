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
"""Tests for conversion_utils.py"""

import datetime
from unittest import TestCase

from mock import patch

from recidiviz.persistence.ingest_info_converter.utils import converter_utils

_NOW = datetime.datetime(2000, 1, 1)


class TestConverterUtils(TestCase):
    """Test conversion util methods."""

    @patch(
        "recidiviz.persistence.ingest_info_converter.utils.converter_utils."
        "datetime.datetime"
    )
    def test_parseAge(self, mock_datetime):
        mock_datetime.now.return_value = _NOW

        expected_birthdate = datetime.date(year=_NOW.date().year - 1000, month=1, day=1)
        assert (
            converter_utils.calculate_birthdate_from_age("1000") == expected_birthdate
        )

    def test_parseBadAge(self):
        with self.assertRaises(ValueError):
            converter_utils.calculate_birthdate_from_age("ABC")
