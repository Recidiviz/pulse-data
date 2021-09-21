# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2020 Recidiviz, Inc.
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

"""Tests for context_utils.py."""
import textwrap
from datetime import datetime
from unittest import TestCase

from recidiviz.reporting.context.context_utils import align_columns, format_date


class ContextUtilsTest(TestCase):
    """Tests for context_utils.py."""

    def test_format_date(self) -> None:
        date = datetime.strptime("20201205112344", "%Y%m%d%H%M%S")
        actual = format_date("20201205112344", current_format="%Y%m%d%H%M%S")
        self.assertEqual(datetime.strftime(date, "%m/%d/%Y"), actual)

    # pylint:disable=trailing-whitespace
    def test_align_columns(self) -> None:
        columns = [
            ["few char", "many characters", "a little"],
            ["1", "2", "3"],
            ["a longer one", "few char", "many"],
        ]

        expected = textwrap.dedent(
            """\
            few char         many characters     a little    
            1                2                   3           
            a longer one     few char            many        """
        )

        self.assertEqual(expected, align_columns(columns))
