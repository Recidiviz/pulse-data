# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2026 Recidiviz, Inc.
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
"""Tests for us_ix_query_fragments.py"""
import unittest

from recidiviz.calculator.query.state.views.analyst_data.us_ix.us_ix_release_district_spans import (
    UsIxReleaseDistrict,
)
from recidiviz.task_eligibility.utils.us_ix_query_fragments import (
    UsIxPlacementMatrixFacility,
)


class TestUsIxPlacementMatrixFacility(unittest.TestCase):
    """Tests for UsIxPlacementMatrixFacility"""

    def test_drive_hours_cover_every_idaho_release_district(self) -> None:
        idaho_release_districts = set(UsIxReleaseDistrict) - {UsIxReleaseDistrict.ISC}
        for facility in UsIxPlacementMatrixFacility:
            with self.subTest(facility=facility):
                self.assertEqual(
                    idaho_release_districts,
                    set(facility.drive_hours_by_release_district),
                )
