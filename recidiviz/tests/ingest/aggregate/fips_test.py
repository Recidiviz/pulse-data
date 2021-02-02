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
"""Tests for fips manipulation."""
from unittest import TestCase

import pandas as pd
from pandas.testing import assert_frame_equal
import us

from recidiviz.common import fips
from recidiviz.common.errors import FipsMergingError

_DUPAGE_COUNTY_FIPS = '17043'  # "DuPage County"
_EFFINGHAM_COUNTY_FIPS = '17049'  # "Effingham County"
_JO_DAVIESS_COUNTY_FIPS = '17085'  # "Jo Daviess County"


class TestFips(TestCase):
    """Tests for fips manipulation."""

    def testValidCountyNames_SanitizesFieldsAndFuzzyJoinsFips(self):
        # Arrange
        subject = pd.DataFrame({
            'county': [
                'DuPage',  # No County suffix
                'Efingham County',  # Spelled incorrect: Efingham -> Effingham
                'Jo Daviess County']  # Exact match
        })

        # Act
        result = fips.add_column_to_df(subject, subject.county, us.states.IL)

        # Assert
        expected_result = pd.DataFrame({
            'county': [
                'DuPage',
                'Efingham County',
                'Jo Daviess County'],
            'fips': [
                _DUPAGE_COUNTY_FIPS,
                _EFFINGHAM_COUNTY_FIPS,
                _JO_DAVIESS_COUNTY_FIPS
            ]
        })

        assert_frame_equal(result, expected_result)

    def testCountyNotInState_RaisesFipsMergingError(self):
        # Arrange
        subject = pd.DataFrame({'county': ['Jo Daviess County']})

        # Act/Assert
        with self.assertRaises(FipsMergingError):
            incorrect_state = us.states.FL
            fips.add_column_to_df(subject, subject.county, incorrect_state)

    def testStateWithNoFipsMappings_RaisesFipsMergingError(self):
        # Arrange
        class FakeState:
            fips = '123456789'  # Non-existing state fips maps to no county fips

        subject = pd.DataFrame({'county': ['Jo Daviess County']})

        # Act/Assert
        with self.assertRaises(FipsMergingError):
            fips.add_column_to_df(subject, subject.county, FakeState)
