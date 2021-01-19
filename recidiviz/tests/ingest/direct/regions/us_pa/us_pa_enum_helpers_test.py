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
"""Tests the functions in the us_pa_enum_helpers file."""
import unittest
from recidiviz.common.constants.state.state_incarceration_period import StateIncarcerationPeriodAdmissionReason, \
    StateIncarcerationPeriodReleaseReason, StateSpecializedPurposeForIncarceration
from recidiviz.ingest.direct.regions.us_pa.us_pa_enum_helpers import incarceration_period_admission_reason_mapper, \
    incarceration_period_release_reason_mapper, incarceration_period_purpose_mapper
from recidiviz.tests.ingest import fixtures


class TestSetDateSpecificAssessmentLevel(unittest.TestCase):
    """Tests the mapper functions from the us_pa_enum_helpers file
    which parse raw text from the incarceration period ingest view"""
    def test_incarceration_period_admission_reason_mapper(self) -> None:
        fixture_path = fixtures.as_filepath('admission_reason_raw_text.csv')
        with open(fixture_path, 'r') as f:
            while True:
                admission_reason_str = f.readline().strip()
                if not admission_reason_str:
                    break
                mapping = incarceration_period_admission_reason_mapper(admission_reason_str)
                self.assertIsNotNone(mapping)
                self.assertIsInstance(mapping, StateIncarcerationPeriodAdmissionReason)

    def test_incarceration_period_release_reason_mapper(self) -> None:
        fixture_path = fixtures.as_filepath('release_reason_raw_text.csv')
        with open(fixture_path, 'r') as f:
            while True:
                release_reason_str = f.readline().strip()
                if not release_reason_str:
                    break
                mapping = incarceration_period_release_reason_mapper(release_reason_str)
                self.assertIsNotNone(mapping)
                self.assertIsInstance(mapping, StateIncarcerationPeriodReleaseReason)

    def test_incarceration_period_purpose_mapper(self) -> None:
        fixture_path = fixtures.as_filepath('purpose_raw_text.csv')
        with open(fixture_path, 'r') as f:
            while True:
                purpose_str = f.readline().strip()
                if not purpose_str:
                    break
                mapping = incarceration_period_purpose_mapper(purpose_str)
                self.assertIsNotNone(mapping)
                self.assertIsInstance(mapping, StateSpecializedPurposeForIncarceration)
