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

from recidiviz.common.constants.state.state_incarceration_period import (
    StateIncarcerationPeriodAdmissionReason,
    StateIncarcerationPeriodReleaseReason,
    StateSpecializedPurposeForIncarceration,
)
from recidiviz.common.constants.state.state_supervision_contact import (
    StateSupervisionContactLocation,
)
from recidiviz.common.constants.state.state_supervision_period import (
    StateSupervisionLevel,
    StateSupervisionPeriodAdmissionReason,
    StateSupervisionPeriodSupervisionType,
    StateSupervisionPeriodTerminationReason,
)
from recidiviz.common.constants.states import StateCode
from recidiviz.ingest.direct.regions.us_pa.us_pa_controller import UsPaController
from recidiviz.ingest.direct.regions.us_pa.us_pa_enum_helpers import (
    incarceration_period_admission_reason_mapper,
    incarceration_period_purpose_mapper,
    incarceration_period_release_reason_mapper,
    supervision_contact_location_mapper,
)
from recidiviz.tests.ingest.direct.fixture_util import direct_ingest_fixture_path


class TestUsPaEnumMapperFunctions(unittest.TestCase):
    """Tests the mapper functions from the us_pa_enum_helpers file
    which parse raw text from the incarceration period ingest view"""

    def setUp(self) -> None:
        self.region_code = StateCode.US_PA.value.lower()

    def test_incarceration_period_admission_reason_mapper(self) -> None:
        fixture_path = direct_ingest_fixture_path(
            region_code=self.region_code, file_name="admission_reason_raw_text.csv"
        )
        with open(fixture_path, "r", encoding="utf-8") as f:
            while True:
                admission_reason_str = f.readline().strip()
                if not admission_reason_str:
                    break
                mapping = incarceration_period_admission_reason_mapper(
                    admission_reason_str
                )
                self.assertIsNotNone(mapping)
                self.assertIsInstance(mapping, StateIncarcerationPeriodAdmissionReason)

    def test_incarceration_period_release_reason_mapper(self) -> None:
        fixture_path = direct_ingest_fixture_path(
            region_code=self.region_code, file_name="release_reason_raw_text.csv"
        )
        with open(fixture_path, "r", encoding="utf-8") as f:
            while True:
                release_reason_str = f.readline().strip()
                if not release_reason_str:
                    break
                mapping = incarceration_period_release_reason_mapper(release_reason_str)
                self.assertIsNotNone(mapping)
                self.assertIsInstance(mapping, StateIncarcerationPeriodReleaseReason)

    def test_incarceration_period_purpose_mapper(self) -> None:
        fixture_path = direct_ingest_fixture_path(
            region_code=self.region_code, file_name="purpose_raw_text.csv"
        )
        with open(fixture_path, "r", encoding="utf-8") as f:
            while True:
                purpose_str = f.readline().strip()
                if not purpose_str:
                    break
                mapping = incarceration_period_purpose_mapper(purpose_str)
                self.assertIsNotNone(mapping)
                self.assertIsInstance(mapping, StateSpecializedPurposeForIncarceration)

    def test_supervision_level_parsing(self) -> None:
        enum_overrides = UsPaController.generate_enum_overrides()
        fixture_path = direct_ingest_fixture_path(
            region_code=self.region_code, file_name="supervision_level_raw_text.csv"
        )
        with open(fixture_path, "r", encoding="utf-8") as f:
            while True:
                supervision_level_str = f.readline().strip()
                if not supervision_level_str:
                    break
                _ = StateSupervisionLevel.parse(supervision_level_str, enum_overrides)

    def test_supervision_period_supervision_type_parsing(self) -> None:
        enum_overrides = UsPaController.generate_enum_overrides()
        fixture_path = direct_ingest_fixture_path(
            region_code=self.region_code,
            file_name="supervision_period_supervision_type_raw_text.csv",
        )
        with open(fixture_path, "r", encoding="utf-8") as f:
            while True:
                supervision_type_str = f.readline().strip()
                if not supervision_type_str:
                    break
                _ = StateSupervisionPeriodSupervisionType.parse(
                    supervision_type_str, enum_overrides
                )

    def test_supervision_period_admission_reason_parsing(self) -> None:
        enum_overrides = UsPaController.generate_enum_overrides()
        fixture_path = direct_ingest_fixture_path(
            region_code=self.region_code,
            file_name="supervision_period_admission_reason_raw_text.csv",
        )
        with open(fixture_path, "r", encoding="utf-8") as f:
            while True:
                supervision_type_str = f.readline().strip()
                if not supervision_type_str:
                    break
                _ = StateSupervisionPeriodAdmissionReason.parse(
                    supervision_type_str, enum_overrides
                )

    def test_supervision_period_termination_reason_parsing(self) -> None:
        enum_overrides = UsPaController.generate_enum_overrides()
        fixture_path = direct_ingest_fixture_path(
            region_code=self.region_code,
            file_name="supervision_period_termination_reason_raw_text.csv",
        )
        with open(fixture_path, "r", encoding="utf-8") as f:
            while True:
                supervision_type_str = f.readline().strip()
                if not supervision_type_str:
                    break
                _ = StateSupervisionPeriodTerminationReason.parse(
                    supervision_type_str, enum_overrides
                )

    def test_supervision_contact_location_mapper(self) -> None:
        fixture_path = direct_ingest_fixture_path(
            region_code=self.region_code,
            file_name="supervision_contacts_location_raw_text.csv",
        )
        with open(fixture_path, "r", encoding="utf-8") as f:
            while True:
                supervision_contact_location_str = f.readline().strip()
                if not supervision_contact_location_str:
                    break
                mapping = supervision_contact_location_mapper(
                    supervision_contact_location_str
                )
                self.assertIsNotNone(mapping)
                self.assertIsInstance(mapping, StateSupervisionContactLocation)
