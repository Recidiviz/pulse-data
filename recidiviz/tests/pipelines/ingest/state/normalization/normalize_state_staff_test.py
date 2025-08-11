# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2024 Recidiviz, Inc.
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
"""Tests for normalize_state_staff.py"""
import datetime
import unittest

from recidiviz.common.constants.states import StateCode
from recidiviz.persistence.entity.entity_utils import get_all_entity_classes_in_module
from recidiviz.persistence.entity.state import entities, normalized_entities
from recidiviz.persistence.entity.state.normalized_entities import NormalizedStateStaff
from recidiviz.pipelines.ingest.state.normalization import normalize_state_staff
from recidiviz.pipelines.ingest.state.normalization.normalize_state_staff import (
    build_normalized_state_staff,
)
from recidiviz.tests.persistence.entity.state.entities_test_utils import (
    generate_full_graph_state_staff,
)
from recidiviz.tests.pipelines.fake_state_calculation_config_manager import (
    start_pipeline_delegate_getter_patchers,
)


class TestNormalizeStateStaff(unittest.TestCase):
    """Tests for normalize_state_staff.py"""

    def setUp(self) -> None:
        self.delegate_patchers = start_pipeline_delegate_getter_patchers(
            normalize_state_staff
        )

    def tearDown(self) -> None:
        for patcher in self.delegate_patchers:
            patcher.stop()

    def test_build_normalized_state_person_full_tree(self) -> None:
        pre_normalization_staff = generate_full_graph_state_staff(
            set_back_edges=True, set_ids=True
        )

        normalized_staff = build_normalized_state_staff(
            staff=pre_normalization_staff,
            staff_external_id_to_staff_id={("S1", "SUPERVISOR"): 7890},
            expected_output_entities=get_all_entity_classes_in_module(
                normalized_entities
            ),
        )

        self.assertIsInstance(normalized_staff, NormalizedStateStaff)

    def test_normalize_staff_simple(self) -> None:
        pre_normalization_staff = entities.StateStaff(
            staff_id=1,
            state_code=StateCode.US_XX.value,
            full_name="Staff Name",
            email="staff@staff.com",
            external_ids=[
                entities.StateStaffExternalId(
                    staff_external_id_id=2,
                    state_code=StateCode.US_XX.value,
                    external_id="123",
                    id_type="US_XX_STAFF",
                )
            ],
            supervisor_periods=[
                entities.StateStaffSupervisorPeriod(
                    staff_supervisor_period_id=4,
                    state_code=StateCode.US_XX.value,
                    external_id="S1",
                    start_date=datetime.date(2023, 1, 1),
                    end_date=datetime.date(2023, 6, 1),
                    supervisor_staff_external_id="S1",
                    supervisor_staff_external_id_type="SUPERVISOR",
                )
            ],
        )

        expected_normalized_staff = normalized_entities.NormalizedStateStaff(
            staff_id=1,
            state_code=StateCode.US_XX.value,
            full_name="Staff Name",
            email="staff@staff.com",
            external_ids=[
                normalized_entities.NormalizedStateStaffExternalId(
                    staff_external_id_id=2,
                    state_code=StateCode.US_XX.value,
                    external_id="123",
                    id_type="US_XX_STAFF",
                )
            ],
            supervisor_periods=[
                normalized_entities.NormalizedStateStaffSupervisorPeriod(
                    staff_supervisor_period_id=4,
                    state_code=StateCode.US_XX.value,
                    external_id="S1",
                    start_date=datetime.date(2023, 1, 1),
                    end_date=datetime.date(2023, 6, 1),
                    supervisor_staff_external_id="S1",
                    supervisor_staff_external_id_type="SUPERVISOR",
                )
            ],
        )
        # Set back edges
        expected_normalized_staff.external_ids[0].staff = expected_normalized_staff
        expected_normalized_staff.supervisor_periods[
            0
        ].staff = expected_normalized_staff

        normalized_staff = build_normalized_state_staff(
            staff=pre_normalization_staff,
            staff_external_id_to_staff_id={("S1", "SUPERVISOR"): 7890},
            expected_output_entities=get_all_entity_classes_in_module(
                normalized_entities
            ),
        )

        self.assertEqual(expected_normalized_staff, normalized_staff)
