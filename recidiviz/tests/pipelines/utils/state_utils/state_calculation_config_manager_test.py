# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2021 Recidiviz, Inc.
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
"""Tests that all states with defined state-specific delegates are supported in the
state_calculation_config_manager functions."""
import datetime
import unittest

from recidiviz.common.constants.state.state_case_type import StateSupervisionCaseType
from recidiviz.common.constants.state.state_person import StateEthnicity
from recidiviz.ingest.direct.regions.direct_ingest_region_utils import (
    get_existing_direct_ingest_states,
)
from recidiviz.persistence.entity.state.entities import StatePerson
from recidiviz.persistence.entity.state.normalized_entities import (
    NormalizedStatePerson,
    NormalizedStateSupervisionPeriod,
)
from recidiviz.pipelines.utils.entity_normalization.normalized_incarceration_period_index import (
    NormalizedIncarcerationPeriodIndex,
)
from recidiviz.pipelines.utils.state_utils.state_calculation_config_manager import (
    get_state_specific_assessment_normalization_delegate,
    get_state_specific_case_compliance_manager,
    get_state_specific_commitment_from_supervision_delegate,
    get_state_specific_incarceration_delegate,
    get_state_specific_incarceration_period_normalization_delegate,
    get_state_specific_normalization_delegate,
    get_state_specific_sentence_normalization_delegate,
    get_state_specific_staff_role_period_normalization_delegate,
    get_state_specific_supervision_delegate,
    get_state_specific_supervision_period_normalization_delegate,
    get_state_specific_violation_delegate,
    get_state_specific_violation_response_normalization_delegate,
)
from recidiviz.tests.pipelines.fake_state_calculation_config_manager import (
    get_all_delegate_getter_fn_names,
)
from recidiviz.utils.range_querier import RangeQuerier


class TestStateCalculationConfigManager(unittest.TestCase):
    """Tests the functions in state_calculation_config_manager.py."""

    def test_has_tests_for_all_delegate_getters(self) -> None:
        all_getter_fn_names = get_all_delegate_getter_fn_names()
        test_names = {t for t in dir(self) if t.startswith("test_get")}

        tested_getter_fns = {t.removeprefix("test_") for t in test_names}

        missing_tests = all_getter_fn_names - tested_getter_fns
        if missing_tests:
            raise ValueError(
                f"Missing a test for the following delegate getter functions: "
                f"{missing_tests}"
            )

    def test_get_state_specific_staff_role_period_normalization_delegate(
        self,
    ) -> None:
        for state_code in get_existing_direct_ingest_states():
            _ = get_state_specific_staff_role_period_normalization_delegate(
                state_code.value, staff_supervisor_periods=[]
            )

    def test_get_state_specific_assessment_normalization_delegate(self) -> None:
        for state_code in get_existing_direct_ingest_states():
            _ = get_state_specific_assessment_normalization_delegate(
                state_code.value,
                person=StatePerson(state_code=state_code.value, person_id=1),
            )

    def test_get_state_specific_violation_response_normalization_delegate(self) -> None:
        for state_code in get_existing_direct_ingest_states():
            _ = get_state_specific_violation_response_normalization_delegate(
                state_code.value, incarceration_periods=[]
            )

    def test_get_state_specific_sentence_normalization_delegate(self) -> None:
        for state_code in get_existing_direct_ingest_states():
            _ = get_state_specific_sentence_normalization_delegate(
                state_code.value, incarceration_periods=[], sentences=[]
            )

    def test_get_state_specific_incarceration_period_normalization_delegate(
        self,
    ) -> None:
        for state_code in get_existing_direct_ingest_states():
            _ = get_state_specific_incarceration_period_normalization_delegate(
                state_code.value, incarceration_sentences=[]
            )

    def test_get_state_specific_supervision_period_normalization_delegate(self) -> None:
        for state_code in get_existing_direct_ingest_states():
            _ = get_state_specific_supervision_period_normalization_delegate(
                state_code.value,
                incarceration_periods=[],
                assessments=[],
                supervision_sentences=[],
                sentences=[],
            )

    def test_get_state_specific_case_compliance_manager(self) -> None:
        for state_code in get_existing_direct_ingest_states():
            test_sp = NormalizedStateSupervisionPeriod(
                supervision_period_id=1,
                state_code=state_code.value,
                external_id="sp1",
                start_date=datetime.date(2020, 1, 1),
                sequence_num=1,
            )
            _ = get_state_specific_case_compliance_manager(
                person=NormalizedStatePerson(
                    state_code=state_code.value,
                    person_id=1,
                    ethnicity=StateEthnicity.PRESENT_WITHOUT_INFO,
                ),
                supervision_period=test_sp,
                case_type=StateSupervisionCaseType.INTERNAL_UNKNOWN,
                start_of_supervision=datetime.date(2020, 1, 1),
                assessments_by_date=RangeQuerier(
                    [], lambda assessment: assessment.assessment_date
                ),
                supervision_contacts_by_date=RangeQuerier(
                    [], lambda contact: contact.contact_date
                ),
                violation_responses=[],
                incarceration_period_index=NormalizedIncarcerationPeriodIndex(
                    sorted_incarceration_periods=[],
                    incarceration_delegate=get_state_specific_incarceration_delegate(
                        state_code=state_code.value
                    ),
                ),
                supervision_delegate=get_state_specific_supervision_delegate(
                    state_code=state_code.value
                ),
            )

    def test_get_state_specific_commitment_from_supervision_delegate(
        self,
    ) -> None:
        for state_code in get_existing_direct_ingest_states():
            _ = get_state_specific_commitment_from_supervision_delegate(
                state_code.value
            )

    def test_get_state_specific_violation_delegate(
        self,
    ) -> None:
        for state_code in get_existing_direct_ingest_states():
            _ = get_state_specific_violation_delegate(state_code.value)

    def test_get_state_specific_incarceration_delegate(
        self,
    ) -> None:
        for state_code in get_existing_direct_ingest_states():
            _ = get_state_specific_incarceration_delegate(state_code.value)

    def test_get_state_specific_supervision_delegate(
        self,
    ) -> None:
        for state_code in get_existing_direct_ingest_states():
            _ = get_state_specific_supervision_delegate(state_code.value)

    def test_get_state_specific_normalization_delegate(
        self,
    ) -> None:
        for state_code in get_existing_direct_ingest_states():
            _ = get_state_specific_normalization_delegate(state_code.value)
