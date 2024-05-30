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
from typing import Any, Dict, List, Sequence, Type, Union

from recidiviz.common.constants.state.state_case_type import StateSupervisionCaseType
from recidiviz.ingest.direct.regions.direct_ingest_region_utils import (
    get_existing_direct_ingest_states,
)
from recidiviz.persistence.entity.base_entity import Entity
from recidiviz.persistence.entity.state.entities import (
    StateAssessment,
    StateIncarcerationPeriod,
    StatePerson,
    StateStaffSupervisorPeriod,
)
from recidiviz.persistence.entity.state.normalized_entities import (
    NormalizedStateSupervisionPeriod,
)
from recidiviz.pipelines.metrics.base_metric_pipeline import MetricPipeline
from recidiviz.pipelines.utils.entity_normalization.normalized_incarceration_period_index import (
    NormalizedIncarcerationPeriodIndex,
)
from recidiviz.pipelines.utils.execution_utils import TableRow
from recidiviz.pipelines.utils.state_utils import state_calculation_config_manager
from recidiviz.pipelines.utils.state_utils.state_calculation_config_manager import (
    _get_state_specific_incarceration_delegate,
    _get_state_specific_supervision_delegate,
    get_required_state_specific_delegates,
    get_required_state_specific_metrics_producer_delegates,
    get_state_specific_case_compliance_manager,
    get_state_specific_incarceration_period_normalization_delegate,
    get_state_specific_sentence_normalization_delegate,
    get_state_specific_staff_role_period_normalization_delegate,
    get_state_specific_supervision_period_normalization_delegate,
    get_state_specific_violation_response_normalization_delegate,
)
from recidiviz.pipelines.utils.state_utils.state_specific_delegate import (
    StateSpecificDelegate,
)
from recidiviz.pipelines.utils.state_utils.state_specific_metrics_producer_delegate import (
    StateSpecificMetricsProducerDelegate,
)
from recidiviz.pipelines.utils.state_utils.templates.us_xx.us_xx_assessment_normalization_delegate import (
    UsXxAssessmentNormalizationDelegate,
)
from recidiviz.pipelines.utils.state_utils.templates.us_xx.us_xx_commitment_from_supervision_utils import (
    UsXxCommitmentFromSupervisionDelegate,
)
from recidiviz.pipelines.utils.state_utils.templates.us_xx.us_xx_incarceration_delegate import (
    UsXxIncarcerationDelegate,
)
from recidiviz.pipelines.utils.state_utils.templates.us_xx.us_xx_incarceration_period_normalization_delegate import (
    UsXxIncarcerationNormalizationDelegate,
)
from recidiviz.pipelines.utils.state_utils.templates.us_xx.us_xx_sentence_normalization_delegate import (
    UsXxSentenceNormalizationDelegate,
)
from recidiviz.pipelines.utils.state_utils.templates.us_xx.us_xx_staff_role_period_normalization_delegate import (
    UsXxStaffRolePeriodNormalizationDelegate,
)
from recidiviz.pipelines.utils.state_utils.templates.us_xx.us_xx_supervision_delegate import (
    UsXxSupervisionDelegate,
)
from recidiviz.pipelines.utils.state_utils.templates.us_xx.us_xx_supervision_period_normalization_delegate import (
    UsXxSupervisionNormalizationDelegate,
)
from recidiviz.pipelines.utils.state_utils.templates.us_xx.us_xx_violation_response_normalization_delegate import (
    UsXxViolationResponseNormalizationDelegate,
)
from recidiviz.pipelines.utils.state_utils.templates.us_xx.us_xx_violations_delegate import (
    UsXxViolationDelegate,
)
from recidiviz.utils.range_querier import RangeQuerier

_DEFAULT_SUPERVISION_PERIOD_ID = 999

DEFAULT_US_MO_SENTENCE_STATUSES = [
    {
        "sentence_external_id": "1061945-20030505-7",
        "sentence_status_external_id": "1061945-20030505-7-26",
        "status_code": "35I1000",
        "status_date": "20180716",
        "status_description": "Court Probation-Revisit",
    },
]

# TODO(#30202): Delete this once all pipelines have been migrated away from get_required_state_specific_delegates
# The state-specific delegates that should be used in state-agnostic tests
STATE_DELEGATES_FOR_TESTS: Dict[str, StateSpecificDelegate] = {
    "StateSpecificIncarcerationNormalizationDelegate": UsXxIncarcerationNormalizationDelegate(),
    "StateSpecificSupervisionNormalizationDelegate": UsXxSupervisionNormalizationDelegate(),
    "StateSpecificViolationResponseNormalizationDelegate": UsXxViolationResponseNormalizationDelegate(),
    "StateSpecificCommitmentFromSupervisionDelegate": UsXxCommitmentFromSupervisionDelegate(),
    "StateSpecificViolationDelegate": UsXxViolationDelegate(),
    "StateSpecificIncarcerationDelegate": UsXxIncarcerationDelegate(),
    "StateSpecificSupervisionDelegate": UsXxSupervisionDelegate(),
    "StateSpecificAssessmentNormalizationDelegate": UsXxAssessmentNormalizationDelegate(),
    "StateSpecificSentenceNormalizationDelegate": UsXxSentenceNormalizationDelegate(),
    "StateSpecificStaffRolePeriodNormalizationDelegate": UsXxStaffRolePeriodNormalizationDelegate(),
}


def test_get_required_state_specific_delegates() -> None:
    """Tests that we can call all functions in the state_calculation_config_manager
    file with all of the state codes that we expect to be supported."""
    for state in get_existing_direct_ingest_states():
        get_required_state_specific_delegates(
            state.value,
            [
                subclass
                for pipeline_cls in MetricPipeline.__subclasses__()
                for subclass in pipeline_cls.state_specific_required_delegates()
                if subclass is not StateSpecificMetricsProducerDelegate
            ],
            entity_kwargs={
                StateAssessment.__name__: [
                    StateAssessment.new_with_defaults(
                        state_code=state.value,
                        external_id="a1",
                    )
                ],
                StatePerson.__name__: [
                    StatePerson.new_with_defaults(state_code=state.value)
                ],
                StateIncarcerationPeriod.__name__: [
                    StateIncarcerationPeriod.new_with_defaults(
                        state_code=state.value,
                        external_id="ip1",
                    )
                ],
                StateStaffSupervisorPeriod.__name__: [
                    StateStaffSupervisorPeriod.new_with_defaults(
                        state_code=state.value,
                        external_id="SSP",
                        start_date=datetime.date(2020, 1, 2),
                        supervisor_staff_external_id="SUP1",
                        supervisor_staff_external_id_type="SUPERVISOR",
                    )
                ],
                "us_mo_sentence_statuses": DEFAULT_US_MO_SENTENCE_STATUSES,
            },
        )

        _get_state_specific_supervision_delegate(state.value)

        for subclass in StateSpecificMetricsProducerDelegate.__subclasses__():
            get_required_state_specific_metrics_producer_delegates(
                state.value, {subclass}
            )


class TestGetRequiredStateSpecificDelegates(unittest.TestCase):
    """Tests the get_required_state_specific_delegates function."""

    def test_get_required_state_specific_delegates_no_delegates(self) -> None:
        required_delegates: List[Type[StateSpecificDelegate]] = []
        entity_kwargs: Dict[str, Union[Sequence[Entity], List[TableRow]]] = {}
        delegates = (
            state_calculation_config_manager.get_required_state_specific_delegates(
                state_code="US_XX",
                required_delegates=required_delegates,
                entity_kwargs=entity_kwargs,
            )
        )

        expected_delegates: Dict[str, Any] = {}

        self.assertEqual(expected_delegates, delegates)

    def test_get_state_specific_staff_role_period_normalization_delegate_defined(
        self,
    ) -> None:
        for state_code in get_existing_direct_ingest_states():
            _ = get_state_specific_staff_role_period_normalization_delegate(
                state_code.value, staff_supervisor_periods=[]
            )

    def test_get_state_specific_violation_response_normalization_delegate(self) -> None:
        for state_code in get_existing_direct_ingest_states():
            _ = get_state_specific_violation_response_normalization_delegate(
                state_code.value, incarceration_periods=[]
            )

    def test_get_state_specific_sentence_normalization_delegate(self) -> None:
        for state_code in get_existing_direct_ingest_states():
            _ = get_state_specific_sentence_normalization_delegate(state_code.value)

    def test_get_state_specific_incarceration_period_normalization_delegate(
        self,
    ) -> None:
        for state_code in get_existing_direct_ingest_states():
            _ = get_state_specific_incarceration_period_normalization_delegate(
                state_code.value
            )

    def test_get_state_specific_supervision_period_normalization_delegate(self) -> None:
        for state_code in get_existing_direct_ingest_states():
            _ = get_state_specific_supervision_period_normalization_delegate(
                state_code.value,
                incarceration_periods=[],
                assessments=[],
                us_mo_sentence_statuses_list=[],
            )

    def test_get_state_specific_case_compliance_manager(self) -> None:
        for state_code in get_existing_direct_ingest_states():
            test_sp = NormalizedStateSupervisionPeriod.new_with_defaults(
                state_code=state_code.value, external_id="sp1", sequence_num=1
            )
            _ = get_state_specific_case_compliance_manager(
                person=StatePerson.new_with_defaults(
                    state_code=state_code.value, person_id=1
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
                    incarceration_delegate=_get_state_specific_incarceration_delegate(
                        state_code=state_code.value
                    ),
                ),
                supervision_delegate=_get_state_specific_supervision_delegate(
                    state_code=state_code.value
                ),
            )

    # TODO(#30202): Add tests for remainder of delegates once they've been migrated over
    #  to public methods.
