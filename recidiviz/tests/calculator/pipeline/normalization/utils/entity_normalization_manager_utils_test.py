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
# pylint: disable=protected-access
"""Tests the entity_normalization_manager_utils file."""
import datetime
import inspect
import unittest
from typing import Dict, Set, Type

from recidiviz.calculator.pipeline.normalization.utils import (
    entity_normalization_manager_utils,
)
from recidiviz.calculator.pipeline.normalization.utils.entity_normalization_manager_utils import (
    normalized_periods_for_calculations,
    normalized_violation_responses_from_processed_versions,
)
from recidiviz.calculator.pipeline.normalization.utils.normalization_managers.entity_normalization_manager import (
    EntityNormalizationManager,
)
from recidiviz.calculator.pipeline.normalization.utils.normalization_managers.supervision_violation_responses_normalization_manager import (
    ViolationResponseNormalizationManager,
)
from recidiviz.calculator.pipeline.utils.state_utils.templates.us_xx.us_xx_incarceration_period_normalization_delegate import (
    UsXxIncarcerationNormalizationDelegate,
)
from recidiviz.calculator.pipeline.utils.state_utils.templates.us_xx.us_xx_supervision_period_normalization_delegate import (
    UsXxSupervisionNormalizationDelegate,
)
from recidiviz.common.constants.state.state_incarceration import StateIncarcerationType
from recidiviz.common.constants.state.state_incarceration_period import (
    StateIncarcerationPeriodAdmissionReason,
    StateIncarcerationPeriodReleaseReason,
    StateSpecializedPurposeForIncarceration,
)
from recidiviz.common.constants.state.state_supervision_period import (
    StateSupervisionPeriodAdmissionReason,
    StateSupervisionPeriodSupervisionType,
    StateSupervisionPeriodTerminationReason,
)
from recidiviz.persistence.entity.base_entity import Entity
from recidiviz.persistence.entity.entity_utils import CoreEntityFieldIndex
from recidiviz.persistence.entity.state.entities import (
    StateIncarcerationPeriod,
    StateSupervisionPeriod,
)
from recidiviz.tests.calculator.pipeline.normalization.utils.normalized_entities_test import (
    classes_in_normalized_entity_subtree,
)
from recidiviz.tests.calculator.pipeline.normalization.utils.normalized_entities_utils_test import (
    get_normalized_violation_tree,
    get_violation_tree,
)
from recidiviz.tests.calculator.pipeline.utils.state_utils.state_calculation_config_manager_test import (
    STATE_DELEGATES_FOR_TESTS,
)


class TestIncarcerationNormalizationDelegate(UsXxIncarcerationNormalizationDelegate):
    def normalization_relies_on_supervision_periods(self) -> bool:
        return True

    def normalization_relies_on_violation_responses(self) -> bool:
        return True


class TestSupervisionNormalizationDelegate(UsXxSupervisionNormalizationDelegate):
    def normalization_relies_on_sentences(self) -> bool:
        return True


class TestNormalizedPeriodsForCalculations(unittest.TestCase):
    """Tests the normalized_periods_for_calculations function."""

    def setUp(self) -> None:
        self.field_index = CoreEntityFieldIndex()
        self.person_id = 123

    def test_normalized_periods_for_calculations(self) -> None:
        supervision_period = StateSupervisionPeriod.new_with_defaults(
            supervision_period_id=111,
            external_id="sp1",
            state_code="US_XX",
            start_date=datetime.date(2017, 3, 5),
            admission_reason=StateSupervisionPeriodAdmissionReason.COURT_SENTENCE,
            termination_date=datetime.date(2017, 5, 9),
            termination_reason=StateSupervisionPeriodTerminationReason.REVOCATION,
        )

        incarceration_period = StateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=111,
            external_id="ip1",
            incarceration_type=StateIncarcerationType.STATE_PRISON,
            state_code="US_XX",
            admission_date=datetime.date(2017, 4, 11),
            admission_reason=StateIncarcerationPeriodAdmissionReason.NEW_ADMISSION,
            release_date=datetime.date(2020, 5, 17),
            release_reason=StateIncarcerationPeriodReleaseReason.SENTENCE_SERVED,
            specialized_purpose_for_incarceration=StateSpecializedPurposeForIncarceration.GENERAL,
        )

        ((processed_ips, _), (processed_sps, _),) = normalized_periods_for_calculations(
            person_id=self.person_id,
            ip_normalization_delegate=STATE_DELEGATES_FOR_TESTS.ip_normalization_delegate,
            sp_normalization_delegate=STATE_DELEGATES_FOR_TESTS.sp_normalization_delegate,
            incarceration_periods=[incarceration_period],
            supervision_periods=[supervision_period],
            normalized_violation_responses=None,
            field_index=self.field_index,
            incarceration_sentences=None,
            supervision_sentences=None,
        )

        self.assertEqual([incarceration_period], processed_ips)
        self.assertEqual([supervision_period], processed_sps)

    def test_normalized_periods_for_calculations_no_sps(self) -> None:
        incarceration_period = StateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=111,
            external_id="ip1",
            incarceration_type=StateIncarcerationType.STATE_PRISON,
            state_code="US_XX",
            admission_date=datetime.date(2017, 4, 11),
            admission_reason=StateIncarcerationPeriodAdmissionReason.NEW_ADMISSION,
            release_date=datetime.date(2020, 5, 17),
            release_reason=StateIncarcerationPeriodReleaseReason.SENTENCE_SERVED,
            specialized_purpose_for_incarceration=StateSpecializedPurposeForIncarceration.GENERAL,
        )

        ((processed_ips, _), (processed_sps, _),) = normalized_periods_for_calculations(
            person_id=self.person_id,
            ip_normalization_delegate=STATE_DELEGATES_FOR_TESTS.ip_normalization_delegate,
            sp_normalization_delegate=STATE_DELEGATES_FOR_TESTS.sp_normalization_delegate,
            incarceration_periods=[incarceration_period],
            supervision_periods=None,
            normalized_violation_responses=None,
            field_index=self.field_index,
            incarceration_sentences=None,
            supervision_sentences=None,
        )

        self.assertEqual([incarceration_period], processed_ips)
        self.assertEqual([], processed_sps)

    def test_normalized_periods_for_calculations_no_sps_state_requires(
        self,
    ) -> None:
        incarceration_period = StateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=111,
            external_id="ip1",
            incarceration_type=StateIncarcerationType.STATE_PRISON,
            state_code="US_XX",
            admission_date=datetime.date(2017, 4, 11),
            admission_reason=StateIncarcerationPeriodAdmissionReason.NEW_ADMISSION,
            release_date=datetime.date(2020, 5, 17),
            release_reason=StateIncarcerationPeriodReleaseReason.SENTENCE_SERVED,
        )

        with self.assertRaises(ValueError):
            # Assert an error is raised if the supervision_periods arg is None for a
            # state that relies on supervision periods for IP pre-processing
            (_, _,) = normalized_periods_for_calculations(
                person_id=self.person_id,
                ip_normalization_delegate=TestIncarcerationNormalizationDelegate(),
                sp_normalization_delegate=STATE_DELEGATES_FOR_TESTS.sp_normalization_delegate,
                incarceration_periods=[incarceration_period],
                supervision_periods=None,
                normalized_violation_responses=[],
                field_index=self.field_index,
                incarceration_sentences=None,
                supervision_sentences=None,
            )

    def test_normalized_periods_for_calculations_no_violation_responses_state_requires(
        self,
    ) -> None:
        incarceration_period = StateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=111,
            external_id="ip1",
            incarceration_type=StateIncarcerationType.STATE_PRISON,
            state_code="US_XX",
            admission_date=datetime.date(2017, 4, 11),
            admission_reason=StateIncarcerationPeriodAdmissionReason.NEW_ADMISSION,
            release_date=datetime.date(2020, 5, 17),
            release_reason=StateIncarcerationPeriodReleaseReason.SENTENCE_SERVED,
        )

        with self.assertRaises(ValueError):
            # Assert an error is raised if the violation_responses arg is None for a
            # state that relies on violation responses for IP pre-processing
            (_, _,) = normalized_periods_for_calculations(
                person_id=self.person_id,
                ip_normalization_delegate=TestIncarcerationNormalizationDelegate(),
                sp_normalization_delegate=STATE_DELEGATES_FOR_TESTS.sp_normalization_delegate,
                incarceration_periods=[incarceration_period],
                supervision_periods=[],
                normalized_violation_responses=None,
                field_index=self.field_index,
                incarceration_sentences=None,
                supervision_sentences=None,
            )

    def test_normalized_periods_for_calculations_no_sentences_state_requires(
        self,
    ) -> None:
        supervision_period = StateSupervisionPeriod.new_with_defaults(
            supervision_period_id=111,
            external_id="sp1",
            supervision_type=StateSupervisionPeriodSupervisionType.PAROLE,
            state_code="US_XX",
            start_date=datetime.date(2017, 5, 1),
            termination_date=datetime.date(2018, 2, 1),
            admission_reason=StateSupervisionPeriodAdmissionReason.CONDITIONAL_RELEASE,
            termination_reason=StateSupervisionPeriodTerminationReason.RETURN_TO_INCARCERATION,
        )

        with self.assertRaises(ValueError):
            # Assert an error is raised if either sentences arg is None for a state
            # that relies on sentences for SP pre-processing
            (_, _) = normalized_periods_for_calculations(
                person_id=self.person_id,
                ip_normalization_delegate=STATE_DELEGATES_FOR_TESTS.ip_normalization_delegate,
                sp_normalization_delegate=TestSupervisionNormalizationDelegate(),
                incarceration_periods=[],
                supervision_periods=[supervision_period],
                normalized_violation_responses=None,
                field_index=self.field_index,
                incarceration_sentences=None,
                supervision_sentences=None,
            )

    def test_normalized_periods_for_calculations_no_ips(self) -> None:
        supervision_period = StateSupervisionPeriod.new_with_defaults(
            supervision_period_id=111,
            external_id="sp1",
            state_code="US_XX",
            start_date=datetime.date(2017, 3, 5),
            admission_reason=StateSupervisionPeriodAdmissionReason.COURT_SENTENCE,
            termination_date=datetime.date(2017, 5, 9),
            termination_reason=StateSupervisionPeriodTerminationReason.REVOCATION,
        )

        ((processed_ips, _), (processed_sps, _),) = normalized_periods_for_calculations(
            person_id=self.person_id,
            ip_normalization_delegate=STATE_DELEGATES_FOR_TESTS.ip_normalization_delegate,
            sp_normalization_delegate=STATE_DELEGATES_FOR_TESTS.sp_normalization_delegate,
            incarceration_periods=None,
            supervision_periods=[supervision_period],
            normalized_violation_responses=None,
            field_index=self.field_index,
            incarceration_sentences=None,
            supervision_sentences=None,
        )

        self.assertEqual([], processed_ips)
        self.assertEqual([supervision_period], processed_sps)

    def test_normalized_periods_for_calculations_empty_lists(self) -> None:

        ((processed_ips, _), (processed_sps, _),) = normalized_periods_for_calculations(
            person_id=self.person_id,
            ip_normalization_delegate=STATE_DELEGATES_FOR_TESTS.ip_normalization_delegate,
            sp_normalization_delegate=STATE_DELEGATES_FOR_TESTS.sp_normalization_delegate,
            incarceration_periods=[],
            supervision_periods=[],
            normalized_violation_responses=[],
            field_index=self.field_index,
            incarceration_sentences=None,
            supervision_sentences=None,
        )

        self.assertEqual([], processed_ips)
        self.assertEqual([], processed_sps)

    def test_normalization_managers_completeness(self) -> None:
        all_normalization_managers = [
            obj
            for _, obj in inspect.getmembers(entity_normalization_manager_utils)
            if inspect.isclass(obj)
            and issubclass(obj, EntityNormalizationManager)
            and obj != EntityNormalizationManager
        ]

        self.assertCountEqual(
            all_normalization_managers,
            entity_normalization_manager_utils.NORMALIZATION_MANAGERS,
        )

    def test_non_overlapping_normalized_subtrees(self) -> None:
        """Confirms that there are no entity types that are modified by more than one
        normalization manager."""
        normalized_entities_by_manager: Dict[str, Set[Type[Entity]]] = {}

        for (
            normalization_manager
        ) in entity_normalization_manager_utils.NORMALIZATION_MANAGERS:
            normalized_entities_by_manager[normalization_manager.__name__] = set(
                normalization_manager.normalized_entity_classes()
            )

        for manager, normalized_entities in normalized_entities_by_manager.items():
            for (
                other_manager,
                other_normalized_entities,
            ) in normalized_entities_by_manager.items():
                if manager == other_manager:
                    continue

                if shared_entities := normalized_entities.intersection(
                    other_normalized_entities
                ):
                    raise ValueError(
                        f"Entities that are normalized by {manager} are also "
                        f"normalized by {other_manager}: {shared_entities}. "
                        f"Entity cannot be normalized by more than one normalization "
                        f"manager."
                    )

    def test_subtree_completeness(self) -> None:
        """Tests that there are no entities reachable by any of the entities in the
        normalized_entity_classes list of a normalization manager that aren't also
        listed as modified by the manager during normalization."""
        for manager in entity_normalization_manager_utils.NORMALIZATION_MANAGERS:
            normalized_entities = set(manager.normalized_entity_classes())
            all_entities_in_subtree: Set[Type[Entity]] = set()

            for entity in normalized_entities:
                all_entities_in_subtree.update(
                    classes_in_normalized_entity_subtree(entity)
                )

            self.assertEqual(normalized_entities, all_entities_in_subtree)


class TestNormalizedViolationResponsesFromProcessedVersions(unittest.TestCase):
    """Tests the normalized_violation_responses_from_processed_versions function."""

    def setUp(self) -> None:
        self.field_index = CoreEntityFieldIndex()

    def test_normalized_violation_responses_from_processed_versions(self) -> None:
        violation = get_violation_tree()

        violation_responses = violation.supervision_violation_responses

        additional_attributes = ViolationResponseNormalizationManager.additional_attributes_map_for_normalized_vrs(
            violation_responses=violation_responses
        )

        normalized_responses = normalized_violation_responses_from_processed_versions(
            processed_violation_responses=violation_responses,
            additional_vr_attributes=additional_attributes,
            field_index=self.field_index,
        )

        expected_responses = (
            get_normalized_violation_tree().supervision_violation_responses
        )

        self.assertEqual(expected_responses, normalized_responses)

    def test_normalized_violation_responses_from_processed_versions_multiple_violations(
        self,
    ) -> None:
        violation_1 = get_violation_tree(starting_id_value=123)
        violation_2 = get_violation_tree(starting_id_value=456)

        violation_responses = (
            violation_1.supervision_violation_responses
            + violation_2.supervision_violation_responses
        )

        additional_attributes = ViolationResponseNormalizationManager.additional_attributes_map_for_normalized_vrs(
            violation_responses=violation_responses
        )

        normalized_responses = normalized_violation_responses_from_processed_versions(
            processed_violation_responses=violation_responses,
            additional_vr_attributes=additional_attributes,
            field_index=self.field_index,
        )

        expected_responses = (
            get_normalized_violation_tree(
                starting_id_value=123
            ).supervision_violation_responses
            + get_normalized_violation_tree(
                starting_id_value=456, starting_sequence_num=2
            ).supervision_violation_responses
        )

        self.assertEqual(expected_responses, normalized_responses)

    def test_normalized_violation_responses_from_processed_versions_no_violation(
        self,
    ) -> None:
        violation = get_violation_tree()

        violation_responses = violation.supervision_violation_responses
        for vr in violation_responses:
            vr.supervision_violation = None

        additional_attributes = ViolationResponseNormalizationManager.additional_attributes_map_for_normalized_vrs(
            violation_responses=violation_responses
        )

        with self.assertRaises(ValueError) as e:
            _ = normalized_violation_responses_from_processed_versions(
                processed_violation_responses=violation_responses,
                additional_vr_attributes=additional_attributes,
                field_index=self.field_index,
            )

        self.assertTrue(
            "Found empty supervision_violation on response" in e.exception.args[0]
        )
