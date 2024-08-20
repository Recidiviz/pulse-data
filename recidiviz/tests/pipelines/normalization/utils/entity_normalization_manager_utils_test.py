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

import attr

from recidiviz.common.attr_mixins import attr_field_referenced_cls_name_for_field_name
from recidiviz.common.constants.state.state_incarceration import StateIncarcerationType
from recidiviz.common.constants.state.state_incarceration_period import (
    StateIncarcerationPeriodAdmissionReason,
    StateIncarcerationPeriodReleaseReason,
    StateSpecializedPurposeForIncarceration,
)
from recidiviz.common.constants.state.state_supervision_period import (
    StateSupervisionPeriodAdmissionReason,
    StateSupervisionPeriodTerminationReason,
)
from recidiviz.common.constants.states import StateCode
from recidiviz.persistence.entity.base_entity import Entity, RootEntity
from recidiviz.persistence.entity.entity_utils import (
    get_entity_class_in_module_with_name,
)
from recidiviz.persistence.entity.state import entities as state_entities
from recidiviz.persistence.entity.state.entities import (
    StateIncarcerationPeriod,
    StateSupervisionPeriod,
)
from recidiviz.pipelines.normalization.utils import entity_normalization_manager_utils
from recidiviz.pipelines.normalization.utils.entity_normalization_manager_utils import (
    normalized_periods_for_calculations,
    normalized_violation_responses_from_processed_versions,
)
from recidiviz.pipelines.normalization.utils.normalization_managers.entity_normalization_manager import (
    EntityNormalizationManager,
)
from recidiviz.pipelines.normalization.utils.normalization_managers.supervision_violation_responses_normalization_manager import (
    ViolationResponseNormalizationManager,
)
from recidiviz.pipelines.utils.execution_utils import (
    build_staff_external_id_to_staff_id_map,
)
from recidiviz.pipelines.utils.state_utils.templates.us_xx.us_xx_incarceration_period_normalization_delegate import (
    UsXxIncarcerationNormalizationDelegate,
)
from recidiviz.pipelines.utils.state_utils.templates.us_xx.us_xx_supervision_period_normalization_delegate import (
    UsXxSupervisionNormalizationDelegate,
)
from recidiviz.pipelines.utils.state_utils.templates.us_xx.us_xx_violation_response_normalization_delegate import (
    UsXxViolationResponseNormalizationDelegate,
)
from recidiviz.tests.persistence.entity.normalized_entities_utils_test import (
    get_normalized_violation_tree,
    get_violation_tree,
)

STATE_PERSON_TO_STATE_STAFF_LIST = [
    {
        "person_id": 123,
        "staff_id": 10000,
        "staff_external_id": "EMP1",
        "staff_external_id_type": "US_XX_STAFF_ID",
    },
    {
        "person_id": 123,
        "staff_id": 20000,
        "staff_external_id": "EMP2",
        "staff_external_id_type": "US_XX_STAFF_ID",
    },
    {
        "person_id": 123,
        "staff_id": 30000,
        "staff_external_id": "EMP3",
        "staff_external_id_type": "US_XX_STAFF_ID",
    },
]


def classes_in_normalized_entity_subtree(
    entity_cls: Type[Entity],
) -> Set[Type[Entity]]:
    """Returns all classes in the subtree of the given entity class that could
    potentially be impacted by normalization. A subtree is defined as any entity that
    can be reached from a given entity without traversing an edge to StatePerson.

    Excludes any classes that cannot be hydrated in calculation pipelines,
    since these classes cannot be impacted by normalization.
    """
    explored_nodes: Set[Type[Entity]] = set()
    unexplored_nodes: Set[Type[Entity]] = {entity_cls}

    while unexplored_nodes:
        node_entity_class = unexplored_nodes.pop()
        for field in attr.fields_dict(node_entity_class):  # type: ignore[arg-type]
            related_class_name = attr_field_referenced_cls_name_for_field_name(
                node_entity_class, field
            )
            if not related_class_name:
                continue

            related_class = get_entity_class_in_module_with_name(
                state_entities, related_class_name
            )
            if issubclass(related_class, RootEntity):
                continue

            if related_class not in explored_nodes:
                unexplored_nodes.add(related_class)

        explored_nodes.add(node_entity_class)

    return explored_nodes


class TestNormalizedPeriodsForCalculations(unittest.TestCase):
    """Tests the normalized_periods_for_calculations function."""

    def setUp(self) -> None:
        self.state_code = StateCode.US_XX
        self.ip_normalization_delegate = UsXxIncarcerationNormalizationDelegate()
        self.sp_normalization_delegate = UsXxSupervisionNormalizationDelegate()
        self.person_id = 123

    def test_normalized_periods_for_calculations(self) -> None:
        supervision_period = StateSupervisionPeriod.new_with_defaults(
            supervision_period_id=111,
            external_id="sp1",
            state_code=self.state_code.value,
            start_date=datetime.date(2017, 3, 5),
            admission_reason=StateSupervisionPeriodAdmissionReason.COURT_SENTENCE,
            termination_date=datetime.date(2017, 5, 9),
            termination_reason=StateSupervisionPeriodTerminationReason.REVOCATION,
        )

        incarceration_period = StateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=111,
            external_id="ip1",
            incarceration_type=StateIncarcerationType.STATE_PRISON,
            state_code=self.state_code.value,
            admission_date=datetime.date(2017, 4, 11),
            admission_reason=StateIncarcerationPeriodAdmissionReason.NEW_ADMISSION,
            release_date=datetime.date(2020, 5, 17),
            release_reason=StateIncarcerationPeriodReleaseReason.SENTENCE_SERVED,
            specialized_purpose_for_incarceration=StateSpecializedPurposeForIncarceration.GENERAL,
        )

        ((processed_ips, _), (processed_sps, _),) = normalized_periods_for_calculations(
            person_id=self.person_id,
            ip_normalization_delegate=self.ip_normalization_delegate,
            sp_normalization_delegate=self.sp_normalization_delegate,
            incarceration_periods=[incarceration_period],
            supervision_periods=[supervision_period],
            normalized_violation_responses=[],
            staff_external_id_to_staff_id=build_staff_external_id_to_staff_id_map(
                STATE_PERSON_TO_STATE_STAFF_LIST
            ),
        )

        self.assertEqual([incarceration_period], processed_ips)
        self.assertEqual([supervision_period], processed_sps)

    def test_normalized_periods_for_calculations_no_sps(self) -> None:
        incarceration_period = StateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=111,
            external_id="ip1",
            incarceration_type=StateIncarcerationType.STATE_PRISON,
            state_code=self.state_code.value,
            admission_date=datetime.date(2017, 4, 11),
            admission_reason=StateIncarcerationPeriodAdmissionReason.NEW_ADMISSION,
            release_date=datetime.date(2020, 5, 17),
            release_reason=StateIncarcerationPeriodReleaseReason.SENTENCE_SERVED,
            specialized_purpose_for_incarceration=StateSpecializedPurposeForIncarceration.GENERAL,
        )

        ((processed_ips, _), (processed_sps, _),) = normalized_periods_for_calculations(
            person_id=self.person_id,
            ip_normalization_delegate=self.ip_normalization_delegate,
            sp_normalization_delegate=self.sp_normalization_delegate,
            incarceration_periods=[incarceration_period],
            supervision_periods=[],
            normalized_violation_responses=[],
            staff_external_id_to_staff_id=build_staff_external_id_to_staff_id_map(
                STATE_PERSON_TO_STATE_STAFF_LIST
            ),
        )

        self.assertEqual([incarceration_period], processed_ips)
        self.assertEqual([], processed_sps)

    def test_normalized_periods_for_calculations_no_ips(self) -> None:
        supervision_period = StateSupervisionPeriod.new_with_defaults(
            supervision_period_id=111,
            external_id="sp1",
            state_code=self.state_code.value,
            start_date=datetime.date(2017, 3, 5),
            admission_reason=StateSupervisionPeriodAdmissionReason.COURT_SENTENCE,
            termination_date=datetime.date(2017, 5, 9),
            termination_reason=StateSupervisionPeriodTerminationReason.REVOCATION,
        )

        ((processed_ips, _), (processed_sps, _),) = normalized_periods_for_calculations(
            person_id=self.person_id,
            ip_normalization_delegate=self.ip_normalization_delegate,
            sp_normalization_delegate=self.sp_normalization_delegate,
            incarceration_periods=[],
            supervision_periods=[supervision_period],
            normalized_violation_responses=[],
            staff_external_id_to_staff_id=build_staff_external_id_to_staff_id_map(
                STATE_PERSON_TO_STATE_STAFF_LIST
            ),
        )

        self.assertEqual([], processed_ips)
        self.assertEqual([supervision_period], processed_sps)

    def test_normalized_periods_for_calculations_empty_lists(self) -> None:
        ((processed_ips, _), (processed_sps, _),) = normalized_periods_for_calculations(
            person_id=self.person_id,
            ip_normalization_delegate=self.ip_normalization_delegate,
            sp_normalization_delegate=self.sp_normalization_delegate,
            incarceration_periods=[],
            supervision_periods=[],
            normalized_violation_responses=[],
            staff_external_id_to_staff_id=build_staff_external_id_to_staff_id_map(
                STATE_PERSON_TO_STATE_STAFF_LIST
            ),
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
        self.delegate = UsXxViolationResponseNormalizationDelegate()

    def test_normalized_violation_responses_from_processed_versions(self) -> None:
        violation = get_violation_tree()

        violation_responses = violation.supervision_violation_responses

        normalization_manager = ViolationResponseNormalizationManager(
            person_id=123,
            violation_responses=violation_responses,
            delegate=self.delegate,
            staff_external_id_to_staff_id=build_staff_external_id_to_staff_id_map(
                STATE_PERSON_TO_STATE_STAFF_LIST
            ),
        )

        (
            processed_violation_responses,
            additional_attributes,
        ) = normalization_manager.normalized_violation_responses_for_calculations()

        normalized_responses = normalized_violation_responses_from_processed_versions(
            processed_violation_responses=processed_violation_responses,
            additional_vr_attributes=additional_attributes,
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

        normalization_manager = ViolationResponseNormalizationManager(
            person_id=123,
            violation_responses=violation_responses,
            delegate=self.delegate,
            staff_external_id_to_staff_id=build_staff_external_id_to_staff_id_map(
                STATE_PERSON_TO_STATE_STAFF_LIST
            ),
        )

        (
            processed_violation_responses,
            additional_attributes,
        ) = normalization_manager.normalized_violation_responses_for_calculations()

        normalized_responses = normalized_violation_responses_from_processed_versions(
            processed_violation_responses=processed_violation_responses,
            additional_vr_attributes=additional_attributes,
        )

        expected_violation_1 = get_normalized_violation_tree(starting_id_value=123)
        expected_violation_2 = get_normalized_violation_tree(starting_id_value=456)

        # Normalization sorts violation responses in this order
        expected_violation_1.supervision_violation_responses[0].sequence_num = 0  # type: ignore[attr-defined]
        expected_violation_2.supervision_violation_responses[0].sequence_num = 1  # type: ignore[attr-defined]
        expected_violation_1.supervision_violation_responses[1].sequence_num = 2  # type: ignore[attr-defined]
        expected_violation_2.supervision_violation_responses[1].sequence_num = 3  # type: ignore[attr-defined]

        expected_responses = (
            expected_violation_1.supervision_violation_responses
            + expected_violation_2.supervision_violation_responses
        )

        self.assertEqual(expected_responses, normalized_responses)

    def test_normalized_violation_responses_from_processed_versions_no_violation(
        self,
    ) -> None:
        violation = get_violation_tree()

        violation_responses = violation.supervision_violation_responses
        for vr in violation_responses:
            vr.supervision_violation = None

        normalization_manager = ViolationResponseNormalizationManager(
            person_id=123,
            violation_responses=violation_responses,
            delegate=self.delegate,
            staff_external_id_to_staff_id=build_staff_external_id_to_staff_id_map(
                STATE_PERSON_TO_STATE_STAFF_LIST
            ),
        )

        additional_attributes = (
            normalization_manager.additional_attributes_map_for_normalized_vrs(
                violation_responses=violation_responses
            )
        )

        with self.assertRaises(ValueError) as e:
            _ = normalized_violation_responses_from_processed_versions(
                processed_violation_responses=violation_responses,
                additional_vr_attributes=additional_attributes,
            )

        self.assertTrue(
            "Found empty supervision_violation on response" in e.exception.args[0]
        )
