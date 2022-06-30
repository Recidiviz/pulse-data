# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2022 Recidiviz, Inc.
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
"""Tests the normalized_entities_utils.py file."""
import datetime
import unittest
from typing import Optional

import attr
import mock

from recidiviz.big_query.big_query_utils import MAX_BQ_INT
from recidiviz.calculator.pipeline.normalization.utils import normalized_entities
from recidiviz.calculator.pipeline.normalization.utils.normalized_entities import (
    NormalizedStateEntity,
    NormalizedStateSupervisionViolatedConditionEntry,
    NormalizedStateSupervisionViolation,
    NormalizedStateSupervisionViolationResponse,
    NormalizedStateSupervisionViolationResponseDecisionEntry,
    NormalizedStateSupervisionViolationTypeEntry,
    add_normalized_entity_validator_to_ref_fields,
)
from recidiviz.calculator.pipeline.normalization.utils.normalized_entities_utils import (
    NORMALIZED_ENTITY_CLASSES,
    AdditionalAttributesMap,
    clear_entity_id_index_cache,
    copy_entities_and_add_unique_ids,
    get_shared_additional_attributes_map_for_entities,
    merge_additional_attributes_maps,
    update_normalized_entity_with_globally_unique_id,
)
from recidiviz.common.constants.state.state_incarceration_period import (
    StateSpecializedPurposeForIncarceration,
)
from recidiviz.common.constants.states import StateCode
from recidiviz.persistence.entity.entity_utils import get_all_entity_classes_in_module
from recidiviz.persistence.entity.state import entities
from recidiviz.persistence.entity.state.entities import (
    StateIncarcerationPeriod,
    StateSupervisionViolation,
    StateSupervisionViolationResponse,
)
from recidiviz.tests.calculator.pipeline.normalization.utils.normalization_managers.supervision_violation_responses_normalization_manager_test import (
    hydrate_bidirectional_relationships_on_expected_response,
)


@attr.s(
    eq=False,
    kw_only=True,
    field_transformer=add_normalized_entity_validator_to_ref_fields,
)
class FakeNormalizedStateSupervisionSentence(
    entities.StateSupervisionSentence, NormalizedStateEntity
):
    """Fake NormalizedStateSupervisionSentence to use in tests."""


@attr.s(
    eq=False,
    kw_only=True,
    field_transformer=add_normalized_entity_validator_to_ref_fields,
)
class FakeNormalizedStateCharge(entities.StateCharge, NormalizedStateEntity):
    """Fake NormalizedStateCharge to use in tests."""


@attr.s(
    eq=False,
    kw_only=True,
    field_transformer=add_normalized_entity_validator_to_ref_fields,
)
class FakeNormalizedStateCourtCase(entities.StateCourtCase, NormalizedStateEntity):
    """Fake NormalizedStateCourtCase to use in tests."""


class TestNormalizedEntityClassesCoverage(unittest.TestCase):
    """Tests that all entity classes with Normalized versions are listed in
    NORMALIZED_ENTITY_CLASSES."""

    def test_normalized_entity_classes_coverage(self) -> None:
        entity_classes = [
            entity_class
            for entity_class in get_all_entity_classes_in_module(normalized_entities)
            if issubclass(entity_class, NormalizedStateEntity)
        ]

        self.assertCountEqual(entity_classes, NORMALIZED_ENTITY_CLASSES)


class TestMergeAdditionalAttributesMaps(unittest.TestCase):
    """Tests the merge_additional_attributes_maps function."""

    def test_merge_additional_attributes_maps(self) -> None:
        map_1: AdditionalAttributesMap = {
            StateIncarcerationPeriod.__name__: {123: {"sequence_num": 0}}
        }

        map_2: AdditionalAttributesMap = {
            StateIncarcerationPeriod.__name__: {
                123: {"purpose_for_incarceration_subtype": "XYZ"}
            }
        }

        merged_map = merge_additional_attributes_maps([map_1, map_2])

        expected_merged_maps: AdditionalAttributesMap = {
            StateIncarcerationPeriod.__name__: {
                123: {"sequence_num": 0, "purpose_for_incarceration_subtype": "XYZ"}
            }
        }

        self.assertEqual(expected_merged_maps, merged_map)

    def test_merge_additional_attributes_maps_diff_entity_types(self) -> None:
        map_1: AdditionalAttributesMap = {
            StateSupervisionViolationResponse.__name__: {123: {"sequence_num": 0}}
        }

        map_2: AdditionalAttributesMap = {
            StateIncarcerationPeriod.__name__: {
                123: {"purpose_for_incarceration_subtype": "XYZ"}
            }
        }

        merged_map = merge_additional_attributes_maps([map_1, map_2])

        expected_merged_maps: AdditionalAttributesMap = {
            StateSupervisionViolationResponse.__name__: {123: {"sequence_num": 0}},
            StateIncarcerationPeriod.__name__: {
                123: {"purpose_for_incarceration_subtype": "XYZ"}
            },
        }

        self.assertEqual(expected_merged_maps, merged_map)

    def test_merge_additional_attributes_maps_diff_entity_ids(self) -> None:
        map_1: AdditionalAttributesMap = {
            StateIncarcerationPeriod.__name__: {123: {"sequence_num": 0}}
        }

        map_2: AdditionalAttributesMap = {
            StateIncarcerationPeriod.__name__: {
                456: {"purpose_for_incarceration_subtype": "XYZ"}
            }
        }

        merged_map = merge_additional_attributes_maps([map_1, map_2])

        expected_merged_maps: AdditionalAttributesMap = {
            StateIncarcerationPeriod.__name__: {
                123: {"sequence_num": 0},
                456: {"purpose_for_incarceration_subtype": "XYZ"},
            }
        }

        self.assertEqual(expected_merged_maps, merged_map)

    def test_merge_additional_attributes_maps_empty_map(self) -> None:
        map_1: AdditionalAttributesMap = {
            StateIncarcerationPeriod.__name__: {123: {"sequence_num": 0}}
        }

        map_2: AdditionalAttributesMap = {}

        merged_map = merge_additional_attributes_maps([map_1, map_2])

        expected_merged_maps: AdditionalAttributesMap = {
            StateIncarcerationPeriod.__name__: {
                123: {"sequence_num": 0},
            }
        }

        self.assertEqual(expected_merged_maps, merged_map)

    def test_merge_additional_attributes_maps_empty_map_with_name(self) -> None:
        map_1: AdditionalAttributesMap = {
            StateIncarcerationPeriod.__name__: {123: {"sequence_num": 0}}
        }

        map_2: AdditionalAttributesMap = {StateSupervisionViolation.__name__: {}}

        merged_map = merge_additional_attributes_maps([map_1, map_2])

        expected_merged_maps: AdditionalAttributesMap = {
            StateIncarcerationPeriod.__name__: {
                123: {"sequence_num": 0},
            },
            StateSupervisionViolation.__name__: {},
        }

        self.assertEqual(expected_merged_maps, merged_map)


class TestGetSharedAdditionalAttributesMapForEntities(unittest.TestCase):
    """Tests the get_shared_additional_attributes_map_for_entities function."""

    def test_get_shared_additional_attributes_map_for_entities(self) -> None:
        entities_for_map = [
            StateIncarcerationPeriod.new_with_defaults(
                state_code="US_XX",
                incarceration_period_id=111,
                specialized_purpose_for_incarceration=StateSpecializedPurposeForIncarceration.PAROLE_BOARD_HOLD,
                admission_date=datetime.date(2000, 1, 1),
                release_date=datetime.date(2002, 1, 1),
            ),
            StateIncarcerationPeriod.new_with_defaults(
                state_code="US_XX",
                incarceration_period_id=222,
                specialized_purpose_for_incarceration=StateSpecializedPurposeForIncarceration.PAROLE_BOARD_HOLD,
                admission_date=datetime.date(2002, 1, 1),
            ),
        ]

        attributes_map = get_shared_additional_attributes_map_for_entities(
            entities=entities_for_map
        )

        expected_attributes_map: AdditionalAttributesMap = {
            StateIncarcerationPeriod.__name__: {
                111: {"sequence_num": 0},
                222: {"sequence_num": 1},
            }
        }

        self.assertEqual(expected_attributes_map, attributes_map)

    def test_get_shared_additional_attributes_map_for_entities_not_sequenced(
        self,
    ) -> None:
        entities_for_map = [get_violation_tree()]

        attributes_map = get_shared_additional_attributes_map_for_entities(
            entities=entities_for_map
        )

        expected_attributes_map: AdditionalAttributesMap = {
            StateSupervisionViolation.__name__: {}
        }

        self.assertEqual(expected_attributes_map, attributes_map)


def get_violation_tree(
    starting_id_value: Optional[int] = None,
) -> entities.StateSupervisionViolation:
    """Returns a tree of entities connected to the StateSupervisionViolation
    for use in tests that normalize violation data.

    DO NOT UPDATE THIS WITHOUT ALSO UPDATING get_normalized_violation_tree.
    """
    starting_id_value = starting_id_value or 1

    supervision_violation = entities.StateSupervisionViolation.new_with_defaults(
        supervision_violation_id=starting_id_value,
        violation_date=datetime.date(year=2004, month=9, day=1),
        state_code="US_XX",
        is_violent=False,
        supervision_violation_types=[
            entities.StateSupervisionViolationTypeEntry.new_with_defaults(
                supervision_violation_type_entry_id=starting_id_value + 1,
                state_code="US_XX",
                violation_type=entities.StateSupervisionViolationType.TECHNICAL,
                violation_type_raw_text="TECHNICAL",
            ),
        ],
        supervision_violated_conditions=[
            entities.StateSupervisionViolatedConditionEntry.new_with_defaults(
                supervision_violated_condition_entry_id=starting_id_value + 2,
                state_code="US_XX",
                condition="MISSED CURFEW",
            )
        ],
    )

    supervision_violation_response_1 = entities.StateSupervisionViolationResponse.new_with_defaults(
        supervision_violation_response_id=starting_id_value + 3,
        response_type=entities.StateSupervisionViolationResponseType.CITATION,
        response_date=datetime.date(year=2004, month=9, day=2),
        state_code="US_XX",
        deciding_body_type=entities.StateSupervisionViolationResponseDecidingBodyType.SUPERVISION_OFFICER,
        supervision_violation=supervision_violation,
        supervision_violation_response_decisions=[
            entities.StateSupervisionViolationResponseDecisionEntry.new_with_defaults(
                state_code="US_XX",
                supervision_violation_response_decision_entry_id=starting_id_value + 4,
                decision_raw_text="X",
            )
        ],
    )

    supervision_violation_response_2 = entities.StateSupervisionViolationResponse.new_with_defaults(
        supervision_violation_response_id=starting_id_value + 5,
        response_type=entities.StateSupervisionViolationResponseType.VIOLATION_REPORT,
        response_date=datetime.date(year=2004, month=10, day=3),
        state_code="US_XX",
        deciding_body_type=entities.StateSupervisionViolationResponseDecidingBodyType.SUPERVISION_OFFICER,
        supervision_violation=supervision_violation,
        supervision_violation_response_decisions=[
            entities.StateSupervisionViolationResponseDecisionEntry.new_with_defaults(
                state_code="US_XX",
                supervision_violation_response_decision_entry_id=starting_id_value + 6,
                decision_raw_text="Y",
            )
        ],
    )

    hydrate_bidirectional_relationships_on_expected_response(
        supervision_violation_response_1
    )

    hydrate_bidirectional_relationships_on_expected_response(
        supervision_violation_response_2
    )

    supervision_violation.supervision_violation_responses = [
        supervision_violation_response_1,
        supervision_violation_response_2,
    ]

    return supervision_violation


def get_normalized_violation_tree(
    starting_id_value: Optional[int] = 0, starting_sequence_num: Optional[int] = 0
) -> NormalizedStateSupervisionViolation:
    """Returns a tree of normalized versions of the entities returned by
    _get_violation_response_tree."""
    starting_id_value = starting_id_value or 1
    starting_sequence_num = starting_sequence_num or 0

    supervision_violation = NormalizedStateSupervisionViolation.new_with_defaults(
        supervision_violation_id=starting_id_value,
        violation_date=datetime.date(year=2004, month=9, day=1),
        state_code="US_XX",
        is_violent=False,
        supervision_violation_types=[
            NormalizedStateSupervisionViolationTypeEntry.new_with_defaults(
                state_code="US_XX",
                supervision_violation_type_entry_id=starting_id_value + 1,
                violation_type=entities.StateSupervisionViolationType.TECHNICAL,
                violation_type_raw_text="TECHNICAL",
            ),
        ],
        supervision_violated_conditions=[
            NormalizedStateSupervisionViolatedConditionEntry.new_with_defaults(
                supervision_violated_condition_entry_id=starting_id_value + 2,
                state_code="US_XX",
                condition="MISSED CURFEW",
            )
        ],
    )

    supervision_violation_response_1 = NormalizedStateSupervisionViolationResponse.new_with_defaults(
        supervision_violation_response_id=starting_id_value + 3,
        response_type=entities.StateSupervisionViolationResponseType.CITATION,
        response_date=datetime.date(year=2004, month=9, day=2),
        state_code="US_XX",
        deciding_body_type=entities.StateSupervisionViolationResponseDecidingBodyType.SUPERVISION_OFFICER,
        sequence_num=starting_sequence_num,
        supervision_violation=supervision_violation,
        supervision_violation_response_decisions=[
            NormalizedStateSupervisionViolationResponseDecisionEntry.new_with_defaults(
                state_code="US_XX",
                supervision_violation_response_decision_entry_id=starting_id_value + 4,
                decision_raw_text="X",
            )
        ],
    )

    supervision_violation_response_2 = NormalizedStateSupervisionViolationResponse.new_with_defaults(
        supervision_violation_response_id=starting_id_value + 5,
        response_type=entities.StateSupervisionViolationResponseType.VIOLATION_REPORT,
        response_date=datetime.date(year=2004, month=10, day=3),
        state_code="US_XX",
        deciding_body_type=entities.StateSupervisionViolationResponseDecidingBodyType.SUPERVISION_OFFICER,
        sequence_num=starting_sequence_num + 1,
        supervision_violation=supervision_violation,
        supervision_violation_response_decisions=[
            NormalizedStateSupervisionViolationResponseDecisionEntry.new_with_defaults(
                state_code="US_XX",
                supervision_violation_response_decision_entry_id=starting_id_value + 6,
                decision_raw_text="Y",
            )
        ],
    )

    hydrate_bidirectional_relationships_on_expected_response(
        supervision_violation_response_1
    )
    hydrate_bidirectional_relationships_on_expected_response(
        supervision_violation_response_2
    )

    supervision_violation.supervision_violation_responses = [
        supervision_violation_response_1,
        supervision_violation_response_2,
    ]

    return supervision_violation


class TestUpdateNormalizedEntityWithGloballyUniqueId(unittest.TestCase):
    """Tests the update_normalized_entity_with_globally_unique_id function."""

    def setUp(self) -> None:
        clear_entity_id_index_cache()
        self.mock_safe_object_id_patcher = mock.patch(
            "recidiviz.calculator.pipeline.normalization."
            "utils.normalized_entities_utils._fixed_length_object_id_for_entity",
            return_value=88888,
        )
        self.mock_safe_object_id = self.mock_safe_object_id_patcher.start()

    def tearDown(self) -> None:
        self.mock_safe_object_id_patcher.stop()

    def test_update_normalized_entity_with_globally_unique_id(self) -> None:
        state_code = StateCode.US_XX
        person_id = 990000012345

        entity = entities.StateSupervisionViolationTypeEntry.new_with_defaults(
            supervision_violation_type_entry_id=456,
            state_code=state_code.value,
            violation_type=entities.StateSupervisionViolationType.TECHNICAL,
            violation_type_raw_text="TECHNICAL",
        )

        update_normalized_entity_with_globally_unique_id(
            person_id=person_id, entity=entity
        )

        expected_id_value = 99000001234588888

        self.assertEqual(expected_id_value, entity.get_id())

    @mock.patch(
        "recidiviz.calculator.pipeline.normalization.utils.normalized_entities_utils"
        "._get_entity_id_index_for_person_id_entity"
    )
    def test_update_normalized_entity_with_globally_unique_id_id_taken(
        self, mock_id_index: mock.MagicMock
    ) -> None:
        """Tests that when the first two id values created have already been assigned
        to an entity of the same type for the person that the value is incremented
        until a unique value is found."""
        mock_id_index.return_value = {88888, 88889}

        state_code = StateCode.US_XX
        person_id = 990000012345

        entity = entities.StateSupervisionViolationTypeEntry.new_with_defaults(
            supervision_violation_type_entry_id=456,
            state_code=state_code.value,
            violation_type=entities.StateSupervisionViolationType.TECHNICAL,
            violation_type_raw_text="TECHNICAL",
        )

        update_normalized_entity_with_globally_unique_id(
            person_id=person_id, entity=entity
        )

        expected_id_value = 99000001234588890

        self.assertEqual(expected_id_value, entity.get_id())

    @mock.patch(
        "recidiviz.calculator.pipeline.normalization.utils"
        ".normalized_entities_utils._get_entity_id_index_for_person_id_entity"
    )
    def test_update_normalized_entity_with_globally_unique_id_id_taken_over_5_digits(
        self, mock_id_index: mock.MagicMock
    ) -> None:
        """Tests that when the first id value has already been assigned to an entity
        of the same type for the person, and the incremented value is over 5 digits,
        that the value chosen is still within 5 digits."""
        self.mock_safe_object_id.return_value = 99999
        mock_id_index.return_value = {99999}

        state_code = StateCode.US_XX
        person_id = 990000012345

        entity = entities.StateSupervisionViolationTypeEntry.new_with_defaults(
            supervision_violation_type_entry_id=456,
            state_code=state_code.value,
            violation_type=entities.StateSupervisionViolationType.TECHNICAL,
            violation_type_raw_text="TECHNICAL",
        )

        update_normalized_entity_with_globally_unique_id(
            person_id=person_id, entity=entity
        )

        expected_id_value = 9900000123450

        self.assertEqual(expected_id_value, entity.get_id())

    @mock.patch(
        "recidiviz.calculator.pipeline.normalization.utils."
        "normalized_entities_utils._add_entity_id_to_cache"
    )
    def test_update_normalized_entity_with_globally_unique_id_assert_added_to_index(
        self, mock_add_to_id_index: mock.MagicMock
    ) -> None:
        state_code = StateCode.US_XX
        person_id = 990000012345

        entity = entities.StateSupervisionViolationTypeEntry.new_with_defaults(
            supervision_violation_type_entry_id=456,
            state_code=state_code.value,
            violation_type=entities.StateSupervisionViolationType.TECHNICAL,
            violation_type_raw_text="TECHNICAL",
        )

        update_normalized_entity_with_globally_unique_id(
            person_id=person_id, entity=entity
        )

        expected_id_value = 99000001234588888

        self.assertEqual(expected_id_value, entity.get_id())
        mock_add_to_id_index.assert_called_with(
            person_id=person_id,
            entity_name="StateSupervisionViolationTypeEntry",
            entity_id=88888,
        )

    def test_update_normalized_entity_with_globally_unique_id_multiple(self) -> None:
        """Tests that the entity_id_index is working to ensure unique entity ids
        within the same entity type."""
        state_code = StateCode.US_XX
        person_id = 990000012345

        entity = entities.StateSupervisionViolationTypeEntry.new_with_defaults(
            supervision_violation_type_entry_id=456,
            state_code=state_code.value,
            violation_type=entities.StateSupervisionViolationType.TECHNICAL,
            violation_type_raw_text="TECHNICAL",
        )

        update_normalized_entity_with_globally_unique_id(
            person_id=person_id, entity=entity
        )

        expected_id_value = 99000001234588888
        self.assertEqual(expected_id_value, entity.get_id())

        # Entity of the same type, so should get incremented id value
        entity_2 = entities.StateSupervisionViolationTypeEntry.new_with_defaults(
            supervision_violation_type_entry_id=789,
            state_code=state_code.value,
            violation_type=entities.StateSupervisionViolationType.TECHNICAL,
            violation_type_raw_text="TECHNICAL",
        )

        update_normalized_entity_with_globally_unique_id(
            person_id=person_id, entity=entity_2
        )

        expected_id_value_2 = 99000001234588889
        self.assertEqual(expected_id_value_2, entity_2.get_id())

        # Entity of a new type, so can get un-incremented value
        entity_3 = entities.StateSupervisionPeriod.new_with_defaults(
            supervision_period_id=789,
            state_code=state_code.value,
        )

        update_normalized_entity_with_globally_unique_id(
            person_id=person_id, entity=entity_3
        )

        expected_id_value_3 = 99000001234588888
        self.assertEqual(expected_id_value_3, entity_3.get_id())

    def test_update_normalized_entity_with_globally_unique_id_max_int(self) -> None:
        """Tests that we raise an error if the person_id value is already the size of
        the maximum integer value in BigQuery."""
        state_code = StateCode.US_XX
        self.mock_safe_object_id_patcher.stop()

        entity = entities.StateSupervisionViolationTypeEntry.new_with_defaults(
            supervision_violation_type_entry_id=789,
            state_code=state_code.value,
            violation_type=entities.StateSupervisionViolationType.TECHNICAL,
            violation_type_raw_text="TECHNICAL",
        )

        person_id = MAX_BQ_INT

        with self.assertRaises(ValueError) as e:
            update_normalized_entity_with_globally_unique_id(
                person_id=person_id, entity=entity
            )

        self.assertEqual(
            "Our database person_id values have gotten too large and are clobbering "
            "the FIPS code values in the id mask used to write to BigQuery. Must fix "
            "person_id values before running more normalization pipelines.",
            e.exception.args[0],
        )

        self.mock_safe_object_id_patcher.start()


class TestCopyEntitiesAndAddUniqueIds(unittest.TestCase):
    """Tests the copy_entities_and_add_unique_ids function."""

    def setUp(self) -> None:
        clear_entity_id_index_cache()
        self.mock_safe_object_id_patcher = mock.patch(
            "recidiviz.calculator.pipeline.normalization."
            "utils.normalized_entities_utils._fixed_length_object_id_for_entity",
            return_value=88888,
        )
        self.mock_safe_object_id_patcher.start()

    def tearDown(self) -> None:
        self.mock_safe_object_id_patcher.stop()

    def test_copy_entities_and_add_unique_ids(self) -> None:
        state_code = StateCode.US_XX
        person_id = 990000012345

        entity_1 = entities.StateSupervisionViolationTypeEntry.new_with_defaults(
            supervision_violation_type_entry_id=456,
            state_code=state_code.value,
            violation_type=entities.StateSupervisionViolationType.TECHNICAL,
            violation_type_raw_text="TECHNICAL",
        )

        entity_2 = entities.StateSupervisionViolationTypeEntry.new_with_defaults(
            supervision_violation_type_entry_id=789,
            state_code=state_code.value,
            violation_type=entities.StateSupervisionViolationType.TECHNICAL,
            violation_type_raw_text="TECHNICAL",
        )

        updated_entities = copy_entities_and_add_unique_ids(
            person_id=person_id, entities=[entity_1, entity_2]
        )

        expected_entities = [
            attr.evolve(
                entity_1, supervision_violation_type_entry_id=99000001234588888
            ),
            attr.evolve(
                entity_2, supervision_violation_type_entry_id=99000001234588889
            ),
        ]

        self.assertEqual(expected_entities, updated_entities)
        self.assertNotEqual(id(entity_1), id(updated_entities[0]))

    def test_copy_entities_and_add_unique_ids_no_entities(self) -> None:
        person_id = 990000012345

        # Assert no error
        _ = copy_entities_and_add_unique_ids(person_id=person_id, entities=[])
