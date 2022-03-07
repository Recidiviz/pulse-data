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
from typing import Set

import attr
from google.cloud import bigquery
from google.cloud.bigquery.schema import SchemaField

from recidiviz.calculator.pipeline.utils.entity_normalization import normalized_entities
from recidiviz.calculator.pipeline.utils.entity_normalization.normalized_entities import (
    NormalizedStateEntity,
    NormalizedStateIncarcerationPeriod,
    NormalizedStateSupervisionCaseTypeEntry,
    NormalizedStateSupervisionViolatedConditionEntry,
    NormalizedStateSupervisionViolation,
    NormalizedStateSupervisionViolationResponse,
    NormalizedStateSupervisionViolationResponseDecisionEntry,
    NormalizedStateSupervisionViolationTypeEntry,
)
from recidiviz.calculator.pipeline.utils.entity_normalization.normalized_entities_utils import (
    NORMALIZED_ENTITY_CLASSES,
    AdditionalAttributesMap,
    bq_schema_for_normalized_state_entity,
    convert_entities_to_normalized_dicts,
    convert_entity_trees_to_normalized_versions,
    fields_unique_to_normalized_class,
    get_shared_additional_attributes_map_for_entities,
    merge_additional_attributes_maps,
)
from recidiviz.common.constants.state.state_incarceration_period import (
    StateSpecializedPurposeForIncarceration,
)
from recidiviz.persistence.entity.entity_utils import get_all_entity_classes_in_module
from recidiviz.persistence.entity.state import entities
from recidiviz.persistence.entity.state.entities import (
    StateIncarcerationPeriod,
    StateSupervisionViolation,
    StateSupervisionViolationResponse,
)
from recidiviz.tests.calculator.pipeline.utils.entity_normalization.supervision_violation_responses_normalization_manager_test import (
    hydrate_bidirectional_relationships_on_expected_response,
)


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


class TestBQSchemaForNormalizedStateEntity(unittest.TestCase):
    """Tests the bq_schema_for_normalized_state_entity function."""

    def test_bq_schema_for_normalized_state_entity(self) -> None:
        """Test that we can call this function for all NormalizedStateEntity entities
        without crashing"""
        for entity in NORMALIZED_ENTITY_CLASSES:
            _ = bq_schema_for_normalized_state_entity(entity)

    def test_bq_schema_for_normalized_state_entity_test_output(self) -> None:
        schema_for_entity = bq_schema_for_normalized_state_entity(
            NormalizedStateSupervisionViolatedConditionEntry
        )

        expected_schema = [
            SchemaField("state_code", bigquery.enums.SqlTypeNames.STRING.value),
            SchemaField("condition", bigquery.enums.SqlTypeNames.STRING.value),
            SchemaField(
                "supervision_violated_condition_entry_id",
                bigquery.enums.SqlTypeNames.INTEGER.value,
            ),
            SchemaField(
                "supervision_violation_id",
                bigquery.enums.SqlTypeNames.INTEGER.value,
            ),
            SchemaField("person_id", bigquery.enums.SqlTypeNames.INTEGER.value),
        ]

        self.assertEqual(expected_schema, schema_for_entity)

    def test_bq_schema_for_normalized_state_entity_extra_attributes(self) -> None:
        """Tests that fields that exist on a NormalizedStateEntity but not on the
        base entity are included in the schema columns."""
        normalized_ip = normalized_entities.NormalizedStateIncarcerationPeriod
        base_ip = StateIncarcerationPeriod

        normalized_ip_field_names = set(attr.fields_dict(normalized_ip).keys())
        base_ip_field_names = set(attr.fields_dict(base_ip).keys())
        fields_unique_to_norm_ip = normalized_ip_field_names.difference(
            base_ip_field_names
        )

        self.assertNotEqual(set(), fields_unique_to_norm_ip)

        norm_ip_schema = bq_schema_for_normalized_state_entity(normalized_ip)
        schema_cols = [field.name for field in norm_ip_schema]

        for field in fields_unique_to_norm_ip:
            self.assertIn(field, schema_cols)


class TestFieldsUniqueToNormalizedClass(unittest.TestCase):
    """Tests the fields_unique_to_normalized_class function."""

    def test_fields_unique_to_normalized_class(self) -> None:
        entity = NormalizedStateIncarcerationPeriod

        expected_unique_fields = {"sequence_num", "purpose_for_incarceration_subtype"}

        self.assertEqual(
            expected_unique_fields, fields_unique_to_normalized_class(entity)
        )

    def test_fields_unique_to_normalized_class_no_extra_fields(self) -> None:
        entity = NormalizedStateSupervisionCaseTypeEntry

        expected_unique_fields: Set[str] = set()

        self.assertEqual(
            expected_unique_fields, fields_unique_to_normalized_class(entity)
        )


class TestConvertEntityTreesToNormalizedVersions(unittest.TestCase):
    """Tests the convert_entity_trees_to_normalized_versions function."""

    def test_convert_entity_trees_to_normalized_versions(self):
        violation_with_tree = get_violation_tree()

        additional_attributes_map = {
            StateSupervisionViolationResponse.__name__: {
                111: {"sequence_num": 0},
                222: {"sequence_num": 1},
            }
        }

        normalized_trees = convert_entity_trees_to_normalized_versions(
            root_entities=[violation_with_tree],
            normalized_entity_class=NormalizedStateSupervisionViolation,
            additional_attributes_map=additional_attributes_map,
        )

        self.assertEqual([get_normalized_violation_tree()], normalized_trees)

    def test_convert_entity_trees_to_normalized_versions_ips(self):
        ips = [
            StateIncarcerationPeriod.new_with_defaults(
                state_code="US_XX",
                incarceration_period_id=111,
            ),
            StateIncarcerationPeriod.new_with_defaults(
                state_code="US_XX",
                incarceration_period_id=222,
            ),
        ]

        additional_attributes_map = {
            StateIncarcerationPeriod.__name__: {
                111: {"sequence_num": 0, "purpose_for_incarceration_subtype": "XYZ"},
                222: {"sequence_num": 1, "purpose_for_incarceration_subtype": "AAA"},
            }
        }

        normalized_trees = convert_entity_trees_to_normalized_versions(
            root_entities=ips,
            normalized_entity_class=NormalizedStateIncarcerationPeriod,
            additional_attributes_map=additional_attributes_map,
        )

        expected_normalized_ips = [
            NormalizedStateIncarcerationPeriod.new_with_defaults(
                state_code="US_XX",
                incarceration_period_id=111,
                sequence_num=0,
                purpose_for_incarceration_subtype="XYZ",
            ),
            NormalizedStateIncarcerationPeriod.new_with_defaults(
                state_code="US_XX",
                incarceration_period_id=222,
                sequence_num=1,
                purpose_for_incarceration_subtype="AAA",
            ),
        ]

        self.assertEqual(expected_normalized_ips, normalized_trees)

    def test_convert_entity_trees_to_normalized_versions_bad_root(self):
        """Tests that an error is raised if we try to normalize an entity where there
        are reverse relationship fields that are lists.

        We cannot send in StateSupervisionViolationResponses as root entities to
        normalization because the StateSupervisionViolation, a related entity,
        stores a list of StateSupervisionViolationResponses.
        """
        violation_with_tree = get_violation_tree()

        with self.assertRaises(ValueError) as e:
            _ = convert_entity_trees_to_normalized_versions(
                root_entities=violation_with_tree.supervision_violation_responses,
                normalized_entity_class=NormalizedStateSupervisionViolationResponse,
                additional_attributes_map={
                    StateSupervisionViolationResponse.__name__: {
                        111: {"sequence_num": 0},
                        222: {"sequence_num": 1},
                    }
                },
            )

        self.assertEqual(
            "Recursive normalization conversion cannot support "
            "reverse fields that store lists. Found entity of type "
            f"{entities.StateSupervisionViolation.__name__} with the field "
            f"[supervision_violation_responses] that stores a list of type "
            f"{StateSupervisionViolationResponse.__name__}. Try normalizing "
            f"this entity tree with the {entities.StateSupervisionViolation.__name__} class as the "
            f"root instead.",
            e.exception.args[0],
        )

    def test_convert_entity_trees_to_normalized_versions_empty_list(self):
        normalized_trees = convert_entity_trees_to_normalized_versions(
            root_entities=[],
            normalized_entity_class=NormalizedStateIncarcerationPeriod,
        )

        self.assertEqual([], normalized_trees)


class TestConvertEntitiesToNormalizedDicts(unittest.TestCase):
    """Tests the convert_entities_to_normalized_dicts function."""

    def test_convert_entities_to_normalized_dicts(self):
        person_id = 123

        entities_to_convert = [
            StateIncarcerationPeriod.new_with_defaults(
                state_code="US_XX",
                incarceration_period_id=111,
                specialized_purpose_for_incarceration=StateSpecializedPurposeForIncarceration.PAROLE_BOARD_HOLD,
                admission_date=datetime.date(2000, 1, 1),
            )
        ]

        additional_attributes_map: AdditionalAttributesMap = {
            StateIncarcerationPeriod.__name__: {
                111: {"sequence_num": 0, "purpose_for_incarceration_subtype": "XYZ"},
            }
        }

        expected_output = [
            (
                StateIncarcerationPeriod.__name__,
                {
                    "admission_date": datetime.date(2000, 1, 1),
                    "admission_reason": None,
                    "admission_reason_raw_text": None,
                    "county_code": None,
                    "custodial_authority": None,
                    "custodial_authority_raw_text": None,
                    "external_id": None,
                    "facility": None,
                    "facility_security_level": None,
                    "facility_security_level_raw_text": None,
                    "housing_unit": None,
                    "incarceration_period_id": 111,
                    "incarceration_type": None,
                    "incarceration_type_raw_text": None,
                    "projected_release_reason": None,
                    "projected_release_reason_raw_text": None,
                    "release_date": None,
                    "release_reason": None,
                    "release_reason_raw_text": None,
                    "specialized_purpose_for_incarceration": "PAROLE_BOARD_HOLD",
                    "specialized_purpose_for_incarceration_raw_text": None,
                    "state_code": "US_XX",
                    "sequence_num": 0,
                    "purpose_for_incarceration_subtype": "XYZ",
                    "person_id": 123,
                },
            )
        ]

        converted_output = convert_entities_to_normalized_dicts(
            person_id=person_id,
            entities=entities_to_convert,
            additional_attributes_map=additional_attributes_map,
        )

        self.assertEqual(expected_output, converted_output)

    def test_convert_entities_to_normalized_dicts_violations(self):
        person_id = 123

        entities_to_convert = [get_violation_tree()]

        additional_attributes_map: AdditionalAttributesMap = (
            get_shared_additional_attributes_map_for_entities(entities_to_convert)
        )

        # TODO(#10724): Update this test once convert_entities_to_normalized_dicts is
        #  recursive
        expected_output = [
            (
                StateSupervisionViolation.__name__,
                {
                    "external_id": None,
                    "is_sex_offense": None,
                    "is_violent": False,
                    "person_id": 123,
                    "state_code": "US_XX",
                    "supervision_violation_id": 123,
                    "violation_date": datetime.date(2004, 9, 1),
                },
            )
        ]

        converted_output = convert_entities_to_normalized_dicts(
            person_id=person_id,
            entities=entities_to_convert,
            additional_attributes_map=additional_attributes_map,
        )

        self.assertEqual(expected_output, converted_output)


class TestMergeAdditionalAttributesMaps(unittest.TestCase):
    """Tests the merge_additional_attributes_maps function."""

    def test_merge_additional_attributes_maps(self):
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

    def test_merge_additional_attributes_maps_diff_entity_types(self):
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

    def test_merge_additional_attributes_maps_diff_entity_ids(self):
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

    def test_merge_additional_attributes_maps_empty_map(self):
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

    def test_merge_additional_attributes_maps_empty_map_with_name(self):
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

    def test_get_shared_additional_attributes_map_for_entities(self):
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

    def test_get_shared_additional_attributes_map_for_entities_not_sequenced(self):
        entities_for_map = [get_violation_tree()]

        attributes_map = get_shared_additional_attributes_map_for_entities(
            entities=entities_for_map
        )

        expected_attributes_map: AdditionalAttributesMap = {
            StateSupervisionViolation.__name__: {}
        }

        self.assertEqual(expected_attributes_map, attributes_map)


def get_violation_tree() -> entities.StateSupervisionViolation:
    """Returns a tree of entities connected to the StateSupervisionViolation
    for use in tests that normalize violation data.

    DO NOT UPDATE THIS WITHOUT ALSO UPDATING get_normalized_violation_tree.
    """
    supervision_violation = entities.StateSupervisionViolation.new_with_defaults(
        supervision_violation_id=123,
        violation_date=datetime.date(year=2004, month=9, day=1),
        state_code="US_XX",
        is_violent=False,
        supervision_violation_types=[
            entities.StateSupervisionViolationTypeEntry.new_with_defaults(
                supervision_violation_type_entry_id=456,
                state_code="US_XX",
                violation_type=entities.StateSupervisionViolationType.TECHNICAL,
                violation_type_raw_text="TECHNICAL",
            ),
        ],
        supervision_violated_conditions=[
            entities.StateSupervisionViolatedConditionEntry.new_with_defaults(
                supervision_violated_condition_entry_id=789,
                state_code="US_XX",
                condition="MISSED CURFEW",
            )
        ],
    )

    supervision_violation_response_1 = entities.StateSupervisionViolationResponse.new_with_defaults(
        supervision_violation_response_id=111,
        response_type=entities.StateSupervisionViolationResponseType.CITATION,
        response_date=datetime.date(year=2004, month=9, day=2),
        state_code="US_XX",
        deciding_body_type=entities.StateSupervisionViolationResponseDecidingBodyType.SUPERVISION_OFFICER,
        supervision_violation=supervision_violation,
        supervision_violation_response_decisions=[
            entities.StateSupervisionViolationResponseDecisionEntry.new_with_defaults(
                state_code="US_XX",
                supervision_violation_response_decision_entry_id=1,
                decision_raw_text="X",
            )
        ],
    )

    supervision_violation_response_2 = entities.StateSupervisionViolationResponse.new_with_defaults(
        supervision_violation_response_id=222,
        response_type=entities.StateSupervisionViolationResponseType.VIOLATION_REPORT,
        response_date=datetime.date(year=2004, month=10, day=3),
        state_code="US_XX",
        deciding_body_type=entities.StateSupervisionViolationResponseDecidingBodyType.SUPERVISION_OFFICER,
        supervision_violation=supervision_violation,
        supervision_violation_response_decisions=[
            entities.StateSupervisionViolationResponseDecisionEntry.new_with_defaults(
                state_code="US_XX",
                supervision_violation_response_decision_entry_id=2,
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


def get_normalized_violation_tree() -> NormalizedStateSupervisionViolation:
    """Returns a tree of normalized versions of the entities returned by
    _get_violation_response_tree."""
    supervision_violation = NormalizedStateSupervisionViolation.new_with_defaults(
        supervision_violation_id=123,
        violation_date=datetime.date(year=2004, month=9, day=1),
        state_code="US_XX",
        is_violent=False,
        supervision_violation_types=[
            NormalizedStateSupervisionViolationTypeEntry.new_with_defaults(
                state_code="US_XX",
                supervision_violation_type_entry_id=456,
                violation_type=entities.StateSupervisionViolationType.TECHNICAL,
                violation_type_raw_text="TECHNICAL",
            ),
        ],
        supervision_violated_conditions=[
            NormalizedStateSupervisionViolatedConditionEntry.new_with_defaults(
                supervision_violated_condition_entry_id=789,
                state_code="US_XX",
                condition="MISSED CURFEW",
            )
        ],
    )

    supervision_violation_response_1 = NormalizedStateSupervisionViolationResponse.new_with_defaults(
        supervision_violation_response_id=111,
        response_type=entities.StateSupervisionViolationResponseType.CITATION,
        response_date=datetime.date(year=2004, month=9, day=2),
        state_code="US_XX",
        deciding_body_type=entities.StateSupervisionViolationResponseDecidingBodyType.SUPERVISION_OFFICER,
        sequence_num=0,
        supervision_violation=supervision_violation,
        supervision_violation_response_decisions=[
            NormalizedStateSupervisionViolationResponseDecisionEntry.new_with_defaults(
                state_code="US_XX",
                supervision_violation_response_decision_entry_id=1,
                decision_raw_text="X",
            )
        ],
    )

    supervision_violation_response_2 = NormalizedStateSupervisionViolationResponse.new_with_defaults(
        supervision_violation_response_id=222,
        response_type=entities.StateSupervisionViolationResponseType.VIOLATION_REPORT,
        response_date=datetime.date(year=2004, month=10, day=3),
        state_code="US_XX",
        deciding_body_type=entities.StateSupervisionViolationResponseDecidingBodyType.SUPERVISION_OFFICER,
        sequence_num=1,
        supervision_violation=supervision_violation,
        supervision_violation_response_decisions=[
            NormalizedStateSupervisionViolationResponseDecisionEntry.new_with_defaults(
                state_code="US_XX",
                supervision_violation_response_decision_entry_id=2,
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
