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
)
from recidiviz.calculator.pipeline.utils.entity_normalization.normalized_entities_utils import (
    NORMALIZED_ENTITY_CLASSES,
    bq_schema_for_normalized_state_entity,
    fields_unique_to_normalized_class,
)
from recidiviz.persistence.entity.entity_utils import get_all_entity_classes_in_module
from recidiviz.persistence.entity.state.entities import StateIncarcerationPeriod


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
