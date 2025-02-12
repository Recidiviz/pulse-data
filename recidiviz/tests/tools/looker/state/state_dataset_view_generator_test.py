# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2023 Recidiviz, Inc.
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
"""Unit tests for state entity LookML View generation"""
import unittest

from mock import MagicMock, patch

from recidiviz.big_query.big_query_address import BigQueryAddress
from recidiviz.looker.lookml_view import LookMLView
from recidiviz.looker.lookml_view_field import (
    DimensionLookMLViewField,
    LookMLFieldType,
    MeasureLookMLViewField,
)
from recidiviz.looker.lookml_view_field_parameter import (
    FieldParameterDrillFields,
    FieldParameterHidden,
    FieldParameterPrimaryKey,
    FieldParameterSql,
    FieldParameterType,
    FieldParameterValueFormat,
)
from recidiviz.looker.lookml_view_source_table import SqlTableAddress
from recidiviz.tests.persistence.database.schema_entity_converter.fake_entities_module_context import (
    FakeEntitiesModuleContext,
)
from recidiviz.tests.persistence.entity import fake_entities
from recidiviz.tools.looker.state.state_dataset_view_generator import (
    generate_state_views,
)

# TODO(#23292) Remove fixture files from old tests


class StateViewGenerator(unittest.TestCase):
    """Tests LookML view generation functions for states"""

    def setUp(self) -> None:
        self.module_context_patchers = [
            patch(
                "recidiviz.persistence.entity.entities_bq_schema.entities_module_context_for_module",
                return_value=FakeEntitiesModuleContext(),
            ),
            patch(
                "recidiviz.persistence.entity.entity_metadata_helper.entities_module_context_for_module",
                return_value=FakeEntitiesModuleContext(),
            ),
        ]
        for patcher in self.module_context_patchers:
            patcher.start()

    def tearDown(self) -> None:
        for patcher in self.module_context_patchers:
            patcher.stop()

    @patch(
        "recidiviz.tools.looker.state.state_dataset_view_generator.ENTITIES_MODULE",
        fake_entities,
    )
    @patch(
        "recidiviz.persistence.entity.entity_metadata_helper.get_entities_by_association_table_id",
        return_value=(fake_entities.FakeAnotherEntity, fake_entities.FakeEntity),
    )
    def test_generate_lookml_views(self, _mock_get_entities: MagicMock) -> None:
        expected_views = [
            LookMLView(
                view_name="fake_another_entity",
                table=SqlTableAddress(
                    address=BigQueryAddress(
                        dataset_id="state", table_id="fake_another_entity"
                    )
                ),
                fields=[
                    DimensionLookMLViewField(
                        field_name="another_entity_id",
                        parameters=[
                            FieldParameterType(field_type=LookMLFieldType.NUMBER),
                            FieldParameterValueFormat(value="0"),
                            FieldParameterSql(sql_text="${TABLE}.another_entity_id"),
                        ],
                    ),
                    DimensionLookMLViewField(
                        field_name="another_name",
                        parameters=[
                            FieldParameterType(field_type=LookMLFieldType.STRING),
                            FieldParameterSql(sql_text="${TABLE}.another_name"),
                        ],
                    ),
                    DimensionLookMLViewField(
                        field_name="fake_person_id",
                        parameters=[
                            FieldParameterType(field_type=LookMLFieldType.NUMBER),
                            FieldParameterHidden(is_hidden=True),
                            FieldParameterValueFormat(value="0"),
                            FieldParameterSql(sql_text="${TABLE}.fake_person_id"),
                        ],
                    ),
                    DimensionLookMLViewField(
                        field_name="state_code",
                        parameters=[
                            FieldParameterType(field_type=LookMLFieldType.STRING),
                            FieldParameterSql(sql_text="${TABLE}.state_code"),
                        ],
                    ),
                    MeasureLookMLViewField(
                        field_name="count",
                        parameters=[
                            FieldParameterType(field_type=LookMLFieldType.COUNT),
                            FieldParameterDrillFields(fields=[]),
                        ],
                    ),
                ],
            ),
            LookMLView(
                view_name="fake_another_entity_fake_entity_association",
                table=SqlTableAddress(
                    address=BigQueryAddress(
                        dataset_id="state",
                        table_id="fake_another_entity_fake_entity_association",
                    )
                ),
                fields=[
                    DimensionLookMLViewField(
                        field_name="fake_another_entity_id",
                        parameters=[
                            FieldParameterType(field_type=LookMLFieldType.NUMBER),
                            FieldParameterValueFormat(value="0"),
                            FieldParameterSql(
                                sql_text="${TABLE}.fake_another_entity_id"
                            ),
                        ],
                    ),
                    DimensionLookMLViewField(
                        field_name="fake_entity_id",
                        parameters=[
                            FieldParameterType(field_type=LookMLFieldType.NUMBER),
                            FieldParameterValueFormat(value="0"),
                            FieldParameterSql(sql_text="${TABLE}.fake_entity_id"),
                        ],
                    ),
                    DimensionLookMLViewField(
                        field_name="primary_key",
                        parameters=[
                            FieldParameterType(field_type=LookMLFieldType.STRING),
                            FieldParameterPrimaryKey(is_primary_key=True),
                            FieldParameterSql(
                                sql_text='CONCAT(${TABLE}.fake_another_entity_id, "_", ${TABLE}.fake_entity_id)'
                            ),
                        ],
                    ),
                    DimensionLookMLViewField(
                        field_name="state_code",
                        parameters=[
                            FieldParameterType(field_type=LookMLFieldType.STRING),
                            FieldParameterSql(sql_text="${TABLE}.state_code"),
                        ],
                    ),
                    MeasureLookMLViewField(
                        field_name="count",
                        parameters=[
                            FieldParameterType(field_type=LookMLFieldType.COUNT),
                            FieldParameterDrillFields(fields=[]),
                        ],
                    ),
                ],
            ),
            LookMLView(
                view_name="fake_entity",
                table=SqlTableAddress(
                    address=BigQueryAddress(dataset_id="state", table_id="fake_entity")
                ),
                fields=[
                    DimensionLookMLViewField(
                        field_name="entity_id",
                        parameters=[
                            FieldParameterType(field_type=LookMLFieldType.NUMBER),
                            FieldParameterValueFormat(value="0"),
                            FieldParameterSql(sql_text="${TABLE}.entity_id"),
                        ],
                    ),
                    DimensionLookMLViewField(
                        field_name="fake_person_id",
                        parameters=[
                            FieldParameterType(field_type=LookMLFieldType.NUMBER),
                            FieldParameterHidden(is_hidden=True),
                            FieldParameterValueFormat(value="0"),
                            FieldParameterSql(sql_text="${TABLE}.fake_person_id"),
                        ],
                    ),
                    DimensionLookMLViewField(
                        field_name="name",
                        parameters=[
                            FieldParameterType(field_type=LookMLFieldType.STRING),
                            FieldParameterSql(sql_text="${TABLE}.name"),
                        ],
                    ),
                    DimensionLookMLViewField(
                        field_name="state_code",
                        parameters=[
                            FieldParameterType(field_type=LookMLFieldType.STRING),
                            FieldParameterSql(sql_text="${TABLE}.state_code"),
                        ],
                    ),
                    MeasureLookMLViewField(
                        field_name="count",
                        parameters=[
                            FieldParameterType(field_type=LookMLFieldType.COUNT),
                            FieldParameterDrillFields(fields=[]),
                        ],
                    ),
                ],
            ),
            LookMLView(
                view_name="fake_person",
                table=SqlTableAddress(
                    address=BigQueryAddress(dataset_id="state", table_id="fake_person")
                ),
                fields=[
                    DimensionLookMLViewField(
                        field_name="fake_person_id",
                        parameters=[
                            FieldParameterType(field_type=LookMLFieldType.NUMBER),
                            FieldParameterPrimaryKey(is_primary_key=True),
                            FieldParameterValueFormat(value="0"),
                            FieldParameterSql(sql_text="${TABLE}.fake_person_id"),
                        ],
                    ),
                    DimensionLookMLViewField(
                        field_name="full_name",
                        parameters=[
                            FieldParameterType(field_type=LookMLFieldType.STRING),
                            FieldParameterSql(sql_text="${TABLE}.full_name"),
                        ],
                    ),
                    DimensionLookMLViewField(
                        field_name="state_code",
                        parameters=[
                            FieldParameterType(field_type=LookMLFieldType.STRING),
                            FieldParameterSql(sql_text="${TABLE}.state_code"),
                        ],
                    ),
                    MeasureLookMLViewField(
                        field_name="count",
                        parameters=[
                            FieldParameterType(field_type=LookMLFieldType.COUNT),
                            FieldParameterDrillFields(fields=[]),
                        ],
                    ),
                ],
            ),
        ]
        for i, view in enumerate(
            sorted(generate_state_views(), key=lambda v: v.view_name)
        ):
            expected_view = expected_views[i]
            self.assertEqual(view.table, expected_view.table)
            self.assertEqual(view.view_name, expected_view.view_name)
            self.assertEqual(len(view.fields), len(expected_view.fields))
            for j, field in enumerate(view.fields):
                expected_field = expected_view.fields[j]
                self.assertEqual(field.field_name, expected_field.field_name)
                self.assertEqual(field.parameters, expected_field.parameters)
