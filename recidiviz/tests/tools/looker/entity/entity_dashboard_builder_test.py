# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2025 Recidiviz, Inc.
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
"""Tests for recidiviz.tools.looker.entity.entity_dashboard_builder."""
import unittest
from typing import Any
from unittest.mock import patch

import attr

from recidiviz.big_query.big_query_address import BigQueryAddress
from recidiviz.ingest.views.dataset_config import STATE_BASE_DATASET
from recidiviz.looker.lookml_dashboard_builder import (
    LookMLDashboardElementMetadata,
    LookMLDashboardElementsProvider,
)
from recidiviz.looker.lookml_dashboard_element import (
    LookMLDashboardElement,
    LookMLListen,
)
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
from recidiviz.persistence.entity.entities_module_context_factory import (
    entities_module_context_for_module,
)
from recidiviz.persistence.entity.state import entities as state_entities
from recidiviz.persistence.entity.state.state_entity_mixins import StateEntityMixin
from recidiviz.tests.persistence.entity import fake_entities
from recidiviz.tests.persistence.entity.fake_entities_module_context import (
    FakeEntitiesModuleContext,
)
from recidiviz.tools.looker.entity.entity_dashboard_builder import (
    EntityLookMLDashboardBuilder,
)
from recidiviz.tools.looker.entity.entity_views_builder import (
    generate_entity_lookml_views,
)

LOOKML_VIEWS = [
    LookMLView(
        view_name="fake_another_entity",
        table=SqlTableAddress(
            address=BigQueryAddress(dataset_id="state", table_id="fake_another_entity")
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
                    FieldParameterSql(sql_text="${TABLE}.fake_another_entity_id"),
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
    LookMLView(
        view_name="fake_person_external_id",
        table=SqlTableAddress(
            address=BigQueryAddress(
                dataset_id="state", table_id="fake_person_external_id"
            )
        ),
        fields=[
            DimensionLookMLViewField(
                field_name="external_id",
                parameters=[
                    FieldParameterType(field_type=LookMLFieldType.STRING),
                    FieldParameterSql(sql_text="${TABLE}.external_id"),
                ],
            ),
            DimensionLookMLViewField(
                field_name="fake_person_external_id_id",
                parameters=[
                    FieldParameterType(field_type=LookMLFieldType.NUMBER),
                    FieldParameterPrimaryKey(is_primary_key=True),
                    FieldParameterValueFormat(value="0"),
                    FieldParameterSql(sql_text="${TABLE}.fake_person_external_id_id"),
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
                field_name="id_type",
                parameters=[
                    FieldParameterType(field_type=LookMLFieldType.STRING),
                    FieldParameterSql(sql_text="${TABLE}.id_type"),
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


@attr.define
class FakePersonDashboardElementsProvider(LookMLDashboardElementsProvider):
    def build_dashboard_elements(
        self, explore_name: str, all_filters_listen: LookMLListen, model: str
    ) -> list[LookMLDashboardElement]:
        """Builds LookML dashboard elements for FakePerson."""
        return [
            LookMLDashboardElement.for_table_chart(
                name=table_metadata.name,
                explore=explore_name,
                listen=all_filters_listen,
                fields=table_metadata.fields,
                sorts=table_metadata.sort_fields,
                model=model,
            )
            for table_metadata in self.table_element_metadata
        ]


class EntityDashboardBuilderTest(unittest.TestCase):
    """Tests for the state dataset dashboard builder."""

    def setUp(self) -> None:
        self.original_issubclass = issubclass
        self.patcher_issubclass = patch(
            "builtins.issubclass",
            side_effect=lambda cls, base: (
                True
                if (cls is fake_entities.FakePerson and base is StateEntityMixin)
                else self.original_issubclass(cls, base)
            ),
        )
        self.mock_issubclass = self.patcher_issubclass.start()

        def _mock_get_elements_provider(
            _root_entity_cls: Any,
            _dataset_id: str,
            table_element_metadata: list[LookMLDashboardElementMetadata],
        ) -> LookMLDashboardElementsProvider:
            return FakePersonDashboardElementsProvider(table_element_metadata)

        self.patcher_get_elements_provider = patch(
            "recidiviz.tools.looker.entity.entity_dashboard_builder.get_elements_provider",
            side_effect=_mock_get_elements_provider,
        ).start()

    def tearDown(self) -> None:
        self.patcher_issubclass.stop()
        self.patcher_get_elements_provider.stop()

    def test_build_dashboard(self) -> None:
        expected_dashboard = """- dashboard: fake_person
  layout: newspaper
  title: Fake Person
  load_configuration: wait

  filters:
  - name: Fake Person Id
    title: Fake Person Id
    type: field_filter
    allow_multiple_values: true
    required: false
    ui_config: 
      type: advanced
      display: popover
    model: "@{project_id}"
    explore: fake_person
    field: fake_person.fake_person_id

  - name: State Code
    title: State Code
    type: field_filter
    allow_multiple_values: true
    required: false
    ui_config: 
      type: advanced
      display: popover
    model: "@{project_id}"
    explore: fake_person
    field: fake_person.state_code

  - name: External Id
    title: External Id
    type: field_filter
    allow_multiple_values: true
    required: false
    ui_config: 
      type: advanced
      display: popover
    model: "@{project_id}"
    explore: fake_person
    field: fake_person_external_id.external_id

  - name: Id Type
    title: Id Type
    type: field_filter
    allow_multiple_values: true
    required: false
    ui_config: 
      type: advanced
      display: popover
    model: "@{project_id}"
    explore: fake_person
    field: fake_person_external_id.id_type

  elements:
  - name: Fake Another Entity
    title: Fake Another Entity
    explore: fake_person
    model: "@{project_id}"
    type: looker_grid
    fields: [fake_another_entity.another_entity_id,
      fake_another_entity.another_name,
      fake_another_entity.state_code]
    sorts: []
    listen: 
      Fake Person Id: fake_person.fake_person_id
      State Code: fake_person.state_code
      External Id: fake_person_external_id.external_id
      Id Type: fake_person_external_id.id_type
    row: 0
    col: 0
    width: 12
    height: 6

  - name: Fake Entity
    title: Fake Entity
    explore: fake_person
    model: "@{project_id}"
    type: looker_grid
    fields: [fake_entity.entity_id,
      fake_entity.name,
      fake_entity.state_code]
    sorts: []
    listen: 
      Fake Person Id: fake_person.fake_person_id
      State Code: fake_person.state_code
      External Id: fake_person_external_id.external_id
      Id Type: fake_person_external_id.id_type
    row: 0
    col: 12
    width: 12
    height: 6

  - name: Fake Person
    title: Fake Person
    explore: fake_person
    model: "@{project_id}"
    type: looker_grid
    fields: [fake_person.fake_person_id,
      fake_person.full_name,
      fake_person.state_code]
    sorts: []
    listen: 
      Fake Person Id: fake_person.fake_person_id
      State Code: fake_person.state_code
      External Id: fake_person_external_id.external_id
      Id Type: fake_person_external_id.id_type
    row: 6
    col: 0
    width: 12
    height: 6

  - name: Fake Person External Id
    title: Fake Person External Id
    explore: fake_person
    model: "@{project_id}"
    type: looker_grid
    fields: [fake_person_external_id.external_id,
      fake_person_external_id.fake_person_external_id_id,
      fake_person_external_id.id_type,
      fake_person_external_id.state_code]
    sorts: []
    listen: 
      Fake Person Id: fake_person.fake_person_id
      State Code: fake_person.state_code
      External Id: fake_person_external_id.external_id
      Id Type: fake_person_external_id.id_type
    row: 6
    col: 12
    width: 12
    height: 6
"""

        dashboard = EntityLookMLDashboardBuilder(
            module_context=FakeEntitiesModuleContext(),
            root_entity_cls=fake_entities.FakePerson,
            views=LOOKML_VIEWS,
            dataset_id=STATE_BASE_DATASET,
        ).build_and_validate()

        self.assertEqual(dashboard.build(), expected_dashboard)


class TestStateDashboardBuilder(unittest.TestCase):
    """Tests for building state entity dashboards."""

    def test_generate_state_dashboards(self) -> None:
        views = generate_entity_lookml_views(
            dataset_id=STATE_BASE_DATASET, entities_module=state_entities
        )
        module_context = entities_module_context_for_module(state_entities)
        root_entities = [state_entities.StatePerson, state_entities.StateStaff]
        for root_entity_cls in root_entities:
            # Assert doesn't crash
            _ = EntityLookMLDashboardBuilder(
                module_context=module_context,
                root_entity_cls=root_entity_cls,
                views=views,
                dataset_id=STATE_BASE_DATASET,
            ).build_and_validate()
