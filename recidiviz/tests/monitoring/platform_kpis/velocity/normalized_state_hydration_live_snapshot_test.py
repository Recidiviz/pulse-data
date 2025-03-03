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
"""Unit tests for logic that comprises the normalized state hydration live snapshot bq 
view
"""
from importlib import import_module
from types import ModuleType
from unittest import TestCase
from unittest.mock import patch

import attr

from recidiviz.monitoring.platform_kpis.velocity.normalized_state_hydration_live_snapshot import (
    _build_query_for_entity_cls,
    _get_functionally_non_optional_fields_for_normalized_entity,
)
from recidiviz.persistence.entity.base_entity import Entity, RootEntity
from recidiviz.persistence.entity.entities_module_context import EntitiesModuleContext
from recidiviz.persistence.entity.normalized_entities_utils import NormalizedStateEntity
from recidiviz.persistence.entity.state.state_entity_mixins import SequencedEntityMixin


@attr.s(eq=False, kw_only=True)
class FakeRootNormalizedEntity(NormalizedStateEntity, Entity, RootEntity):
    pk: str = attr.ib()

    @classmethod
    def back_edge_field_name(cls) -> str:
        return "root"


@attr.s(eq=False, kw_only=True)
class FakeSimpleNormalizedEntity(NormalizedStateEntity, Entity):

    pk: str = attr.ib()
    optional_string: str | None = attr.ib()
    optional_int: int | None = attr.ib()

    complex: "FakeComplexNormalizedEntity" = attr.ib()
    root: "FakeRootNormalizedEntity" = attr.ib()


@attr.s(eq=False, kw_only=True)
class FakeComplexNormalizedEntity(NormalizedStateEntity, Entity):

    pk: str = attr.ib()
    root: "FakeRootNormalizedEntity" = attr.ib()
    simple: list["FakeSimpleNormalizedEntity"] = attr.ib()
    optional_int: int | None = attr.ib()


@attr.s(eq=False, kw_only=True)
class FakeSequencedNormalizedEntity(
    NormalizedStateEntity, SequencedEntityMixin, Entity
):

    pk: str = attr.ib()
    root: "FakeRootNormalizedEntity" = attr.ib()


def _get_module_ctx(module: ModuleType) -> EntitiesModuleContext:
    class _FakeEntityModuleContext(EntitiesModuleContext):
        @classmethod
        def entities_module(cls) -> ModuleType:
            return module

        @classmethod
        def class_hierarchy(cls) -> list[str]:
            return [
                FakeRootNormalizedEntity.__name__,
                FakeComplexNormalizedEntity.__name__,
                FakeSimpleNormalizedEntity.__name__,
                FakeSequencedNormalizedEntity.__name__,
            ]

    return _FakeEntityModuleContext()


# pylint: disable=protected-access
class TestNormalizedStateHydrationLiveSnapShot(TestCase):
    """Unit tests for logic that comprises the normalized state hydration live snapshot
    bq view
    """

    _fake_module: ModuleType
    _module_ctx: EntitiesModuleContext

    @classmethod
    def setUpClass(cls) -> None:
        cls._fake_module = import_module(__name__)
        cls._module_ctx = _get_module_ctx(cls._fake_module)

    def setUp(self) -> None:
        self._bq_schema_patcher = patch(
            "recidiviz.persistence.entity.entities_bq_schema.entities_module_context_for_module",
            return_value=self._module_ctx,
        )
        self._bq_schema_patcher.start()
        self._description_patcher = patch(
            "recidiviz.persistence.entity.entities_bq_schema.description_for_field",
            return_value=None,
        )
        self._description_patcher.start()
        self._module_patcher = patch(
            "recidiviz.monitoring.platform_kpis.velocity.normalized_state_hydration_live_snapshot.normalized_entities",
            self._fake_module,
        )
        self._module_patcher.start()

    def tearDown(self) -> None:
        self._bq_schema_patcher.stop()
        self._description_patcher.stop()
        self._module_patcher.stop()

    def test_optional_fields(self) -> None:
        functionally_non_optional_fields = (
            _get_functionally_non_optional_fields_for_normalized_entity(
                entity_cls=FakeSimpleNormalizedEntity, module_ctx=self._module_ctx
            )
        )

        assert functionally_non_optional_fields == ["optional_int", "optional_string"]

        functionally_non_optional_fields = (
            _get_functionally_non_optional_fields_for_normalized_entity(
                entity_cls=FakeComplexNormalizedEntity, module_ctx=self._module_ctx
            )
        )

        assert functionally_non_optional_fields == ["optional_int"]

        functionally_non_optional_fields = (
            _get_functionally_non_optional_fields_for_normalized_entity(
                entity_cls=FakeSequencedNormalizedEntity, module_ctx=self._module_ctx
            )
        )

        assert functionally_non_optional_fields == []

    def test_query_construction(self) -> None:

        query = _build_query_for_entity_cls(
            entity_cls=FakeSimpleNormalizedEntity, module_ctx=self._module_ctx
        )

        assert (
            query
            == """
SELECT
    state_code,
    table_name,
    entity_count,
    column_hydration_counts,
    -- counts 1 for all required fields, and one additional point for each optional flat field that is hydrated
    1 + (SELECT COUNTIF(col.column_name in ('optional_int', 'optional_string') and col.hydrated_count > 0) FROM UNNEST(column_hydration_counts) as col) as column_hydration_score
FROM (
    SELECT
        state_code,
        'fake_simple_normalized_entity' as table_name,
        count(*) as entity_count,
        [
          STRUCT('state_code' AS column_name, COUNTIF(state_code IS NOT NULL) AS hydrated_count),
          STRUCT('pk' AS column_name, COUNTIF(pk IS NOT NULL) AS hydrated_count),
          STRUCT('optional_string' AS column_name, COUNTIF(optional_string IS NOT NULL) AS hydrated_count),
          STRUCT('optional_int' AS column_name, COUNTIF(optional_int IS NOT NULL) AS hydrated_count),
          STRUCT('fake_complex_normalized_entity_id' AS column_name, COUNTIF(fake_complex_normalized_entity_id IS NOT NULL) AS hydrated_count),
          STRUCT('fake_root_normalized_entity_id' AS column_name, COUNTIF(fake_root_normalized_entity_id IS NOT NULL) AS hydrated_count)
        ] as column_hydration_counts,
    FROM `{project_id}.{normalized_state_dataset_id}.fake_simple_normalized_entity`
    GROUP BY state_code, table_name
)
"""
        )

        query = _build_query_for_entity_cls(
            entity_cls=FakeComplexNormalizedEntity, module_ctx=self._module_ctx
        )

        assert (
            query
            == """
SELECT
    state_code,
    table_name,
    entity_count,
    column_hydration_counts,
    -- counts 1 for all required fields, and one additional point for each optional flat field that is hydrated
    1 + (SELECT COUNTIF(col.column_name in ('optional_int') and col.hydrated_count > 0) FROM UNNEST(column_hydration_counts) as col) as column_hydration_score
FROM (
    SELECT
        state_code,
        'fake_complex_normalized_entity' as table_name,
        count(*) as entity_count,
        [
          STRUCT('state_code' AS column_name, COUNTIF(state_code IS NOT NULL) AS hydrated_count),
          STRUCT('pk' AS column_name, COUNTIF(pk IS NOT NULL) AS hydrated_count),
          STRUCT('fake_root_normalized_entity_id' AS column_name, COUNTIF(fake_root_normalized_entity_id IS NOT NULL) AS hydrated_count),
          STRUCT('optional_int' AS column_name, COUNTIF(optional_int IS NOT NULL) AS hydrated_count)
        ] as column_hydration_counts,
    FROM `{project_id}.{normalized_state_dataset_id}.fake_complex_normalized_entity`
    GROUP BY state_code, table_name
)
"""
        )

        query = _build_query_for_entity_cls(
            entity_cls=FakeSequencedNormalizedEntity, module_ctx=self._module_ctx
        )

        assert (
            query
            == """
SELECT
    state_code,
    table_name,
    entity_count,
    column_hydration_counts,
    -- counts 1 for all required fields, and one additional point for each optional flat field that is hydrated
    1 as column_hydration_score
FROM (
    SELECT
        state_code,
        'fake_sequenced_normalized_entity' as table_name,
        count(*) as entity_count,
        [
          STRUCT('state_code' AS column_name, COUNTIF(state_code IS NOT NULL) AS hydrated_count),
          STRUCT('sequence_num' AS column_name, COUNTIF(sequence_num IS NOT NULL) AS hydrated_count),
          STRUCT('pk' AS column_name, COUNTIF(pk IS NOT NULL) AS hydrated_count),
          STRUCT('fake_root_normalized_entity_id' AS column_name, COUNTIF(fake_root_normalized_entity_id IS NOT NULL) AS hydrated_count)
        ] as column_hydration_counts,
    FROM `{project_id}.{normalized_state_dataset_id}.fake_sequenced_normalized_entity`
    GROUP BY state_code, table_name
)
"""
        )
