# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2026 Recidiviz, Inc.
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
"""Tests for the GenerateEntities PTransform."""

import datetime
import unittest

from recidiviz.common.constants.states import StateCode
from recidiviz.ingest.direct.direct_ingest_regions import get_direct_ingest_region
from recidiviz.ingest.direct.ingest_mappings.ingest_view_contents_context import (
    IngestViewContentsContext,
)
from recidiviz.ingest.direct.ingest_mappings.ingest_view_manifest_compiler import (
    IngestViewManifest,
    IngestViewManifestCompiler,
)
from recidiviz.ingest.direct.ingest_mappings.ingest_view_manifest_compiler_delegate import (
    StateSchemaIngestViewManifestCompilerDelegate,
)
from recidiviz.ingest.direct.types.direct_ingest_constants import (
    MATERIALIZATION_TIME_COL_NAME,
    UPPER_BOUND_DATETIME_COL_NAME,
)
from recidiviz.persistence.entity.state.entities import StatePerson, StateStaff
from recidiviz.pipelines.transforms.generate_entities import GenerateEntities
from recidiviz.tests.ingest.direct import fake_regions

_CONTEXT = IngestViewContentsContext(
    is_local=True,
    is_staging=False,
    is_production=False,
    is_sandbox=False,
    state_code=StateCode.US_XX,
)

_UPPER_BOUND = "2024-01-15T00:00:00"
_MATERIALIZATION_TIME = "2024-01-15T01:00:00"


def _make_row(
    columns: dict[str, str | None],
) -> dict[str, str | None]:
    """Wraps a dict of column values with the standard metadata columns."""
    return {
        UPPER_BOUND_DATETIME_COL_NAME: _UPPER_BOUND,
        MATERIALIZATION_TIME_COL_NAME: _MATERIALIZATION_TIME,
        **columns,
    }


# pylint: disable=protected-access
class TestGenerateEntities(unittest.TestCase):
    """Tests for GenerateEntities._generate_entity_from_ingest_view_results."""

    manifest: IngestViewManifest

    @classmethod
    def setUpClass(cls) -> None:
        super().setUpClass()
        region = get_direct_ingest_region(
            region_code="us_dd", region_module_override=fake_regions
        )
        delegate = StateSchemaIngestViewManifestCompilerDelegate(region=region)
        compiler = IngestViewManifestCompiler(delegate)
        cls.manifest = compiler.compile_manifest(ingest_view_name="ingest12")

    def test_generates_entity_with_attributes(self) -> None:
        transform = GenerateEntities(
            self.manifest,
            _CONTEXT,
            expected_root_entity_types=(StatePerson, StateStaff),
        )

        row = _make_row({"COL1": "ID1", "COL2": "John", "COL3": "Doe"})
        upper_bound_ts, entity = transform._generate_entity_from_ingest_view_results(
            row
        )

        self.assertIsInstance(entity, StatePerson)
        assert isinstance(entity, StatePerson)
        self.assertEqual("US_DD", entity.state_code)
        self.assertEqual(1, len(entity.external_ids))
        self.assertEqual("ID1", entity.external_ids[0].external_id)
        self.assertEqual("US_DD_ID_TYPE", entity.external_ids[0].id_type)

        expected_ts = datetime.datetime(2024, 1, 15).timestamp()
        self.assertEqual(expected_ts, upper_bound_ts)

    def test_upper_bound_timestamp_parsing(self) -> None:
        transform = GenerateEntities(
            self.manifest,
            _CONTEXT,
            expected_root_entity_types=(StatePerson, StateStaff),
        )

        row = _make_row({"COL1": "ID1", "COL2": "Jane", "COL3": "Smith"})
        row[UPPER_BOUND_DATETIME_COL_NAME] = "2024-06-01T12:00:00"
        upper_bound_ts, _ = transform._generate_entity_from_ingest_view_results(row)

        expected_ts = datetime.datetime(2024, 6, 1, 12, 0, 0).timestamp()
        self.assertEqual(expected_ts, upper_bound_ts)

    def test_rejects_unexpected_entity_type(self) -> None:
        transform = GenerateEntities(
            self.manifest,
            _CONTEXT,
            expected_root_entity_types=(StateStaff,),
        )

        row = _make_row({"COL1": "ID1", "COL2": "John", "COL3": "Doe"})
        with self.assertRaises(ValueError, msg="Expected one of"):
            transform._generate_entity_from_ingest_view_results(row)

    def test_metadata_columns_stripped(self) -> None:
        transform = GenerateEntities(
            self.manifest,
            _CONTEXT,
            expected_root_entity_types=(StatePerson, StateStaff),
        )

        row = _make_row({"COL1": "ID1", "COL2": "John", "COL3": "Doe"})
        _, entity = transform._generate_entity_from_ingest_view_results(row)

        assert isinstance(entity, StatePerson)
        self.assertEqual(1, len(entity.external_ids))
        self.assertEqual("ID1", entity.external_ids[0].external_id)
