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
"""Tests for identity_pipeline_output_table_collector."""
import unittest
from unittest.mock import patch

from more_itertools import one

from recidiviz.common.constants.states import StateCode
from recidiviz.common.constants.tenants import Tenant
from recidiviz.ingest.direct.regions.direct_ingest_region_utils import (
    get_direct_ingest_states_existing_in_env,
)
from recidiviz.pipelines.pipeline_names import IDENTITY_INGEST_PIPELINE_NAME
from recidiviz.source_tables.identity_pipeline_output_table_collector import (
    build_identity_cluster_output_source_table_collection,
    build_identity_fragment_output_source_table_collection,
    build_identity_pipeline_output_source_table_collections,
    build_identity_rejections_source_table_collection,
)
from recidiviz.source_tables.source_table_config import (
    DataflowPipelineSourceTableLabel,
    SourceTableCollectionUpdateConfig,
    StateSpecificSourceTableLabel,
)

_TENANT = Tenant.US_OZ

_EXPECTED_FRAGMENT_TABLE_IDS: set[str] = {
    "identity_fragment",
    "identity_attributes",
    "identity_email",
    "identity_ethnicity",
    "identity_external_id",
    "identity_gender",
    "identity_name",
    "identity_phone_number",
    "identity_race",
    "identity_sex",
}

_EXPECTED_CLUSTER_TABLE_IDS: set[str] = {
    "identity_cluster",
    "identity_cluster_email",
    "identity_cluster_ethnicity",
    "identity_cluster_external_id",
    "identity_cluster_gender",
    "identity_cluster_name",
    "identity_cluster_phone_number",
    "identity_cluster_race",
    "identity_cluster_sex",
}


class TestBuildIdentityFragmentOutputSourceTableCollection(unittest.TestCase):
    """Tests for build_identity_fragment_output_source_table_collection."""

    def setUp(self) -> None:
        super().setUp()
        self.metadata_patcher = patch("recidiviz.utils.metadata.project_id")
        self.mock_project_id_fn = self.metadata_patcher.start()
        self.mock_project_id_fn.return_value = "recidiviz-456"

    def tearDown(self) -> None:
        self.metadata_patcher.stop()

    def test_dataset_id_is_tenant_specific(self) -> None:
        collection = build_identity_fragment_output_source_table_collection(_TENANT)
        self.assertEqual(collection.dataset_id, "us_oz_identity_fragment")

    def test_carries_identity_ingest_pipeline_label(self) -> None:
        collection = build_identity_fragment_output_source_table_collection(_TENANT)
        self.assertIn(
            DataflowPipelineSourceTableLabel(IDENTITY_INGEST_PIPELINE_NAME),
            collection.labels,
        )
        self.assertIn(
            StateSpecificSourceTableLabel(state_code=StateCode.US_OZ),
            collection.labels,
        )

    def test_emits_one_table_per_fragment_entity(self) -> None:
        collection = build_identity_fragment_output_source_table_collection(_TENANT)
        self.assertEqual(
            {t.address.table_id for t in collection.source_tables},
            _EXPECTED_FRAGMENT_TABLE_IDS,
        )

    def test_all_tables_cluster_on_identity_fragment_id(self) -> None:
        """Every fragment table carries and is clustered on identity_fragment_id,
        the join key relating a fragment's child rows back to it."""
        collection = build_identity_fragment_output_source_table_collection(_TENANT)
        for source_table in collection.source_tables:
            self.assertEqual(
                source_table.clustering_fields,
                ["identity_fragment_id"],
                f"Table [{source_table.address.table_id}] is not clustered on "
                f"identity_fragment_id",
            )

    def test_identity_fragment_id_fk_is_string(self) -> None:
        """A demographic table reaches the root only through the intermediate
        IdentityAttributes, so its identity_fragment_id FK must be STRING."""
        collection = build_identity_fragment_output_source_table_collection(_TENANT)
        child_table = one(
            t for t in collection.source_tables if t.address.table_id == "identity_name"
        )
        fk_field = one(
            f for f in child_table.schema_fields if f.name == "identity_fragment_id"
        )
        self.assertEqual("STRING", fk_field.field_type)


class TestBuildIdentityClusterOutputSourceTableCollection(unittest.TestCase):
    """Tests for build_identity_cluster_output_source_table_collection."""

    def setUp(self) -> None:
        super().setUp()
        self.metadata_patcher = patch("recidiviz.utils.metadata.project_id")
        self.mock_project_id_fn = self.metadata_patcher.start()
        self.mock_project_id_fn.return_value = "recidiviz-456"

    def tearDown(self) -> None:
        self.metadata_patcher.stop()

    def test_dataset_id_is_tenant_specific(self) -> None:
        collection = build_identity_cluster_output_source_table_collection(_TENANT)
        self.assertEqual(collection.dataset_id, "us_oz_identity_cluster")

    def test_carries_identity_ingest_pipeline_label(self) -> None:
        collection = build_identity_cluster_output_source_table_collection(_TENANT)
        self.assertIn(
            DataflowPipelineSourceTableLabel(IDENTITY_INGEST_PIPELINE_NAME),
            collection.labels,
        )
        self.assertIn(
            StateSpecificSourceTableLabel(state_code=StateCode.US_OZ),
            collection.labels,
        )

    def test_emits_one_table_per_cluster_entity(self) -> None:
        collection = build_identity_cluster_output_source_table_collection(_TENANT)
        self.assertEqual(
            {t.address.table_id for t in collection.source_tables},
            _EXPECTED_CLUSTER_TABLE_IDS,
        )

    def test_all_tables_cluster_on_identity_cluster_id(self) -> None:
        """The Identity Service queries by `identity_cluster_id`, so every
        output table must be clustered on it."""
        collection = build_identity_cluster_output_source_table_collection(_TENANT)
        for source_table in collection.source_tables:
            self.assertEqual(
                source_table.clustering_fields,
                ["identity_cluster_id"],
                f"Table [{source_table.address.table_id}] is not clustered on "
                f"identity_cluster_id",
            )

    def test_identity_cluster_id_fk_is_string(self) -> None:
        """Child tables' FK column to the root cluster table must be STRING
        (not the previously-hardcoded INTEGER)."""
        collection = build_identity_cluster_output_source_table_collection(_TENANT)
        child_table = one(
            t
            for t in collection.source_tables
            if t.address.table_id == "identity_cluster_name"
        )
        fk_field = one(
            f for f in child_table.schema_fields if f.name == "identity_cluster_id"
        )
        self.assertEqual("STRING", fk_field.field_type)


class TestBuildIdentityRejectionsSourceTableCollection(unittest.TestCase):
    """Tests for build_identity_rejections_source_table_collection."""

    def test_uses_the_rejections_dataset(self) -> None:
        collection = build_identity_rejections_source_table_collection(_TENANT)
        self.assertEqual(collection.dataset_id, "us_oz_identity_rejections")

    def test_is_regenerable(self) -> None:
        """Each run rewrites the rejections tables, so the collection is
        regenerable."""
        collection = build_identity_rejections_source_table_collection(_TENANT)
        self.assertEqual(
            collection.update_config, SourceTableCollectionUpdateConfig.regenerable()
        )

    def test_carries_identity_ingest_pipeline_label(self) -> None:
        collection = build_identity_rejections_source_table_collection(_TENANT)
        self.assertIn(
            DataflowPipelineSourceTableLabel(IDENTITY_INGEST_PIPELINE_NAME),
            collection.labels,
        )
        self.assertIn(
            StateSpecificSourceTableLabel(state_code=StateCode.US_OZ),
            collection.labels,
        )

    def test_emits_the_rejected_identity_cluster_table(self) -> None:
        collection = build_identity_rejections_source_table_collection(_TENANT)
        source_table = one(collection.source_tables)
        self.assertEqual(source_table.address.table_id, "rejected_identity_cluster")
        self.assertEqual(source_table.clustering_fields, ["identity_cluster_id"])


class TestBuildIdentityPipelineOutputSourceTableCollections(unittest.TestCase):
    """Tests for build_identity_pipeline_output_source_table_collections."""

    def test_collections_per_tenant(self) -> None:
        # Four collections per tenant, each in its own dataset: ingest view
        # results, fragments, cluster output, and rejections.
        state_codes = get_direct_ingest_states_existing_in_env()
        collections = build_identity_pipeline_output_source_table_collections()
        self.assertEqual(len(collections), 4 * len(state_codes))
        self.assertEqual(
            {c.dataset_id for c in collections},
            {
                dataset_id
                for state_code in state_codes
                for dataset_id in (
                    f"{state_code.value.lower()}_identity_ingest_view_results",
                    f"{state_code.value.lower()}_identity_fragment",
                    f"{state_code.value.lower()}_identity_cluster",
                    f"{state_code.value.lower()}_identity_rejections",
                )
            },
        )
