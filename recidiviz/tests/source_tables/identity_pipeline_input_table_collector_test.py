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
"""Tests for identity_pipeline_input_table_collector."""
import unittest

from more_itertools import one

from recidiviz.common.constants.states import StateCode
from recidiviz.common.constants.tenants import Tenant
from recidiviz.ingest.direct.regions.direct_ingest_region_utils import (
    get_direct_ingest_states_existing_in_env,
)
from recidiviz.source_tables.collect_all_source_table_configs import (
    build_source_table_repository_for_collected_schemata,
)
from recidiviz.source_tables.identity_pipeline_input_table_collector import (
    build_identity_overrides_source_table_collection,
    build_identity_pipeline_input_source_table_collections,
)
from recidiviz.source_tables.source_table_config import (
    DataflowPipelineOutputSourceTableLabel,
    SourceTableCollectionUpdateConfig,
    StateSpecificSourceTableLabel,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

_TENANT = Tenant.US_OZ


class TestBuildIdentityOverridesSourceTableCollection(unittest.TestCase):
    """Tests for build_identity_overrides_source_table_collection."""

    def test_uses_the_overrides_dataset(self) -> None:
        collection = build_identity_overrides_source_table_collection(_TENANT)
        self.assertEqual(collection.dataset_id, "us_oz_identity_overrides")

    def test_is_protected(self) -> None:
        """A reviewer's recorded rows must never be dropped by a schema update or
        a pipeline run, so the collection must be protected."""
        collection = build_identity_overrides_source_table_collection(_TENANT)
        self.assertEqual(
            collection.update_config, SourceTableCollectionUpdateConfig.protected()
        )

    def test_does_not_carry_the_pipeline_output_label(self) -> None:
        """The pipeline reads this dataset but does not write it, so it must not
        carry DataflowPipelineOutputSourceTableLabel: sandbox tooling would
        otherwise create (and wipe on recreation) a sandbox copy that pipeline
        runs never read."""
        collection = build_identity_overrides_source_table_collection(_TENANT)
        self.assertFalse(
            any(
                isinstance(label, DataflowPipelineOutputSourceTableLabel)
                for label in collection.labels
            )
        )
        self.assertIn(
            StateSpecificSourceTableLabel(state_code=StateCode.US_OZ),
            collection.labels,
        )

    def test_emits_the_identity_cluster_override_table(self) -> None:
        collection = build_identity_overrides_source_table_collection(_TENANT)
        source_table = one(collection.source_tables)
        self.assertEqual(source_table.address.table_id, "identity_cluster_override")


class TestBuildIdentityPipelineInputSourceTableCollections(unittest.TestCase):
    """Tests for build_identity_pipeline_input_source_table_collections."""

    def test_collections_per_tenant(self) -> None:
        # One collection per tenant, holding the overrides dataset.
        state_codes = get_direct_ingest_states_existing_in_env()
        collections = build_identity_pipeline_input_source_table_collections()
        self.assertEqual(len(collections), len(state_codes))
        self.assertEqual(
            {c.dataset_id for c in collections},
            {
                f"{state_code.value.lower()}_identity_overrides"
                for state_code in state_codes
            },
        )

    def test_overrides_collections_reach_the_schema_update_repository(self) -> None:
        """The overrides tables are not dataflow pipeline output, so they reach
        the schema update process through their own entry in
        build_source_table_repository_for_collected_schemata rather than through
        get_dataflow_output_source_table_collections. This guards that wiring."""
        with local_project_id_override(GCP_PROJECT_STAGING):
            repository = build_source_table_repository_for_collected_schemata(
                project_id=GCP_PROJECT_STAGING
            )
            overrides_dataset_ids = {
                collection.dataset_id
                for collection in repository.source_table_collections
                if collection.dataset_id.endswith("_identity_overrides")
            }
            self.assertEqual(
                overrides_dataset_ids,
                {
                    f"{state_code.value.lower()}_identity_overrides"
                    for state_code in get_direct_ingest_states_existing_in_env()
                },
            )
