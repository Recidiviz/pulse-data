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
"""Tests for sandbox_extraction_bq_helpers"""

import unittest

from recidiviz.big_query.address_overrides import BigQueryAddressOverrides
from recidiviz.big_query.big_query_address import BigQueryAddress
from recidiviz.big_query.big_query_view import BigQueryViewBuilder
from recidiviz.big_query.big_query_view_dag_walker import BigQueryViewDagWalker
from recidiviz.documents.extraction.entity_resolution.entity_resolution_entry_source_map_table import (
    EntityResolutionEntrySourceMapBQTable,
)
from recidiviz.documents.extraction.models.llm_extractor_config import (
    LLMExtractorConfig,
)
from recidiviz.documents.extraction.views.llm_extraction_results_view_collector import (
    collect_first_order_llm_extraction_results_view_builders,
    collect_post_entity_resolution_llm_extraction_results_view_builders,
)
from recidiviz.documents.store.document_store_sandbox_context import (
    DocumentCollectionSandboxLocation,
    DocumentStoreSandboxContext,
)
from recidiviz.source_tables.document_store_source_table_collection import (
    collect_document_store_source_tables_for_configs,
)
from recidiviz.source_tables.extraction_results_source_table_collection import (
    collect_extraction_results_source_table_collections_for_config,
)
from recidiviz.tests.documents import fake_config
from recidiviz.tests.documents.extraction.entity_resolution.entity_resolution_test_utils import (
    fake_entity_resolution_extractor_config_pairs,
    fake_first_order_extractor_config,
    patch_fake_entity_resolution_model_config_name,
)
from recidiviz.tools.documents.sandbox_extraction_bq_helpers import (
    first_order_input_overrides,
    post_entity_resolution_input_overrides,
)
from recidiviz.utils.metadata import local_project_id_override

_NORMALIZED_STATE_REFERENCE_TABLES = {
    BigQueryAddress(dataset_id="normalized_state", table_id="state_person_external_id"),
}

_DOCUMENT_STORE_PREFIX = "doc_store_prefix"
_RESULTS_PREFIX = "results_prefix"


class SandboxOverridesTest(unittest.TestCase):
    """Verifies each sandbox extraction deploy layer re-points every source table its parsed
    views actually read at the table the run reads instead.

    The source tables to check come from the built views' own queries; the expected sandbox
    address for each is computed by classifying it into one of the buckets a sandbox run
    handles, from the same collectors the run itself uses:

      - result tables (this config's, and the entity-resolution configs' at the
        post-resolution layer) and entry->source maps: re-pointed to the results prefix,
        since the run writes them there;
      - the input document store: re-pointed to the document store prefix when the run seeded
        a sandbox store, else read from production (no override);
      - views the deploy itself produces under the results prefix: resolved by prefix, not by
        an override (no override);
      - the normalized_state reference tables the public views join against: intentionally
        read from production (no override).

    A view that gains a source-table dependency none of these buckets recognizes fails the
    test, pointing at the table to classify — either add an override in
    sandbox_extraction_bq_helpers.py or, if the read is intentionally against production, to
    _NORMALIZED_STATE_REFERENCE_TABLES.
    """

    def setUp(self) -> None:
        self.enterContext(local_project_id_override("recidiviz-staging"))
        self.enterContext(patch_fake_entity_resolution_model_config_name())
        self.config = fake_first_order_extractor_config()
        self.er_configs = [
            er_config
            for _, er_config in fake_entity_resolution_extractor_config_pairs()
        ]

    def _seeded_document_store(self) -> DocumentStoreSandboxContext:
        return DocumentStoreSandboxContext(
            document_collection_locations={
                self.config.input_document_collection.name: (
                    DocumentCollectionSandboxLocation(
                        output_prefix=_DOCUMENT_STORE_PREFIX, diff_read_prefix=None
                    )
                )
            },
            extractor_collection_read_prefixes={},
        )

    def _sandbox(self, address: BigQueryAddress, prefix: str) -> BigQueryAddress:
        return BigQueryAddress(
            dataset_id=BigQueryAddressOverrides.format_sandbox_dataset(
                prefix, address.dataset_id
            ),
            table_id=address.table_id,
        )

    def _result_addresses(
        self, configs: list[LLMExtractorConfig]
    ) -> set[BigQueryAddress]:
        return {
            source_table.address
            for config in configs
            for collection in collect_extraction_results_source_table_collections_for_config(
                config
            )
            for source_table in collection.source_tables
        }

    def _entry_source_map_addresses(self) -> set[BigQueryAddress]:
        return set(
            EntityResolutionEntrySourceMapBQTable.addresses_for_collection(
                state_code=self.config.state_code,
                extractor_collection=self.config.extractor_collection,
                sandbox_prefix=None,
            )
        )

    def _input_document_store_addresses(self) -> set[BigQueryAddress]:
        return {
            source_table.address
            for collection in collect_document_store_source_tables_for_configs(
                [self.config.input_document_collection]
            )
            for source_table in collection.source_tables
        }

    def _referenced_source_tables(
        self, view_builders: list[BigQueryViewBuilder]
    ) -> set[BigQueryAddress]:
        dag = BigQueryViewDagWalker([builder.build() for builder in view_builders])
        return dag.get_referenced_source_tables()

    def _deployed_addresses(
        self, view_builders: list[BigQueryViewBuilder]
    ) -> set[BigQueryAddress]:
        deployed: set[BigQueryAddress] = set()
        for builder in view_builders:
            view = builder.build()
            deployed.add(view.address)
            if view.materialized_address is not None:
                deployed.add(view.materialized_address)
        return deployed

    def _assert_overrides_match(
        self,
        *,
        view_builders: list[BigQueryViewBuilder],
        deployed_view_builders: list[BigQueryViewBuilder],
        result_configs: list[LLMExtractorConfig],
        include_entry_source_map: bool,
        overrides: BigQueryAddressOverrides,
        document_store_prefix: str | None,
    ) -> None:
        """Asserts every source table |view_builders| read resolves to its expected sandbox
        address under |overrides|, classifying each against the buckets a run of this layer
        handles. |result_configs| are the configs whose result tables this layer re-points;
        |include_entry_source_map| adds the entry->source maps (post-resolution only);
        |document_store_prefix| is the input document store's prefix, or None when the run
        reads it from production. |deployed_view_builders| are the builders the run deploys
        under the results prefix (a superset of |view_builders| at the post-resolution
        layer, which reads the first-order layer)."""
        entry_source_maps = (
            self._entry_source_map_addresses() if include_entry_source_map else set()
        )
        results_prefix_tables = (
            self._result_addresses(result_configs) | entry_source_maps
        )
        input_document_store = self._input_document_store_addresses()
        deployed = self._deployed_addresses(deployed_view_builders)

        referenced = self._referenced_source_tables(view_builders)
        # Guard against a classification that never matches: if a bucket is not exercised by
        # the views, its assertions are vacuous and a regression would go unnoticed. The
        # results and input document store buckets are read at both layers; entry->source
        # maps only where this layer expects them.
        self.assertTrue(referenced & self._result_addresses(result_configs))
        self.assertTrue(referenced & input_document_store)
        if include_entry_source_map:
            self.assertTrue(referenced & entry_source_maps)

        unclassified: set[BigQueryAddress] = set()
        for address in referenced:
            actual = overrides.get_sandbox_address(address)
            if address in results_prefix_tables:
                self.assertEqual(self._sandbox(address, _RESULTS_PREFIX), actual)
            elif address in input_document_store:
                expected = (
                    self._sandbox(address, document_store_prefix)
                    if document_store_prefix is not None
                    else None
                )
                self.assertEqual(expected, actual)
            elif address in deployed or address in _NORMALIZED_STATE_REFERENCE_TABLES:
                self.assertIsNone(actual)
            else:
                unclassified.add(address)

        self.assertEqual(
            set(),
            unclassified,
            f"These source tables are read by the parsed views but match none of the "
            f"buckets a sandbox run handles: {sorted(a.to_str() for a in unclassified)}. "
            f"Add an override in sandbox_extraction_bq_helpers.py, or, if the read is "
            f"intentionally against production, add it to the classification here.",
        )

    def test_first_order_overrides(self) -> None:
        view_builders = collect_first_order_llm_extraction_results_view_builders(
            [self.config]
        )
        for document_store, document_store_prefix in [
            (None, None),
            (self._seeded_document_store(), _DOCUMENT_STORE_PREFIX),
        ]:
            with self.subTest(seeded_document_store=document_store_prefix is not None):
                self._assert_overrides_match(
                    view_builders=view_builders,
                    deployed_view_builders=view_builders,
                    result_configs=[self.config],
                    include_entry_source_map=False,
                    overrides=first_order_input_overrides(
                        config=self.config,
                        results_sandbox_prefix=_RESULTS_PREFIX,
                        document_store_sandbox=document_store,
                    ),
                    document_store_prefix=document_store_prefix,
                )

    def test_post_resolution_overrides(self) -> None:
        view_builders = (
            collect_post_entity_resolution_llm_extraction_results_view_builders(
                [self.config], config_module=fake_config
            )
        )
        # The post-resolution views read the first-order pre_resolution layer the first-order
        # deploy materialized, so those builders are deployed too.
        deployed_view_builders = (
            collect_first_order_llm_extraction_results_view_builders([self.config])
            + view_builders
        )
        for document_store, document_store_prefix in [
            (None, None),
            (self._seeded_document_store(), _DOCUMENT_STORE_PREFIX),
        ]:
            with self.subTest(seeded_document_store=document_store_prefix is not None):
                self._assert_overrides_match(
                    view_builders=view_builders,
                    deployed_view_builders=deployed_view_builders,
                    result_configs=[self.config, *self.er_configs],
                    include_entry_source_map=True,
                    overrides=post_entity_resolution_input_overrides(
                        config=self.config,
                        er_configs=self.er_configs,
                        results_sandbox_prefix=_RESULTS_PREFIX,
                        document_store_sandbox=document_store,
                    ),
                    document_store_prefix=document_store_prefix,
                )
