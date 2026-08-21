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
from recidiviz.documents.extraction.views.llm_extraction_results_view_collector import (
    collect_first_order_llm_extraction_results_view_builders,
    collect_post_entity_resolution_llm_extraction_results_view_builders,
)
from recidiviz.documents.store.document_store_sandbox_context import (
    DocumentCollectionSandboxLocation,
    DocumentStoreSandboxContext,
)
from recidiviz.tests.documents import fake_config
from recidiviz.tests.documents.extraction.entity_resolution.entity_resolution_test_utils import (
    fake_entity_resolution_extractor_config_pairs,
    fake_first_order_extractor_config,
    patch_fake_entity_resolution_model_config_name,
)
from recidiviz.tools.documents.sandbox_extraction_bq_helpers import (
    first_order_view_input_overrides,
    post_entity_resolution_view_input_overrides,
)
from recidiviz.utils.metadata import local_project_id_override

_DOCUMENT_STORE_PREFIX = "doc_store_prefix"
_RESULTS_PREFIX = "results_prefix"


def _addresses(*address_strs: str) -> set[BigQueryAddress]:
    """Returns the addresses parsed from each of |address_strs|."""
    return {BigQueryAddress.from_str(s) for s in address_strs}


class SandboxOverridesTest(unittest.TestCase):
    """Tests the overrides each deploy layer registers, against every source table the
    parsed views read.

    Each test spells out which of those source tables the layer leaves alone. A layer
    leaves a source table alone either because none of the views it deploys reads it, or
    because its own load_views_to_sandbox call deploys the view that writes it. Any other
    source table a layer leaves alone is read from production, which for a result table or
    a document store table a sandbox run wrote is a bug.
    """

    def setUp(self) -> None:
        self.enterContext(local_project_id_override("recidiviz-staging"))
        self.enterContext(patch_fake_entity_resolution_model_config_name())
        self.first_order_config = fake_first_order_extractor_config()
        self.er_configs = [
            er_config
            for _, er_config in fake_entity_resolution_extractor_config_pairs()
        ]
        self.first_order_view_builders = (
            collect_first_order_llm_extraction_results_view_builders(
                [self.first_order_config]
            )
        )
        self.post_resolution_view_builders = (
            collect_post_entity_resolution_llm_extraction_results_view_builders(
                [self.first_order_config], config_module=fake_config
            )
        )
        self.all_source_tables_read_by_views = self._referenced_source_tables(
            self.first_order_view_builders
        ) | self._referenced_source_tables(self.post_resolution_view_builders)

    def _referenced_source_tables(
        self, view_builders: list[BigQueryViewBuilder]
    ) -> set[BigQueryAddress]:
        """Returns every source table the views in |view_builders| read."""
        dag = BigQueryViewDagWalker([builder.build() for builder in view_builders])
        return dag.get_referenced_source_tables()

    def _document_store_sandbox_context(self) -> DocumentStoreSandboxContext:
        """Returns a context that puts the input document collection in a sandbox, as a
        run that wrote its own copy of the input documents does.
        """
        return DocumentStoreSandboxContext(
            document_collection_locations={
                self.first_order_config.input_document_collection.name: (
                    DocumentCollectionSandboxLocation(
                        output_prefix=_DOCUMENT_STORE_PREFIX, diff_read_prefix=None
                    )
                )
            },
            extractor_collection_read_prefixes={},
        )

    def _production_document_store_context(self) -> DocumentStoreSandboxContext:
        """Returns a context that declares the input document collection un-sandboxed, as
        a run that reads its input documents from the production document store does.
        """
        return DocumentStoreSandboxContext(
            document_collection_locations={
                self.first_order_config.input_document_collection.name: None
            },
            extractor_collection_read_prefixes={},
        )

    def _override_prefix(
        self, address: BigQueryAddress, overrides: BigQueryAddressOverrides
    ) -> str | None:
        """Returns the sandbox dataset prefix |overrides| re-points |address| to, or None
        if |overrides| registers no override for |address|.
        """
        sandbox_address = overrides.get_sandbox_address(address)
        if sandbox_address is None:
            return None
        if sandbox_address.table_id != address.table_id:
            raise ValueError(
                f"Override for [{address.to_str()}] changes the table_id: "
                f"[{sandbox_address.to_str()}]."
            )
        dataset_suffix = f"_{address.dataset_id}"
        if not sandbox_address.dataset_id.endswith(dataset_suffix):
            raise ValueError(
                f"Override for [{address.to_str()}] is not a prefixed copy of the "
                f"original dataset: [{sandbox_address.to_str()}]."
            )
        return sandbox_address.dataset_id.removesuffix(dataset_suffix)

    def _addresses_by_override_prefix(
        self, overrides: BigQueryAddressOverrides
    ) -> dict[str | None, set[BigQueryAddress]]:
        """Returns every source table the parsed views read, grouped by the sandbox
        dataset prefix |overrides| re-points it to. The None key holds the source tables
        |overrides| leaves alone.
        """
        addresses_by_prefix: dict[str | None, set[BigQueryAddress]] = {}
        for address in self.all_source_tables_read_by_views:
            prefix = self._override_prefix(address, overrides)
            addresses_by_prefix.setdefault(prefix, set()).add(address)
        return addresses_by_prefix

    def test_first_order_overrides_with_no_document_store_sandbox(self) -> None:
        """The first-order layer overrides the result tables its own views read. It
        reads the input documents from the production document store. It leaves every
        source table only the post-resolution views read alone, including the first-
        order `__pre_resolution` materialized tables, which the first-order load
        deploys itself. A run with no document store context and a run whose context
        puts the input collection in the production document store both read
        production, so both produce these same overrides.
        """
        expected_addresses_without_override = _addresses(
            "normalized_state.state_person_external_id",
            "us_xx_document_contents.fake_input_notes_document_contents",
            "us_xx_document_extraction_results__pre_resolution.fake_extractor_collection_assignments_materialized",
            "us_xx_document_extraction_results__pre_resolution.fake_extractor_collection_materialized",
            "us_xx_document_extraction_results__validated.fake_extractor_collection_assignment_entity_resolution",
            "us_xx_document_extraction_results__validated.fake_extractor_collection_location_entity_resolution",
            "us_xx_document_extraction_results__validated.fake_extractor_collection_pay_rate_entity_resolution",
            "us_xx_document_store_metadata.fake_extractor_collection_assignment_entry_source_map",
            "us_xx_document_store_metadata.fake_extractor_collection_location_entry_source_map",
            "us_xx_document_store_metadata.fake_extractor_collection_pay_rate_entry_source_map",
            "us_xx_document_store_metadata.fake_input_notes",
        )

        # A run with no document store context at all.
        overrides = first_order_view_input_overrides(
            config=self.first_order_config,
            results_sandbox_prefix=_RESULTS_PREFIX,
            document_store_sandbox=None,
        )
        addresses_by_prefix = self._addresses_by_override_prefix(overrides)
        # Only these prefixes appear. Every source table the assertions below do
        # not list is overridden to the results prefix.
        self.assertEqual(
            {None, _RESULTS_PREFIX},
            set(addresses_by_prefix),
        )
        self.assertEqual(
            expected_addresses_without_override,
            addresses_by_prefix[None],
        )

        # A run whose context puts the input collection in the production document store.
        overrides = first_order_view_input_overrides(
            config=self.first_order_config,
            results_sandbox_prefix=_RESULTS_PREFIX,
            document_store_sandbox=self._production_document_store_context(),
        )
        addresses_by_prefix = self._addresses_by_override_prefix(overrides)
        # Only these prefixes appear. Every source table the assertions below do
        # not list is overridden to the results prefix.
        self.assertEqual(
            {None, _RESULTS_PREFIX},
            set(addresses_by_prefix),
        )
        self.assertEqual(
            expected_addresses_without_override,
            addresses_by_prefix[None],
        )

    def test_first_order_overrides_with_document_store_sandbox(self) -> None:
        """A run that wrote its own copy of the input documents additionally overrides
        the input document collection's document store tables to the document store
        prefix. Everything else matches the no-document-store-sandbox case.
        """
        overrides = first_order_view_input_overrides(
            config=self.first_order_config,
            results_sandbox_prefix=_RESULTS_PREFIX,
            document_store_sandbox=self._document_store_sandbox_context(),
        )

        addresses_by_prefix = self._addresses_by_override_prefix(overrides)

        # Only these prefixes appear. Every source table the assertions below do
        # not list is overridden to the results prefix.
        self.assertEqual(
            {None, _DOCUMENT_STORE_PREFIX, _RESULTS_PREFIX},
            set(addresses_by_prefix),
        )
        self.assertEqual(
            _addresses(
                "normalized_state.state_person_external_id",
                "us_xx_document_extraction_results__pre_resolution.fake_extractor_collection_assignments_materialized",
                "us_xx_document_extraction_results__pre_resolution.fake_extractor_collection_materialized",
                "us_xx_document_extraction_results__validated.fake_extractor_collection_assignment_entity_resolution",
                "us_xx_document_extraction_results__validated.fake_extractor_collection_location_entity_resolution",
                "us_xx_document_extraction_results__validated.fake_extractor_collection_pay_rate_entity_resolution",
                "us_xx_document_store_metadata.fake_extractor_collection_assignment_entry_source_map",
                "us_xx_document_store_metadata.fake_extractor_collection_location_entry_source_map",
                "us_xx_document_store_metadata.fake_extractor_collection_pay_rate_entry_source_map",
            ),
            addresses_by_prefix[None],
        )
        self.assertEqual(
            _addresses(
                "us_xx_document_contents.fake_input_notes_document_contents",
                "us_xx_document_store_metadata.fake_input_notes",
            ),
            addresses_by_prefix[_DOCUMENT_STORE_PREFIX],
        )

    def test_post_resolution_overrides_with_no_document_store_sandbox(self) -> None:
        """The post-resolution layer overrides every result table its views read,
        including the first-order `__pre_resolution` materialized tables, which it
        reads but does not deploy. It reads the input documents from the production
        document store. A run with no document store context and a run whose context
        puts the input collection in the production document store both read
        production, so both produce these same overrides.
        """
        expected_addresses_without_override = _addresses(
            "normalized_state.state_person_external_id",
            "us_xx_document_contents.fake_input_notes_document_contents",
            "us_xx_document_store_metadata.fake_input_notes",
        )

        # A run with no document store context at all.
        overrides = post_entity_resolution_view_input_overrides(
            config=self.first_order_config,
            er_configs=self.er_configs,
            first_order_view_builders=self.first_order_view_builders,
            results_sandbox_prefix=_RESULTS_PREFIX,
            document_store_sandbox=None,
        )
        addresses_by_prefix = self._addresses_by_override_prefix(overrides)
        # Only these prefixes appear. Every source table the assertions below do
        # not list is overridden to the results prefix.
        self.assertEqual(
            {None, _RESULTS_PREFIX},
            set(addresses_by_prefix),
        )
        self.assertEqual(
            expected_addresses_without_override,
            addresses_by_prefix[None],
        )

        # A run whose context puts the input collection in the production document store.
        overrides = post_entity_resolution_view_input_overrides(
            config=self.first_order_config,
            er_configs=self.er_configs,
            first_order_view_builders=self.first_order_view_builders,
            results_sandbox_prefix=_RESULTS_PREFIX,
            document_store_sandbox=self._production_document_store_context(),
        )
        addresses_by_prefix = self._addresses_by_override_prefix(overrides)
        # Only these prefixes appear. Every source table the assertions below do
        # not list is overridden to the results prefix.
        self.assertEqual(
            {None, _RESULTS_PREFIX},
            set(addresses_by_prefix),
        )
        self.assertEqual(
            expected_addresses_without_override,
            addresses_by_prefix[None],
        )

    def test_post_resolution_overrides_with_document_store_sandbox(self) -> None:
        """A run that wrote its own copy of the input documents additionally overrides
        the input document collection's document store tables to the document store
        prefix. Everything else matches the no-document-store-sandbox case.
        """
        overrides = post_entity_resolution_view_input_overrides(
            config=self.first_order_config,
            er_configs=self.er_configs,
            first_order_view_builders=self.first_order_view_builders,
            results_sandbox_prefix=_RESULTS_PREFIX,
            document_store_sandbox=self._document_store_sandbox_context(),
        )

        addresses_by_prefix = self._addresses_by_override_prefix(overrides)

        # Only these prefixes appear. Every source table the assertions below do
        # not list is overridden to the results prefix.
        self.assertEqual(
            {None, _DOCUMENT_STORE_PREFIX, _RESULTS_PREFIX},
            set(addresses_by_prefix),
        )
        self.assertEqual(
            _addresses(
                "normalized_state.state_person_external_id",
            ),
            addresses_by_prefix[None],
        )
        self.assertEqual(
            _addresses(
                "us_xx_document_contents.fake_input_notes_document_contents",
                "us_xx_document_store_metadata.fake_input_notes",
            ),
            addresses_by_prefix[_DOCUMENT_STORE_PREFIX],
        )

    def test_merged_first_order_and_post_resolution_overrides_with_no_document_store_sandbox(
        self,
    ) -> None:
        """The two loads run one after the other, so together they must leave no table a
        run writes pointed at production. What is left is the production
        normalized_state read, plus the input document store, which this run also reads
        from production. A run with no document store context and a run whose context
        puts the input collection in the production document store both read production,
        so both produce these same overrides.
        """
        expected_addresses_without_override = _addresses(
            "normalized_state.state_person_external_id",
            "us_xx_document_contents.fake_input_notes_document_contents",
            "us_xx_document_store_metadata.fake_input_notes",
        )

        # A run with no document store context at all.
        overrides = BigQueryAddressOverrides.merge(
            first_order_view_input_overrides(
                config=self.first_order_config,
                results_sandbox_prefix=_RESULTS_PREFIX,
                document_store_sandbox=None,
            ),
            post_entity_resolution_view_input_overrides(
                config=self.first_order_config,
                er_configs=self.er_configs,
                first_order_view_builders=self.first_order_view_builders,
                results_sandbox_prefix=_RESULTS_PREFIX,
                document_store_sandbox=None,
            ),
        )
        addresses_by_prefix = self._addresses_by_override_prefix(overrides)
        # Only these prefixes appear. Every source table the assertions below do
        # not list is overridden to the results prefix.
        self.assertEqual(
            {None, _RESULTS_PREFIX},
            set(addresses_by_prefix),
        )
        self.assertEqual(
            expected_addresses_without_override,
            addresses_by_prefix[None],
        )

        # A run whose context puts the input collection in the production document store.
        overrides = BigQueryAddressOverrides.merge(
            first_order_view_input_overrides(
                config=self.first_order_config,
                results_sandbox_prefix=_RESULTS_PREFIX,
                document_store_sandbox=self._production_document_store_context(),
            ),
            post_entity_resolution_view_input_overrides(
                config=self.first_order_config,
                er_configs=self.er_configs,
                first_order_view_builders=self.first_order_view_builders,
                results_sandbox_prefix=_RESULTS_PREFIX,
                document_store_sandbox=self._production_document_store_context(),
            ),
        )
        addresses_by_prefix = self._addresses_by_override_prefix(overrides)
        # Only these prefixes appear. Every source table the assertions below do
        # not list is overridden to the results prefix.
        self.assertEqual(
            {None, _RESULTS_PREFIX},
            set(addresses_by_prefix),
        )
        self.assertEqual(
            expected_addresses_without_override,
            addresses_by_prefix[None],
        )

    def test_merged_first_order_and_post_resolution_overrides_with_document_store_sandbox(
        self,
    ) -> None:
        """A run that wrote its own copy of the input documents overrides the input
        document collection's document store tables too. Every table this run writes is
        then overridden, so the production normalized_state read is all that is left.
        """
        overrides = BigQueryAddressOverrides.merge(
            first_order_view_input_overrides(
                config=self.first_order_config,
                results_sandbox_prefix=_RESULTS_PREFIX,
                document_store_sandbox=self._document_store_sandbox_context(),
            ),
            post_entity_resolution_view_input_overrides(
                config=self.first_order_config,
                er_configs=self.er_configs,
                first_order_view_builders=self.first_order_view_builders,
                results_sandbox_prefix=_RESULTS_PREFIX,
                document_store_sandbox=self._document_store_sandbox_context(),
            ),
        )

        addresses_by_prefix = self._addresses_by_override_prefix(overrides)

        # Only these prefixes appear. Every source table the assertions below do
        # not list is overridden to the results prefix.
        self.assertEqual(
            {None, _DOCUMENT_STORE_PREFIX, _RESULTS_PREFIX},
            set(addresses_by_prefix),
        )
        self.assertEqual(
            _addresses(
                "normalized_state.state_person_external_id",
            ),
            addresses_by_prefix[None],
        )
        self.assertEqual(
            _addresses(
                "us_xx_document_contents.fake_input_notes_document_contents",
                "us_xx_document_store_metadata.fake_input_notes",
            ),
            addresses_by_prefix[_DOCUMENT_STORE_PREFIX],
        )
