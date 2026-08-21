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
"""BigQuery setup for a sandbox extraction run: creating the sandbox-prefixed
result and document store tables, building the source-table overrides that
re-point the parsed views at the tables the run reads, and deploying those views.
"""

from recidiviz.big_query.address_overrides import BigQueryAddressOverrides
from recidiviz.big_query.big_query_address import BigQueryAddress
from recidiviz.big_query.big_query_client import BigQueryClient
from recidiviz.big_query.big_query_view import BigQueryViewBuilder
from recidiviz.big_query.big_query_view_dag_walker import (
    BigQueryViewDagWalkerProcessingFailureMode,
)
from recidiviz.documents.extraction.entity_resolution.entity_resolution_entry_source_map_table import (
    EntityResolutionEntrySourceMapBQTable,
)
from recidiviz.documents.extraction.models.llm_extractor_config import (
    LLMExtractorConfig,
)
from recidiviz.documents.store.document_collection_config import (
    DocumentCollectionConfig,
)
from recidiviz.documents.store.document_store_sandbox_context import (
    DocumentStoreSandboxContext,
)
from recidiviz.source_tables.document_store_source_table_collection import (
    collect_document_store_source_tables_for_configs,
)
from recidiviz.source_tables.extraction_results_source_table_collection import (
    collect_extraction_results_source_table_collections_for_config,
)
from recidiviz.source_tables.source_table_config import SourceTableCollection
from recidiviz.source_tables.source_table_update_manager import SourceTableUpdateManager
from recidiviz.tools.load_views_to_sandbox import load_collected_views_to_sandbox


def _create_sandbox_tables(
    *,
    collections: list[SourceTableCollection],
    sandbox_prefix: str,
    table_expiration_ms: int,
    bq_client: BigQueryClient,
) -> None:
    """Creates the sandbox-prefixed tables for |collections| with the given table
    expiration.

    Creates the datasets up front with the requested expiration so the source
    table update manager's own create-if-necessary finds them already present
    rather than applying its default sandbox expiration.
    """
    update_manager = SourceTableUpdateManager(bq_client)
    for collection in collections:
        sandbox_collection = collection.as_sandbox_collection(
            sandbox_dataset_prefix=sandbox_prefix
        )
        bq_client.create_dataset_if_necessary(
            sandbox_collection.dataset_id,
            default_table_expiration_ms=table_expiration_ms,
        )
        update_manager.update(sandbox_collection)


def create_extraction_results_tables(
    *,
    config: LLMExtractorConfig,
    sandbox_prefix: str,
    table_expiration_ms: int,
    bq_client: BigQueryClient,
) -> None:
    """Creates the raw / validated / audit result tables for the extractor in
    the sandbox-prefixed datasets, with the given table expiration.
    """
    _create_sandbox_tables(
        collections=collect_extraction_results_source_table_collections_for_config(
            config
        ),
        sandbox_prefix=sandbox_prefix,
        table_expiration_ms=table_expiration_ms,
        bq_client=bq_client,
    )


def create_document_store_tables(
    *,
    document_collection: DocumentCollectionConfig,
    sandbox_prefix: str,
    table_expiration_ms: int,
    bq_client: BigQueryClient,
) -> None:
    """Creates the sandbox-prefixed document store tables (metadata, contents,
    upload status, and temp datasets) for one input collection under |sandbox_prefix|,
    so the fresh upload has somewhere to write and the extraction has somewhere to read.

    TODO(OBT-42680): Not yet wired into run_sandbox_extraction, which only reads
    from the production document store; this will be called once a run can seed a
    sandbox document store to read from.
    """
    _create_sandbox_tables(
        collections=collect_document_store_source_tables_for_configs(
            [document_collection]
        ),
        sandbox_prefix=sandbox_prefix,
        table_expiration_ms=table_expiration_ms,
        bq_client=bq_client,
    )


def _addresses_in(collections: list[SourceTableCollection]) -> list[BigQueryAddress]:
    """Returns the addresses of every source table across |collections|."""
    return [
        source_table.address
        for collection in collections
        for source_table in collection.source_tables
    ]


def _register_addresses(
    builder: BigQueryAddressOverrides.Builder,
    *,
    addresses: list[BigQueryAddress],
    sandbox_prefix: str,
) -> None:
    """Registers an override pointing each of |addresses| at its |sandbox_prefix|-scoped
    copy."""
    for address in addresses:
        builder.register_sandbox_override_for_address_with_prefix(
            address, sandbox_prefix
        )


def _register_extraction_results_overrides(
    builder: BigQueryAddressOverrides.Builder,
    *,
    config: LLMExtractorConfig,
    results_sandbox_prefix: str,
) -> None:
    """Registers overrides pointing |config|'s raw / validated / audit result tables at
    their |results_sandbox_prefix|-scoped copies, since a run always writes its results
    to a sandbox."""
    _register_addresses(
        builder,
        addresses=_addresses_in(
            collect_extraction_results_source_table_collections_for_config(config)
        ),
        sandbox_prefix=results_sandbox_prefix,
    )


def _register_input_document_store_overrides(
    builder: BigQueryAddressOverrides.Builder,
    *,
    config: LLMExtractorConfig,
    document_store_sandbox: DocumentStoreSandboxContext | None,
) -> None:
    """Registers overrides pointing |config|'s input document store metadata/contents
    tables at the sandbox copies the run wrote them to. Registers nothing when the run
    reads its input from the production document store (no sandbox context, or the input
    collection is declared unsandboxed in the context), so the views read production. The
    context must declare the input collection either way — an undeclared collection raises.
    """
    if document_store_sandbox is None:
        return
    output_prefix = document_store_sandbox.source_read_prefix_for_document_collection(
        config.input_document_collection.name
    )
    if output_prefix is None:
        return
    _register_addresses(
        builder,
        addresses=_addresses_in(
            collect_document_store_source_tables_for_configs(
                [config.input_document_collection]
            )
        ),
        sandbox_prefix=output_prefix,
    )


def _register_deployed_view_overrides(
    builder: BigQueryAddressOverrides.Builder,
    *,
    view_builders: list[BigQueryViewBuilder],
    results_sandbox_prefix: str,
) -> None:
    """Registers overrides pointing every address |view_builders| deploy — each view
    and, where materialized, its materialized table — at its |results_sandbox_prefix|-
    scoped copy.
    """
    addresses: list[BigQueryAddress] = []
    for view_builder in view_builders:
        addresses.append(view_builder.address)
        if view_builder.materialized_address is not None:
            addresses.append(view_builder.materialized_address)
    _register_addresses(
        builder, addresses=addresses, sandbox_prefix=results_sandbox_prefix
    )


def first_order_view_input_overrides(
    *,
    config: LLMExtractorConfig,
    results_sandbox_prefix: str,
    document_store_sandbox: DocumentStoreSandboxContext | None,
) -> BigQueryAddressOverrides:
    """Returns the source-table overrides pointing the first-order parsed views' input
    tables at the tables the run reads: |config|'s result tables (always sandbox-prefixed)
    and its input document store (the sandbox copy when the run seeded one, else
    production)."""
    builder = BigQueryAddressOverrides.Builder(sandbox_prefix=None)
    _register_extraction_results_overrides(
        builder, config=config, results_sandbox_prefix=results_sandbox_prefix
    )
    _register_input_document_store_overrides(
        builder, config=config, document_store_sandbox=document_store_sandbox
    )
    return builder.build()


def post_entity_resolution_view_input_overrides(
    *,
    config: LLMExtractorConfig,
    er_configs: list[LLMExtractorConfig],
    first_order_view_builders: list[BigQueryViewBuilder],
    results_sandbox_prefix: str,
    document_store_sandbox: DocumentStoreSandboxContext | None,
) -> BigQueryAddressOverrides:
    """Returns the source-table overrides for the post-entity-resolution parsed views.

    Covers everything the first-order overrides do — |config|'s first-order result tables
    and its input document store — plus the tables only the post-resolution views read: the
    |er_configs|' result tables, each entity group's entry->source map, and the tables the
    first-order deploy of |first_order_view_builders| produced (the enriched public views
    read the first-order `__pre_resolution` materialized table), all sandbox-prefixed since
    the run writes them. |er_configs| are the entity-resolution configs generated from
    |config|, one per declared entity group.

    The entry->source map lands in the document store metadata dataset but is a result the
    run writes, so it follows the results prefix rather than the document store prefix.

    TODO(OBT-42680): Not yet wired into run_sandbox_extraction, which only runs the
    first-order layer; this will be called once the script runs entity resolution and
    deploys the post-resolution views.
    """
    builder = BigQueryAddressOverrides.Builder(sandbox_prefix=None)
    _register_extraction_results_overrides(
        builder, config=config, results_sandbox_prefix=results_sandbox_prefix
    )
    _register_input_document_store_overrides(
        builder, config=config, document_store_sandbox=document_store_sandbox
    )
    _register_deployed_view_overrides(
        builder,
        view_builders=first_order_view_builders,
        results_sandbox_prefix=results_sandbox_prefix,
    )
    for er_config in er_configs:
        _register_extraction_results_overrides(
            builder, config=er_config, results_sandbox_prefix=results_sandbox_prefix
        )
    _register_addresses(
        builder,
        addresses=EntityResolutionEntrySourceMapBQTable.addresses_for_collection(
            state_code=config.state_code,
            extractor_collection=config.extractor_collection,
            sandbox_prefix=None,
        ),
        sandbox_prefix=results_sandbox_prefix,
    )
    return builder.build()


def deploy_extraction_results_views(
    *,
    config: LLMExtractorConfig,
    view_builders: list[BigQueryViewBuilder],
    results_sandbox_prefix: str,
    input_source_table_overrides: BigQueryAddressOverrides,
    table_expiration_ms: int,
) -> None:
    """Deploys |view_builders| to the sandbox-prefixed datasets, reading from the
    sandbox result tables the run wrote.
    """
    load_collected_views_to_sandbox(
        sandbox_dataset_prefix=results_sandbox_prefix,
        state_code_filter=config.state_code,
        collected_builders=view_builders,
        input_source_table_dataset_overrides_dict=None,
        input_source_table_overrides=input_source_table_overrides,
        allow_slow_views=True,
        rematerialize_changed_views_only=False,
        failure_mode=BigQueryViewDagWalkerProcessingFailureMode.FAIL_FAST,
        schemas_only=False,
        default_table_expiration_ms=table_expiration_ms,
    )
