"""
Defines tests that no new / unexpected references to v1 sentence views are added upstream
of any view that is part of the given metric export for the given state code.
"""

from collections import defaultdict
from functools import cache

import pytest

from recidiviz.big_query.big_query_address import BigQueryAddress
from recidiviz.big_query.big_query_view import BigQueryView
from recidiviz.big_query.big_query_view_dag_walker import BigQueryViewDagWalker
from recidiviz.big_query.big_query_view_utils import build_views_to_update
from recidiviz.calculator.query.state.dataset_config import (
    SENTENCE_SESSIONS_DATASET,
    SENTENCE_SESSIONS_V2_ALL_DATASET,
)
from recidiviz.common.constants.states import StateCode
from recidiviz.metrics.export.export_config import VIEW_COLLECTION_EXPORT_INDEX
from recidiviz.metrics.export.products.product_configs import ProductConfigs
from recidiviz.tests.view_registry.sentences_v1_migration_burndown.us_ar_sentences_v1_burndown import (
    US_AR_SENTENCE_V1_PRODUCT_USAGE_EXEMPTIONS,
)
from recidiviz.tests.view_registry.sentences_v1_migration_burndown.us_az_sentences_v1_burndown import (
    US_AZ_SENTENCE_V1_PRODUCT_USAGE_EXEMPTIONS,
)
from recidiviz.tests.view_registry.sentences_v1_migration_burndown.us_ca_sentences_v1_burndown import (
    US_CA_SENTENCE_V1_PRODUCT_USAGE_EXEMPTIONS,
)
from recidiviz.tests.view_registry.sentences_v1_migration_burndown.us_ia_sentences_v1_burndown import (
    US_IA_SENTENCE_V1_PRODUCT_USAGE_EXEMPTIONS,
)
from recidiviz.tests.view_registry.sentences_v1_migration_burndown.us_ix_sentences_v1_burndown import (
    US_IX_SENTENCE_V1_PRODUCT_USAGE_EXEMPTIONS,
)
from recidiviz.tests.view_registry.sentences_v1_migration_burndown.us_ma_sentences_v1_burndown import (
    US_MA_SENTENCE_V1_PRODUCT_USAGE_EXEMPTIONS,
)
from recidiviz.tests.view_registry.sentences_v1_migration_burndown.us_me_sentences_v1_burndown import (
    US_ME_SENTENCE_V1_PRODUCT_USAGE_EXEMPTIONS,
)
from recidiviz.tests.view_registry.sentences_v1_migration_burndown.us_mi_sentences_v1_burndown import (
    US_MI_SENTENCE_V1_PRODUCT_USAGE_EXEMPTIONS,
)
from recidiviz.tests.view_registry.sentences_v1_migration_burndown.us_mo_sentences_v1_burndown import (
    US_MO_SENTENCE_V1_PRODUCT_USAGE_EXEMPTIONS,
)
from recidiviz.tests.view_registry.sentences_v1_migration_burndown.us_nc_sentences_v1_burndown import (
    US_NC_SENTENCE_V1_PRODUCT_USAGE_EXEMPTIONS,
)
from recidiviz.tests.view_registry.sentences_v1_migration_burndown.us_nd_sentences_v1_burndown import (
    US_ND_SENTENCE_V1_PRODUCT_USAGE_EXEMPTIONS,
)
from recidiviz.tests.view_registry.sentences_v1_migration_burndown.us_ne_sentences_v1_burndown import (
    US_NE_SENTENCE_V1_PRODUCT_USAGE_EXEMPTIONS,
)
from recidiviz.tests.view_registry.sentences_v1_migration_burndown.us_ny_sentences_v1_burndown import (
    US_NY_SENTENCE_V1_PRODUCT_USAGE_EXEMPTIONS,
)
from recidiviz.tests.view_registry.sentences_v1_migration_burndown.us_pa_sentences_v1_burndown import (
    US_PA_SENTENCE_V1_PRODUCT_USAGE_EXEMPTIONS,
)
from recidiviz.tests.view_registry.sentences_v1_migration_burndown.us_tn_sentences_v1_burndown import (
    US_TN_SENTENCE_V1_PRODUCT_USAGE_EXEMPTIONS,
)
from recidiviz.tests.view_registry.sentences_v1_migration_burndown.us_tx_sentences_v1_burndown import (
    US_TX_SENTENCE_V1_PRODUCT_USAGE_EXEMPTIONS,
)
from recidiviz.tests.view_registry.sentences_v1_migration_burndown.us_ut_sentences_v1_burndown import (
    US_UT_SENTENCE_V1_PRODUCT_USAGE_EXEMPTIONS,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override
from recidiviz.view_registry.deployed_views import deployed_view_builders
from recidiviz.view_registry.deprecated_view_reference_exemptions import (
    SENTENCES_V1_DEPRECATED_VIEWS_AND_USAGE_EXEMPTIONS,
)

# For each state and metric export, for each product view in that export, a mapping of
#   deprecated v1 sentences views that are a) referenced directly without going through
#   sentence_sessions/sentence_sessions_v2_all and are b) part of the ancestor graph of
#   this product view, with all places they are referenced.
_SENTENCE_V1_PRODUCT_USAGE_EXEMPTIONS: dict[
    StateCode,
    dict[str, dict[BigQueryAddress, dict[BigQueryAddress, set[BigQueryAddress]]]],
] = {
    StateCode.US_AR: US_AR_SENTENCE_V1_PRODUCT_USAGE_EXEMPTIONS,
    StateCode.US_AZ: US_AZ_SENTENCE_V1_PRODUCT_USAGE_EXEMPTIONS,
    StateCode.US_CA: US_CA_SENTENCE_V1_PRODUCT_USAGE_EXEMPTIONS,
    StateCode.US_IA: US_IA_SENTENCE_V1_PRODUCT_USAGE_EXEMPTIONS,
    StateCode.US_IX: US_IX_SENTENCE_V1_PRODUCT_USAGE_EXEMPTIONS,
    StateCode.US_MA: US_MA_SENTENCE_V1_PRODUCT_USAGE_EXEMPTIONS,
    StateCode.US_ME: US_ME_SENTENCE_V1_PRODUCT_USAGE_EXEMPTIONS,
    StateCode.US_MI: US_MI_SENTENCE_V1_PRODUCT_USAGE_EXEMPTIONS,
    StateCode.US_MO: US_MO_SENTENCE_V1_PRODUCT_USAGE_EXEMPTIONS,
    StateCode.US_NC: US_NC_SENTENCE_V1_PRODUCT_USAGE_EXEMPTIONS,
    StateCode.US_ND: US_ND_SENTENCE_V1_PRODUCT_USAGE_EXEMPTIONS,
    StateCode.US_NE: US_NE_SENTENCE_V1_PRODUCT_USAGE_EXEMPTIONS,
    StateCode.US_NY: US_NY_SENTENCE_V1_PRODUCT_USAGE_EXEMPTIONS,
    StateCode.US_PA: US_PA_SENTENCE_V1_PRODUCT_USAGE_EXEMPTIONS,
    StateCode.US_TN: US_TN_SENTENCE_V1_PRODUCT_USAGE_EXEMPTIONS,
    StateCode.US_TX: US_TX_SENTENCE_V1_PRODUCT_USAGE_EXEMPTIONS,
    StateCode.US_UT: US_UT_SENTENCE_V1_PRODUCT_USAGE_EXEMPTIONS,
}


@cache
def _get_staging_dag_walker_with_descendant_sub_dags() -> BigQueryViewDagWalker:
    """Build and cache the DAG walker for the full view graph, populating descendant
    sub-dags.
    """
    with local_project_id_override(GCP_PROJECT_STAGING):
        view_builders = deployed_view_builders()
        views = build_views_to_update(
            candidate_view_builders=view_builders,
            sandbox_context=None,
        )
        walker = BigQueryViewDagWalker(views)
        walker.populate_descendant_sub_dags()
        return walker


@pytest.fixture(scope="session", name="dag_walker")
def _dag_walker() -> BigQueryViewDagWalker:
    """One-time BigQuery DAG setup fixture."""
    return _get_staging_dag_walker_with_descendant_sub_dags()


def _get_deprecated_v1_sentence_addresses_for_state(
    state_code: StateCode,
) -> set[BigQueryAddress]:
    """Returns all the deprecated v1 sentences views / tables that are relevant to
    the given state_code (i.e. match that state_code or are state-agnostic).
    """
    return {
        address
        for address in SENTENCES_V1_DEPRECATED_VIEWS_AND_USAGE_EXEMPTIONS
        if not address.state_code_for_address()
        or address.state_code_for_address() == state_code
    }


def _get_product_export_addresses(
    state_code: StateCode, export_name: str
) -> set[BigQueryAddress]:
    """Returns all the BQ views relevant to this state_code which are part of the
    metric export with the given export_name.
    """
    export_config = VIEW_COLLECTION_EXPORT_INDEX[export_name]
    return {
        vb.address
        for vb in export_config.view_builders_to_export
        if not vb.address.state_code_for_address()
        or vb.address.state_code_for_address() == state_code
    }


def _should_filter_out_paths_with_address(
    *,
    address: BigQueryAddress,
    state_code_filter: StateCode | None,
    filter_addresses: set[BigQueryAddress],
    filter_datasets: set[str],
) -> bool:
    address_state_code = address.state_code_for_address()
    return (
        address in filter_addresses
        or address.dataset_id in filter_datasets
        or (
            state_code_filter is not None
            and address_state_code is not None
            and address_state_code != state_code_filter
        )
    )


def find_all_paths_from_start_to_end_collections(
    *,
    dag: BigQueryViewDagWalker,
    start_addresses: set[BigQueryAddress],
    end_addresses: set[BigQueryAddress],
    state_code_filter: StateCode | None,
    filter_addresses: set[BigQueryAddress],
    filter_datasets: set[str],
) -> list[list[BigQueryAddress]]:
    """Find all paths that start at any of the start_addresses and terminate at any
    of the end_addresses. All paths are terminated once we reach the first end address.
    If one path is a full sub path of another, it is dropped.

    Descendant sub-DAGs must be already populated on |dag|.
    """

    def process_view(
        view: BigQueryView,
        parent_results: dict[BigQueryView, list[list[BigQueryAddress]]],
    ) -> list[list[BigQueryAddress]]:
        addr = view.address
        view_node = dag.node_for_view(view)

        if _should_filter_out_paths_with_address(
            address=view.address,
            state_code_filter=state_code_filter,
            filter_addresses=filter_addresses,
            filter_datasets=filter_datasets,
        ):
            return []

        all_descendants: set[BigQueryAddress] = set(
            view_node.descendants_sub_dag.nodes_by_address
        )
        if not any(a in all_descendants for a in end_addresses):
            # This is a dead end - we won't get to any of the end
            # addresses if we search this way.
            return []

        paths: list[list[BigQueryAddress]] = []

        # Add direct table-to-view paths (for source tables that are start addresses)
        for src_addr in dag.node_for_view(view).parent_tables:
            if src_addr in start_addresses and src_addr not in dag.nodes_by_address:
                paths.append([src_addr, addr])

        # Start path if this is a start address and there aren't already paths to this
        # from other start views.
        if addr in start_addresses and not parent_results:
            paths.append([addr])

        # Extend paths with this view's address
        for parent_paths in parent_results.values():
            for path in parent_paths:
                # Don't count paths that already terminated in an end address
                if path[-1] not in end_addresses:
                    paths.append(path + [addr])

        return paths

    view_to_paths = dag.process_dag(
        view_process_fn=process_view, synchronous=True
    ).view_results

    # Collect only paths ending in one of the end addresses
    return [
        path
        for view, paths in view_to_paths.items()
        if view.address in end_addresses
        for path in paths
    ]


def find_unique_paths_from_last_valid_start_address_to_terminal(
    paths: list[list[BigQueryAddress]],
    valid_start_addresses: set[BigQueryAddress],
) -> list[list[BigQueryAddress]]:
    """Truncate each path to start from the last occurrence of a valid start address.

    For each path, search backwards until we hit a valid start address, then keep only
    the end of the path starting with that address. Deduplicate the resulting paths.

    Args:
        paths: List of paths (each path is a list of addresses)
        valid_start_addresses: Set of valid start addresses to search for

    Returns:
        Deduplicated list of truncated paths
    """
    truncated_paths = []

    for path in paths:
        # Search backwards to find the last valid start address in the path
        truncate_index = None
        for i in range(len(path) - 1, -1, -1):
            if path[i] in valid_start_addresses:
                truncate_index = i
                break

        # Keep from the ancestor to the end
        if truncate_index is not None:
            truncated_paths.append(path[truncate_index:])

    # Deduplicate by converting to tuples (hashable), using set, then back to lists
    unique_paths = list({tuple(path): path for path in truncated_paths}.values())

    return unique_paths


def _state_code_and_export_tuples() -> list[tuple[StateCode, str]]:
    """Generate all valid (state_code, export_name) pairs based on products.yaml."""
    product_configs = ProductConfigs.from_file()
    valid_pairs = []

    for state_code in StateCode:
        # Get exports configured for this state
        relevant_exports = product_configs.get_export_configs_for_job_filter(
            state_code.value
        )
        export_names = {export["export_job_name"] for export in relevant_exports}

        # Only include pairs where the export is configured for this state
        for export_name in sorted(export_names):
            if export_name in VIEW_COLLECTION_EXPORT_INDEX:
                valid_pairs.append((state_code, export_name))

    return valid_pairs


@pytest.mark.parametrize(("state_code", "export_name"), _state_code_and_export_tuples())
def test_no_v1_references_for_exported_views(
    state_code: StateCode,
    export_name: str,
    # From the _dag_walker() fixture above
    dag_walker: BigQueryViewDagWalker,
) -> None:
    """Tests that no new / unexpected references to v1 sentence views are added upstream
    of any view that is part of the given metric export for the given state code.
    """
    deprecated_addresses = _get_deprecated_v1_sentence_addresses_for_state(state_code)
    paths = find_all_paths_from_start_to_end_collections(
        dag=dag_walker,
        start_addresses=deprecated_addresses,
        end_addresses=_get_product_export_addresses(state_code, export_name),
        state_code_filter=state_code,
        filter_addresses=set(),
        filter_datasets={SENTENCE_SESSIONS_DATASET, SENTENCE_SESSIONS_V2_ALL_DATASET},
    )

    # Truncate paths to start at the last deprecated address in the path
    paths = find_unique_paths_from_last_valid_start_address_to_terminal(
        paths, valid_start_addresses=deprecated_addresses
    )

    # Group paths by end product address and extract first edges (the first place we see
    # a deprecated view referenced by a non-deprecated view)
    product_view_address_path_first_edges: dict[
        BigQueryAddress, set[tuple[BigQueryAddress, BigQueryAddress]]
    ] = defaultdict(set)

    for path in paths:
        product_view_address = path[-1]
        product_view_address_path_first_edges[product_view_address].add(
            (path[0], path[1])
        )

    # Get exemptions for this state and export, converting from dict to set of tuples
    state_exemptions_dict = _SENTENCE_V1_PRODUCT_USAGE_EXEMPTIONS.get(
        state_code, {}
    ).get(export_name, {})

    # Convert dict-based exemptions to set of tuples for easier comparison
    state_exemptions: dict[
        BigQueryAddress, set[tuple[BigQueryAddress, BigQueryAddress]]
    ] = {}
    for product_view_address, edges_dict in state_exemptions_dict.items():
        state_exemptions[product_view_address] = {
            (start_addr, next_addr)
            for start_addr, next_addrs in edges_dict.items()
            for next_addr in next_addrs
        }

    errors = []
    for (
        product_view_address,
        bad_edges,
    ) in product_view_address_path_first_edges.items():
        # Get exempted edges for this product view
        exempted_edges = state_exemptions.get(product_view_address, set())

        # Find edges that are not exempted
        non_exempted_edges = bad_edges - exempted_edges

        if non_exempted_edges:
            formatted_edges = "\n".join(
                f"    {start.to_str()} --> {next_view.to_str()}"
                for start, next_view in sorted(
                    non_exempted_edges, key=lambda e: (e[0].to_str(), e[1].to_str())
                )
            )
            errors.append(
                f"  Product view: {product_view_address.to_str()}\n"
                f"  Deprecated ancestors (non-exempted):\n{formatted_edges}"
            )

    # Check for stale exemptions (exemptions that are no longer necessary)
    for product_view_address, exempted_edges in state_exemptions.items():
        # Get the actual bad edges for this product view
        actual_bad_edges = product_view_address_path_first_edges.get(
            product_view_address, set()
        )

        # Find exemptions that are no longer needed
        stale_exemptions = exempted_edges - actual_bad_edges

        if stale_exemptions:
            formatted_edges = "\n".join(
                f"    {start.to_str()} --> {next_view.to_str()}"
                for start, next_view in sorted(
                    stale_exemptions, key=lambda e: (e[0].to_str(), e[1].to_str())
                )
            )
            errors.append(
                f"  Product view: {product_view_address.to_str()}\n"
                f"  Stale exemptions (no longer needed):\n{formatted_edges}"
            )

    if errors:
        error_message = (
            f"Found issues with v1 sentence view references in "
            f"ancestors of [{export_name}] views for {state_code.value}:\n\n"
            + "\n\n".join(errors)
        )
        raise AssertionError(error_message)
