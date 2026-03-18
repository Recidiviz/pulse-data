"""
Defines tests that no new / unexpected references to v1 sentence views are added upstream
of any view that is part of the given metric export for the given state code.
"""

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
from recidiviz.tests.view_registry.sentences_v1_migration_burndown.us_co_sentences_v1_burndown import (
    US_CO_SENTENCE_V1_PRODUCT_USAGE_EXEMPTIONS,
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
    StateCode.US_CO: US_CO_SENTENCE_V1_PRODUCT_USAGE_EXEMPTIONS,
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


def _is_filtered_address(
    address: BigQueryAddress,
    state_code_filter: StateCode,
    filter_datasets: set[str],
) -> bool:
    address_state_code = address.state_code_for_address()
    return address.dataset_id in filter_datasets or (
        address_state_code is not None and address_state_code != state_code_filter
    )


def _find_deprecated_v1_dependency_edges_for_product_views(
    *,
    dag: BigQueryViewDagWalker,
    deprecated_addresses: set[BigQueryAddress],
    product_view_addresses: set[BigQueryAddress],
    state_code: StateCode,
    filter_datasets: set[str],
) -> dict[BigQueryAddress, set[tuple[BigQueryAddress, BigQueryAddress]]]:
    """For each product view, returns which deprecated v1 views are referenced in that
    product view's ancestor graph, as tuples of (deprecated_view, referencing_view)
    edges, representing the point where a deprecated view is first referenced by a
    non-deprecated view. Only product views with at least one edge are included.

    Walks the view DAG from roots to leaves, propagating a set of v1 reference edges
    through each view.

    The propagation rules are:
    - Deprecated views do not propagate edges; they act as new chain starts. This
      means if deprecated_A -> deprecated_B -> view_C, only (deprecated_B, view_C)
      is tracked (the closest deprecated ancestor).
    - Product views collect edges but do not propagate them to their descendants,
      so each product view only reports deprecated references that reach it without
      passing through another product view in the same export.
    - Views in |filter_datasets| or for a different state are skipped entirely.

    Descendant sub-DAGs must be already populated on |dag|.
    """

    def process_view(
        view: BigQueryView,
        parent_results: dict[
            BigQueryView, set[tuple[BigQueryAddress, BigQueryAddress]]
        ],
    ) -> set[tuple[BigQueryAddress, BigQueryAddress]]:
        addr = view.address
        view_node = dag.node_for_view(view)

        if _is_filtered_address(addr, state_code, filter_datasets):
            return set()

        # Prune views that can't reach any product view
        all_descendants = view_node.descendants_sub_dag.nodes_by_address
        if not any(a in all_descendants for a in product_view_addresses):
            return set()

        # Deprecated views don't propagate - they start new chains
        if addr in deprecated_addresses:
            return set()

        result: set[tuple[BigQueryAddress, BigQueryAddress]] = set()

        # Check for deprecated source tables (tables not in the DAG as views)
        for src_addr in view_node.parent_tables:
            if (
                src_addr in deprecated_addresses
                and src_addr not in dag.nodes_by_address
            ):
                result.add((src_addr, addr))

        for parent_view, parent_edges in parent_results.items():
            if parent_view.address in deprecated_addresses:
                # Parent is deprecated - this view is the first non-deprecated
                # reference, forming a new deprecated view reference edge
                result.add((parent_view.address, addr))
            elif parent_view.address not in product_view_addresses:
                # Inherit edges from non-deprecated, non-product-view parents.
                # Edges stop at product views so that downstream product views
                # only see references that reach them independently.
                result |= parent_edges

        return result

    view_to_edges = dag.process_dag(
        view_process_fn=process_view, synchronous=True
    ).view_results

    return {
        view.address: edges
        for view, edges in view_to_edges.items()
        if edges and view.address in product_view_addresses
    }


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
    export_addresses = _get_product_export_addresses(state_code, export_name)
    deprecated_addresses = _get_deprecated_v1_sentence_addresses_for_state(state_code)

    v1_ancestor_edges_by_product_view = (
        _find_deprecated_v1_dependency_edges_for_product_views(
            dag=dag_walker,
            deprecated_addresses=deprecated_addresses,
            product_view_addresses=export_addresses,
            state_code=state_code,
            filter_datasets={
                SENTENCE_SESSIONS_DATASET,
                SENTENCE_SESSIONS_V2_ALL_DATASET,
            },
        )
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
    ) in v1_ancestor_edges_by_product_view.items():
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
        actual_bad_edges = v1_ancestor_edges_by_product_view.get(
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
