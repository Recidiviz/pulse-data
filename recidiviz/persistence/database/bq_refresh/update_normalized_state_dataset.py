# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2023 Recidiviz, Inc.
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
"""Functionality to union together dataflow (and legacy ingest) output for all states 
into a single `normalized_state` dataset."""
from google.cloud.bigquery import SchemaField
from more_itertools import one

from recidiviz.big_query.address_overrides import BigQueryAddressOverrides
from recidiviz.big_query.big_query_address import BigQueryAddress
from recidiviz.big_query.big_query_view import (
    BigQueryViewBuilder,
    SimpleBigQueryViewBuilder,
)
from recidiviz.big_query.view_update_manager import (
    create_managed_dataset_and_deploy_views_for_view_builders,
)
from recidiviz.calculator.query.state.dataset_config import (
    NORMALIZED_STATE_DATASET,
    NORMALIZED_STATE_VIEWS_DATASET,
)
from recidiviz.common.constants.states import StateCode
from recidiviz.ingest.direct.regions.direct_ingest_region_utils import (
    get_direct_ingest_states_existing_in_env,
)
from recidiviz.pipelines.ingest.dataset_config import state_dataset_for_state_code
from recidiviz.pipelines.ingest.normalization_in_ingest_gating import (
    is_combined_ingest_and_normalization_launched_in_env,
)
from recidiviz.pipelines.normalization.dataset_config import (
    normalized_state_dataset_for_state_code_ingest_pipeline_output,
    normalized_state_dataset_for_state_code_legacy_normalization_output,
)
from recidiviz.source_tables.ingest_pipeline_output_table_collector import (
    build_ingest_pipeline_output_source_table_collections,
)
from recidiviz.source_tables.normalization_pipeline_output_table_collector import (
    build_normalization_pipeline_output_source_table_collections,
    build_normalization_pipeline_output_table_id_to_schemas,
)
from recidiviz.source_tables.source_table_config import SourceTableConfig
from recidiviz.source_tables.source_table_update_manager import SourceTableUpdateManager
from recidiviz.source_tables.union_tables_output_table_collector import (
    build_unioned_normalized_state_source_table_collection,
)
from recidiviz.utils import metadata


def _build_single_source_table_select_statement(
    output_table_schema: list[SchemaField], input_source_table: SourceTableConfig
) -> str:
    """Builds a SELECT statement that will pull in data from the given input table.
    The result will have the provided output columns. If the source table doesn't have
    a given column, the column will be filled with NULLs.
    """
    input_table_columns = {f.name for f in input_source_table.schema_fields}
    columns_strs = (
        column.name
        if column.name in input_table_columns
        else f"CAST(NULL AS {column.field_type}) AS {column.name}"
        for column in output_table_schema
    )
    project_specific_address = input_source_table.address.to_project_specific_address(
        metadata.project_id()
    )
    return (
        f"SELECT {', '.join(columns_strs)}\n"
        f"FROM {project_specific_address.format_address_for_query()}\n"
    )


def _build_normalized_state_unioned_view_builder(
    output_table_address: BigQueryAddress,
    output_table_schema: list[SchemaField],
    input_tables_by_state: dict[StateCode, SourceTableConfig],
) -> BigQueryViewBuilder:
    """Creates a view builder that combines state-specific outputs for the given input
    tables and materializes to the given output address.
    """
    table_id = output_table_address.table_id
    queries = (
        _build_single_source_table_select_statement(
            output_table_schema, input_tables_by_state[state_code]
        )
        for state_code in sorted(input_tables_by_state, key=lambda s: s.value)
    )

    return SimpleBigQueryViewBuilder(
        dataset_id=NORMALIZED_STATE_VIEWS_DATASET,
        view_id=f"{table_id}_view",
        description=(
            f"Output for {table_id} unioned from state-specific Dataflow "
            f"ingest pipeline us_xx_normalized_state output datasets."
        ),
        view_query_template="UNION ALL\n".join(queries),
        should_materialize=True,
        materialized_address_override=output_table_address,
    )


def _get_normalized_input_dataset_for_table(
    state_code: StateCode, table_id: str
) -> str:
    """For a given table, returns the dataset we should read from for this state_code
    to get data that will be loaded into the normalized_state dataset for that state.
    """
    if is_combined_ingest_and_normalization_launched_in_env(state_code):
        return normalized_state_dataset_for_state_code_ingest_pipeline_output(
            state_code
        )

    # TODO(#31741): Delete all logic below here once normalization in ingest is rolled
    #  out to all states.
    if table_id in build_normalization_pipeline_output_table_id_to_schemas():
        return normalized_state_dataset_for_state_code_legacy_normalization_output(
            state_code
        )

    # If this table is not part of the legacy normalization pipeline output, read from
    # the non-normalized ingest pipeline output instead.
    return state_dataset_for_state_code(state_code)


def _get_all_candidate_source_tables() -> list[SourceTableConfig]:
    collections = (
        *build_ingest_pipeline_output_source_table_collections(),
        *build_normalization_pipeline_output_source_table_collections(),
    )
    return [
        source_table
        for collection in collections
        for source_table in collection.source_tables
    ]


def _normalized_input_source_tables_by_output_table(
    normalized_state_output_source_tables: list[SourceTableConfig],
    states: list[StateCode],
) -> dict[BigQueryAddress, dict[StateCode, SourceTableConfig]]:
    """Builds a map of normalized_state output address -> map of state code to the input
    table we should read from for that state.
    """
    all_candidate_source_tables = _get_all_candidate_source_tables()
    result = {}
    for table in normalized_state_output_source_tables:
        output_address = table.address

        input_tables_by_state = {}
        for state_code in states:
            input_table_dataset_id = _get_normalized_input_dataset_for_table(
                state_code, table.address.table_id
            )
            input_source_table = one(
                t
                for t in all_candidate_source_tables
                if t.address
                == BigQueryAddress(
                    dataset_id=input_table_dataset_id, table_id=table.address.table_id
                )
            )
            input_tables_by_state[state_code] = input_source_table

        result[output_address] = input_tables_by_state
    return result


def get_normalized_state_view_builders(
    state_codes_filter: list[StateCode] | None,
) -> list[BigQueryViewBuilder]:
    states = (
        state_codes_filter
        if state_codes_filter
        else get_direct_ingest_states_existing_in_env()
    )
    normalized_state_output_source_tables = (
        build_unioned_normalized_state_source_table_collection().source_tables
    )
    normalized_input_tables_by_output_table = (
        _normalized_input_source_tables_by_output_table(
            normalized_state_output_source_tables, states
        )
    )
    return [
        _build_normalized_state_unioned_view_builder(
            table.address,
            table.schema_fields,
            input_tables_by_state=normalized_input_tables_by_output_table[
                table.address
            ],
        )
        for table in normalized_state_output_source_tables
    ]


# TODO(#29519): Delete this function when we move the `normalized_state` dataset refresh into the
#  view graph.
def combine_sources_into_single_normalized_state_dataset(
    state_codes_filter: list[StateCode] | None,
    output_sandbox_prefix: str | None,
    input_dataset_overrides: BigQueryAddressOverrides | None,
) -> None:
    """Creates the `state` views for all tables, combining output from each individual
    ingest pipeline.
    """
    view_builders = get_normalized_state_view_builders(state_codes_filter)

    address_overrides = None

    if input_dataset_overrides and not output_sandbox_prefix:
        raise ValueError(
            "Must define output_sandbox_prefix if input_dataset_overrides are set."
        )

    if state_codes_filter and not output_sandbox_prefix:
        raise ValueError(
            "Must define output_sandbox_prefix if state_codes_filter are set."
        )

    if output_sandbox_prefix:
        address_overrides_builder = (
            input_dataset_overrides.to_builder(sandbox_prefix=output_sandbox_prefix)
            if input_dataset_overrides
            else BigQueryAddressOverrides.Builder(sandbox_prefix=output_sandbox_prefix)
        )
        address_overrides_builder.register_sandbox_override_for_entire_dataset(
            NORMALIZED_STATE_VIEWS_DATASET
        )
        address_overrides_builder.register_sandbox_override_for_entire_dataset(
            NORMALIZED_STATE_DATASET
        )
        address_overrides = address_overrides_builder.build()

    create_managed_dataset_and_deploy_views_for_view_builders(
        view_source_table_datasets={
            source_table.address.dataset_id
            for source_table in _get_all_candidate_source_tables()
        },
        view_builders_to_update=view_builders,
        address_overrides=address_overrides,
        force_materialize=True,
        # Clean old tables out of the datasets if they have been removed from the schema.
        historically_managed_datasets_to_clean={
            NORMALIZED_STATE_VIEWS_DATASET,
            NORMALIZED_STATE_DATASET,
        },
    )

    # Ensure that the produced output schema matches the expected schema
    SourceTableUpdateManager().update(
        build_unioned_normalized_state_source_table_collection()
    )
