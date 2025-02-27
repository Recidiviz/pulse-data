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
into a single `state` dataset."""
from typing import List, Optional

from sqlalchemy import Table

from recidiviz.big_query.address_overrides import BigQueryAddressOverrides
from recidiviz.big_query.big_query_address import (
    BigQueryAddress,
    ProjectSpecificBigQueryAddress,
)
from recidiviz.big_query.big_query_view import (
    BigQueryViewBuilder,
    SimpleBigQueryViewBuilder,
)
from recidiviz.big_query.view_update_manager import (
    create_managed_dataset_and_deploy_views_for_view_builders,
)
from recidiviz.calculator.query.state.dataset_config import (
    STATE_BASE_DATASET,
    STATE_BASE_LEGACY_DATASET,
    STATE_BASE_VIEWS_DATASET,
    state_dataset_for_state_code,
)
from recidiviz.common.constants.states import StateCode
from recidiviz.ingest.direct.gating import is_ingest_in_dataflow_enabled
from recidiviz.ingest.direct.regions.direct_ingest_region_utils import (
    get_direct_ingest_states_existing_in_env,
)
from recidiviz.ingest.direct.types.direct_ingest_instance import DirectIngestInstance
from recidiviz.persistence.database.schema_table_region_filtered_query_builder import (
    BigQuerySchemaTableRegionFilteredQueryBuilder,
)
from recidiviz.utils import metadata


def _view_builder_for_table(
    table: Table,
    ingest_instance: DirectIngestInstance,
    dataflow_states: List[StateCode],
    legacy_states: List[StateCode],
) -> BigQueryViewBuilder:
    """Creates a view builder for the given state table that combines per state output
    from legacy ingest and ingest pipelines into a single view."""
    column_names = [column.name for column in table.columns]
    legacy_address = ProjectSpecificBigQueryAddress(
        project_id=metadata.project_id(),
        dataset_id=STATE_BASE_LEGACY_DATASET,
        table_id=table.name,
    )
    legacy_state_codes_for_query = (
        BigQuerySchemaTableRegionFilteredQueryBuilder.format_region_codes_for_sql(
            [state_code.value for state_code in legacy_states]
        )
    )
    dataflow_addresses = [
        ProjectSpecificBigQueryAddress(
            project_id=metadata.project_id(),
            dataset_id=state_dataset_for_state_code(state_code, ingest_instance),
            table_id=table.name,
        )
        for state_code in dataflow_states
    ]

    queries = [
        # Legacy query
        f"SELECT {', '.join(column_names)}\n"
        f"FROM {legacy_address.format_address_for_query()}\n"
        f"WHERE state_code IN ({legacy_state_codes_for_query})\n"
    ] + [
        # Dataflow queries
        f"SELECT {', '.join(column_names)}\n"
        f"FROM {dataflow_address.format_address_for_query()}\n"
        for dataflow_address in dataflow_addresses
    ]

    return SimpleBigQueryViewBuilder(
        dataset_id=STATE_BASE_VIEWS_DATASET,
        view_id=f"{table.name}_view",
        # TODO(#20930): Remove legacy ingest from description.
        description=f"Output for {table.name} unioned from dataflow and legacy ingest.",
        view_query_template="UNION ALL\n".join(queries),
        should_materialize=True,
        materialized_address_override=BigQueryAddress(
            dataset_id=STATE_BASE_DATASET, table_id=table.name
        ),
    )


def combine_ingest_sources_into_single_state_dataset(
    ingest_instance: DirectIngestInstance,
    tables: List[Table],
    output_sandbox_prefix: Optional[str] = None,
) -> None:
    """Creates the `state` views for all tables, combining output from legacy ingest and
    pipelines."""
    if ingest_instance == DirectIngestInstance.SECONDARY and not output_sandbox_prefix:
        raise ValueError(
            "Refresh can only proceed for secondary databases into a sandbox."
        )

    dataflow_states, legacy_states = [], []
    for state in get_direct_ingest_states_existing_in_env():
        # TODO(#24285): Check if a run has actually happened (recently?).
        if is_ingest_in_dataflow_enabled(state, ingest_instance):
            dataflow_states.append(state)
        else:
            legacy_states.append(state)

    view_builders = [
        _view_builder_for_table(
            table,
            ingest_instance=ingest_instance,
            dataflow_states=dataflow_states,
            legacy_states=legacy_states,
        )
        for table in tables
    ]

    address_overrides = None
    if output_sandbox_prefix:
        address_overrides_builder = BigQueryAddressOverrides.Builder(
            sandbox_prefix=output_sandbox_prefix
        )
        address_overrides_builder.register_sandbox_override_for_entire_dataset(
            STATE_BASE_LEGACY_DATASET
        )
        address_overrides_builder.register_sandbox_override_for_entire_dataset(
            STATE_BASE_VIEWS_DATASET
        )
        address_overrides_builder.register_sandbox_override_for_entire_dataset(
            STATE_BASE_DATASET
        )
        address_overrides = address_overrides_builder.build()

    create_managed_dataset_and_deploy_views_for_view_builders(
        view_source_table_datasets={STATE_BASE_LEGACY_DATASET}
        | {
            state_dataset_for_state_code(state, ingest_instance)
            for state in dataflow_states
        },
        view_builders_to_update=view_builders,
        address_overrides=address_overrides,
        force_materialize=True,
        # Clean old tables out of the datasets if they have been removed from the schema.
        historically_managed_datasets_to_clean={
            STATE_BASE_VIEWS_DATASET,
            STATE_BASE_DATASET,
        },
    )
