# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2024 Recidiviz, Inc.
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
"""Defines view builders that can be used to generate the `state` dataset."""
from google.cloud.bigquery import SchemaField

from recidiviz.big_query.big_query_address import BigQueryAddress
from recidiviz.big_query.big_query_view import BigQueryViewBuilder
from recidiviz.big_query.union_all_big_query_view_builder import (
    UnionAllBigQueryViewBuilder,
)
from recidiviz.common.constants.states import StateCode
from recidiviz.ingest.direct.regions.direct_ingest_region_utils import (
    get_direct_ingest_states_existing_in_env,
)
from recidiviz.ingest.views.dataset_config import (
    STATE_BASE_DATASET,
    STATE_BASE_VIEWS_DATASET,
)
from recidiviz.persistence.entity.entities_bq_schema import (
    get_bq_schema_for_entities_module,
)
from recidiviz.persistence.entity.state import entities as state_entities
from recidiviz.pipelines.ingest.dataset_config import state_dataset_for_state_code


def _view_builder_for_table(
    table_id: str, table_schema: list[SchemaField], states: list[StateCode]
) -> BigQueryViewBuilder:
    """Creates a view builder for the given table that combines state-specific ingest
    pipeline output into a single view that will materialize to the `state` dataset.
    """
    column_names = [column.name for column in table_schema]
    state_specific_addresses = [
        BigQueryAddress(
            dataset_id=state_dataset_for_state_code(state_code),
            table_id=table_id,
        )
        for state_code in states
    ]

    return UnionAllBigQueryViewBuilder(
        dataset_id=STATE_BASE_VIEWS_DATASET,
        view_id=f"{table_id}_view",
        description=(
            f"Output for {table_id} unioned from state-specific Dataflow "
            f"ingest pipeline us_xx_state output datasets."
        ),
        parents=state_specific_addresses,
        custom_select_statement=f"SELECT {', '.join(column_names)}\n",
        materialized_address_override=BigQueryAddress(
            dataset_id=STATE_BASE_DATASET,
            table_id=table_id,
        ),
    )


def create_unioned_state_dataset_view_builders() -> list[BigQueryViewBuilder]:
    """Returns a list of view builders that can be materialized to form the `state`
    dataset, a BQ representation of the schema defined in state/entities.py.
    """
    states = get_direct_ingest_states_existing_in_env()
    return [
        _view_builder_for_table(table_id, schema_fields, states=states)
        for table_id, schema_fields in get_bq_schema_for_entities_module(
            state_entities
        ).items()
    ]
