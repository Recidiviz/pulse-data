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
"""Defines view builders that can be used to generate the `normalized_state` dataset."""
from google.cloud.bigquery import SchemaField

from recidiviz.big_query.big_query_address import BigQueryAddress
from recidiviz.big_query.big_query_view import BigQueryViewBuilder
from recidiviz.big_query.union_all_big_query_view_builder import (
    UnionAllBigQueryViewBuilder,
)
from recidiviz.calculator.query.state.dataset_config import (
    NORMALIZED_STATE_DATASET,
    NORMALIZED_STATE_VIEWS_DATASET,
)
from recidiviz.common.constants.states import StateCode
from recidiviz.ingest.direct.regions.direct_ingest_region_utils import (
    get_direct_ingest_states_existing_in_env,
)
from recidiviz.persistence.entity.entities_bq_schema import (
    get_bq_schema_for_entities_module,
)
from recidiviz.persistence.entity.state import normalized_entities
from recidiviz.pipelines.ingest.dataset_config import (
    normalized_state_dataset_for_state_code,
)


def _view_builder_for_table(
    table_id: str, table_schema: list[SchemaField], states: list[StateCode]
) -> BigQueryViewBuilder:
    """Creates a view builder for the given table that combines state-specific ingest
    pipeline output into a single view that will materialize to the `normalized_state`
    dataset.
    """
    column_names = [column.name for column in table_schema]
    state_specific_addresses = [
        BigQueryAddress(
            dataset_id=normalized_state_dataset_for_state_code(state_code),
            table_id=table_id,
        )
        for state_code in states
    ]

    return UnionAllBigQueryViewBuilder(
        dataset_id=NORMALIZED_STATE_VIEWS_DATASET,
        view_id=f"{table_id}_view",
        description=(
            f"Output for {table_id} unioned from state-specific Dataflow "
            f"ingest pipeline us_xx_normalized_state_new output datasets."
        ),
        parents=state_specific_addresses,
        custom_select_statement=f"SELECT {', '.join(column_names)}\n",
        materialized_address_override=BigQueryAddress(
            dataset_id=NORMALIZED_STATE_DATASET,
            table_id=table_id,
        ),
    )


def create_unioned_normalized_state_dataset_view_builders() -> list[
    BigQueryViewBuilder
]:
    """Returns a list of view builders that can be materialized to form the
    `normalized_state` dataset, a BQ representation of the schema defined in
    state/normalized_entities.py.
    """
    states = get_direct_ingest_states_existing_in_env()
    return [
        _view_builder_for_table(table_id, schema_fields, states=states)
        for table_id, schema_fields in get_bq_schema_for_entities_module(
            normalized_entities
        ).items()
    ]
