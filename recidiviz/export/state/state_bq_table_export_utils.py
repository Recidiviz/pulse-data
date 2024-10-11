# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2020 Recidiviz, Inc.
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

"""Exports data from the state dataset in BigQuery to another dataset in BigQuery."""
from types import ModuleType
from typing import List, Optional

from google.cloud import bigquery

from recidiviz.big_query.big_query_address import ProjectSpecificBigQueryAddress
from recidiviz.ingest.views.dataset_config import (
    NORMALIZED_STATE_DATASET,
    STATE_BASE_DATASET,
)
from recidiviz.persistence.entity.entities_bq_schema import (
    get_bq_schema_for_entity_table,
)
from recidiviz.persistence.entity.state import entities as state_entities
from recidiviz.persistence.entity.state import normalized_entities


def state_table_export_query_str(
    table: bigquery.table.TableListItem, state_codes: List[str]
) -> Optional[str]:
    """Returns a query string that can retrieve the data from the given table for the
    given states.
    """

    address = ProjectSpecificBigQueryAddress.from_table_reference(table.reference)

    if address.dataset_id == STATE_BASE_DATASET:
        entities_module: ModuleType = state_entities
    elif address.dataset_id == NORMALIZED_STATE_DATASET:
        entities_module = normalized_entities
    else:
        raise ValueError(
            f"Received export request for a table not in the `state` or "
            f"`normalized_state` dataset, which is required. Was in dataset "
            f"[{address.dataset_id}] instead"
        )
    table_schema = get_bq_schema_for_entity_table(
        entities_module, table_id=address.table_id
    )

    columns_str = ",".join(f"{address.table_id}.{c.name}" for c in table_schema)

    state_codes_str = ",".join(
        [f"'{region_code.upper()}'" for region_code in state_codes]
    )
    return (
        f"SELECT {columns_str} "
        f"FROM {address.format_address_for_query()} {address.table_id} "
        f"WHERE state_code IN ({state_codes_str});"
    )
