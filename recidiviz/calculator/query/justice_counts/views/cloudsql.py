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
"""Materialized view of the various tables in postgres."""

from typing import List

from recidiviz.calculator.query.justice_counts import dataset_config
from recidiviz.big_query.big_query_view import SimpleBigQueryViewBuilder
from recidiviz.persistence.database.base_schema import JusticeCountsBase
from recidiviz.persistence.database.schema_table_region_filtered_query_builder import (
    FederatedSchemaTableRegionFilteredQueryBuilder,
)
from recidiviz.persistence.database.schema_utils import get_justice_counts_table_classes
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

# TODO(#7285): Migrate to use standard federaded export code path and delete this file
#   and associated views.
TABLE_QUERY_TEMPLATE = """
    /*{description}*/
    SELECT
        *
    FROM EXTERNAL_QUERY("{project_id}.US.justice_counts_cloudsql", "{postgres_query}")
    """


def get_table_view_builders() -> List[SimpleBigQueryViewBuilder]:
    """Returns populated, formatted query builders for all Justice Counts BigQuery views."""
    table_view_builders = []
    for table in get_justice_counts_table_classes():
        query_builder = FederatedSchemaTableRegionFilteredQueryBuilder(
            metadata_base=JusticeCountsBase,
            table=table,
            columns_to_include=[c.name for c in table.columns],
        )
        table_view_builders.append(
            SimpleBigQueryViewBuilder(
                dataset_id=dataset_config.JUSTICE_COUNTS_BASE_DATASET,
                view_id=table.name,
                view_query_template=TABLE_QUERY_TEMPLATE,
                should_materialize=True,
                description=f"Data from the {table.name} table imported into BQ",
                postgres_query=query_builder.full_query(),
            )
        )
    return table_view_builders


if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        for builder in get_table_view_builders():
            builder.build_and_print()
