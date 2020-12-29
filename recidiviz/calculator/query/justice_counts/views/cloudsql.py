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

import sqlalchemy

from recidiviz.calculator.query.justice_counts import dataset_config
from recidiviz.big_query.big_query_view import SimpleBigQueryViewBuilder
from recidiviz.persistence.database.schema_utils import get_justice_counts_table_classes
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

# TODO(#5081): Instead of querying directly from Postgres, migrate this to use the export to CSV and import into BQ flow
# so that it is consistent with other databases. This will require splitting Dimensions and DimensionValues out into
# their own entities in the Postgres schema, as arrays are not supported in CSV imports to BQ. We can then build a view
# to recreate the arrays here.
TABLE_QUERY_TEMPLATE = \
    """
    /*{description}*/
    SELECT
        *
    FROM EXTERNAL_QUERY("{project_id}.US.justice_counts_cloudsql", "SELECT {columns} FROM {table};")
    """

def get_table_view_builders() -> List[SimpleBigQueryViewBuilder]:
    table_view_builders = []
    for table in get_justice_counts_table_classes():
        select_columns = []
        for column in table.columns:
            if isinstance(column.type, sqlalchemy.Enum):
                select_columns.append(f"CAST({column.name} as VARCHAR)")
            else:
                select_columns.append(column.name)

        table_view_builders.append(SimpleBigQueryViewBuilder(
            dataset_id=dataset_config.JUSTICE_COUNTS_BASE_DATASET,
            view_id=table.name,
            view_query_template=TABLE_QUERY_TEMPLATE,
            should_materialize=True,
            description=f"Data from the {table.name} table imported into BQ",
            columns=', '.join(select_columns),
            table=table.name,
        ))
    return table_view_builders

if __name__ == '__main__':
    with local_project_id_override(GCP_PROJECT_STAGING):
        for builder in get_table_view_builders():
            builder.build_and_print()
