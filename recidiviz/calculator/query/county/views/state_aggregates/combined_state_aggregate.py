# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2019 Recidiviz, Inc.
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
"""Define views created by concatenating state-reported aggregate reports."""

import sqlalchemy
from sqlalchemy.sql.sqltypes import SchemaType

from recidiviz.big_query.big_query_view import SimpleBigQueryViewBuilder
from recidiviz.calculator.query.county import dataset_config
from recidiviz.calculator.query.county.views.state_aggregates import (
    state_aggregate_mappings,
)
from recidiviz.persistence.database import schema_utils
from recidiviz.persistence.database.session_factory import SessionFactory
from recidiviz.persistence.database.sqlalchemy_database_key import SQLAlchemyDatabaseKey


def _to_bq_table(query_str: str) -> str:
    """Rename schema table_names with supported BQ syntax."""
    base_dataset = dataset_config.COUNTY_BASE_DATASET

    for table in schema_utils.get_aggregate_table_classes():
        bq_table_name = "`{{project_id}}.{base_dataset}.{table_name}`".format(
            base_dataset=base_dataset, table_name=table.name
        )
        query_str = query_str.replace(table.name, bq_table_name)

    return query_str


with SessionFactory.using_database(
    database_key=SQLAlchemyDatabaseKey.for_schema(SchemaType.JAILS),
    autocommit=False,
) as session:
    _QUERIES = [m.to_query(session) for m in state_aggregate_mappings.MAPPINGS]
    _UNIONED_STATEMENT = sqlalchemy.union_all(*_QUERIES)
    _BQ_UNIONED_STATEMENT_QUERY_TEMPLATE = _to_bq_table(
        str(_UNIONED_STATEMENT.compile())
    )
COMBINED_STATE_AGGREGATE_DESCRIPTION = """
The concatenation of all 'state-reported aggregate reports' after mapping each column
to a shared column_name."""

# This view is the concatenation of all "state-reported aggregate reports" after
# mapping each column to a shared column_name. The next logical derivation from
# this view is to combine it with a view of our "scraped website data" that maps
# to the same shared column_names. Both of these views should be aggregated to
# the same level (eg. jurisdiction level via jurisdiction_id).
COMBINED_STATE_AGGREGATE_VIEW_BUILDER: SimpleBigQueryViewBuilder = (
    SimpleBigQueryViewBuilder(
        dataset_id=dataset_config.UNMANAGED_VIEWS_DATASET,
        view_id="combined_state_aggregates",
        description=COMBINED_STATE_AGGREGATE_DESCRIPTION,
        view_query_template=_BQ_UNIONED_STATEMENT_QUERY_TEMPLATE,
    )
)
