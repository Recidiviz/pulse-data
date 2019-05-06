# Recidiviz - a platform for tracking granular recidivism metrics in real time
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

from recidiviz.calculator.bq import export_config
from recidiviz.calculator.bq.views.bqview import BigQueryView
from recidiviz.calculator.bq.views.state_aggregates import \
    state_aggregate_mappings
from recidiviz.persistence.database import schema_utils
from recidiviz.utils import metadata


def _to_bq_table(query_str: str) -> str:
    """Rename schema table_names with supported BQ syntax."""
    project_id = metadata.project_id()
    base_dataset = export_config.BASE_TABLES_BQ_DATASET

    for table in schema_utils.get_aggregate_table_classes():
        bq_table_name = '`{project_id}.{base_dataset}.{table_name}`'.format(
            project_id=project_id,
            base_dataset=base_dataset,
            table_name=table.__tablename__
        )
        query_str = query_str.replace(table.__tablename__, bq_table_name)

    return query_str


_QUERIES = [m.to_query() for m in state_aggregate_mappings.MAPPINGS]
_UNIONED_STATEMENT = sqlalchemy.union_all(*_QUERIES)
_BQ_UNIONED_STATEMENT = _to_bq_table(str(_UNIONED_STATEMENT.compile()))

# This view is the concatenation of all "state-reported aggregate reports" after
# mapping each column to a shared column_name. The next logical derivation from
# this view is to combine it with a view of our "scraped website data" that maps
# to the same shared column_names. Both of these views should be aggregated to
# the same level (eg. jurisdiction level via jurisdiction_id).
COMBINED_STATE_AGGREGATE_VIEW = BigQueryView(
    view_id='combined_state_aggregates',
    view_query=_BQ_UNIONED_STATEMENT
)
