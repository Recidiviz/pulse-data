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
"""A view collector that generates BigQuery views that count the instances of
particular enum values for a given table.

Its results are split by state and by whether it is a placeholder object.
"""
from typing import List

from recidiviz.big_query.big_query_view import SimpleBigQueryViewBuilder
from recidiviz.big_query.big_query_view_collector import BigQueryViewCollector
from recidiviz.ingest.views.dataset_config import VIEWS_DATASET
from recidiviz.ingest.views.metadata_helpers import (
    METADATA_EXCLUDED_PROPERTIES,
    METADATA_TABLES_WITH_CUSTOM_COUNTERS,
    BigQueryTableChecker,
    get_enum_property_names,
    get_state_tables,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

ENUM_COUNTER_STATE_QUERY_TEMPLATE = \
    """
SELECT
  state_code,
  IF({column_name} IS NULL, 'NULL', {column_name}) AS {column_name},
  COUNT(*) AS total_count,
  0 AS placeholder_count
FROM
  `{project_id}.state.{table_name}`
GROUP BY state_code, `{column_name}`
ORDER BY state_code, `{column_name}`;
"""

ENUM_COUNTER_WITH_PLACEHOLDERS_STATE_QUERY_TEMPLATE = \
    """
SELECT
  state_code,
  IF({column_name} IS NULL, 'NULL', {column_name}) AS {column_name},
  COUNT(*) AS total_count,
  IFNULL(SUM(CASE WHEN external_id IS NULL THEN 1 END), 0) AS placeholder_count
FROM
  `{project_id}.state.{table_name}`
GROUP BY state_code, `{column_name}`
ORDER BY state_code, `{column_name}`;
"""


class StateTableEnumCounterBigQueryViewCollector(BigQueryViewCollector[SimpleBigQueryViewBuilder]):
    def collect_view_builders(self) -> List[SimpleBigQueryViewBuilder]:
        builders = []
        for entity, table_name in get_state_tables():
            if table_name in METADATA_TABLES_WITH_CUSTOM_COUNTERS:
                continue
            has_placeholders = 'external_id' in entity.get_column_property_names()
            table_column_checker = BigQueryTableChecker('state', table_name)
            for col in get_enum_property_names(entity):
                if col in METADATA_EXCLUDED_PROPERTIES:
                    continue
                template = ENUM_COUNTER_WITH_PLACEHOLDERS_STATE_QUERY_TEMPLATE if has_placeholders \
                    else ENUM_COUNTER_STATE_QUERY_TEMPLATE
                builders.append(
                    SimpleBigQueryViewBuilder(
                        dataset_id=VIEWS_DATASET,
                        view_id=f'ingest_state_metadata__{table_name}__{col}',
                        view_query_template=template,
                        table_name=table_name,
                        column_name=col,
                        should_build_predicate=table_column_checker.get_has_column_predicate(col),
                    )
                )
        return builders


if __name__ == '__main__':
    with local_project_id_override(GCP_PROJECT_STAGING):
        collector = StateTableEnumCounterBigQueryViewCollector()
        for builder in collector.collect_view_builders():
            builder.build_and_print()
