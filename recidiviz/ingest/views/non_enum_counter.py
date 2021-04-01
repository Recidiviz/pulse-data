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
"""
A view collector that generates BigQuery views for non-enum columns and counts
the number of NULL vs non-NULL values.

Its results are split by state and by whether it is a placeholder object.

NB: In the future, this functionality should be separated out based on the semantics
of the column. E.g. date counters should count non-nulls but also report min and
max dates in their ranges. Enums have already been split accordingly.
"""
from typing import List

from recidiviz.big_query.big_query_table_checker import BigQueryTableChecker
from recidiviz.big_query.big_query_view import SimpleBigQueryViewBuilder
from recidiviz.big_query.big_query_view_collector import BigQueryViewCollector
from recidiviz.ingest.views.dataset_config import VIEWS_DATASET
from recidiviz.ingest.views.metadata_helpers import (
    METADATA_EXCLUDED_PROPERTIES,
    METADATA_TABLES_WITH_CUSTOM_COUNTERS,
    get_non_enum_property_names,
    get_state_tables,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

NON_ENUM_COUNTER_STATE_QUERY_TEMPLATE = """
WITH table_rows AS (
  SELECT
    state_code,
    IF({column_name} IS NULL, 'NULL', 'NOT_NULL') AS {column_name}
  FROM
    `{project_id}.state.{table_name}`
)
SELECT
  state_code,
  {column_name},
  COUNT(*) AS total_count,
  0 AS placeholder_count
FROM table_rows
GROUP BY state_code, {column_name}
ORDER BY state_code;
"""

NON_ENUM_COUNTER_WITH_PLACEHOLDERS_STATE_QUERY_TEMPLATE = """
WITH table_rows AS (
  SELECT
    state_code,
    external_id,
    IF({column_name} IS NULL, 'NULL', 'NOT_NULL') AS {column_name}
  FROM
    `{project_id}.state.{table_name}`
)
SELECT
  state_code,
  {column_name},
  COUNT(*) AS total_count,
  IFNULL(SUM(CASE WHEN external_id IS NULL THEN 1 END), 0) AS placeholder_count
FROM table_rows
GROUP BY state_code, {column_name}
ORDER BY state_code;
"""

STATE_TABLE_NON_ENUM_COLUMN_DESCRIPTION_TEMPLATE = """View for non-enum column: [{col}]
 that also counts the number of NULL vs non-NULL values. Its results are split by state
 and by whether it is a placeholder object."""


class StateTableNonEnumCounterBigQueryViewCollector(
    BigQueryViewCollector[SimpleBigQueryViewBuilder]
):
    """A BigQueryViewCollector that prepares view builders to produce metadata about
    non-enum columns on tables in the state schema."""

    def collect_view_builders(self) -> List[SimpleBigQueryViewBuilder]:
        builders = []
        for entity, table_name in get_state_tables():
            if table_name in METADATA_TABLES_WITH_CUSTOM_COUNTERS:
                continue
            has_placeholders = "external_id" in entity.get_column_property_names()
            table_column_checker = BigQueryTableChecker("state", table_name)
            for col in get_non_enum_property_names(entity):
                if col in METADATA_EXCLUDED_PROPERTIES:
                    continue
                template = (
                    NON_ENUM_COUNTER_WITH_PLACEHOLDERS_STATE_QUERY_TEMPLATE
                    if has_placeholders
                    else NON_ENUM_COUNTER_STATE_QUERY_TEMPLATE
                )

                builders.append(
                    SimpleBigQueryViewBuilder(
                        dataset_id=VIEWS_DATASET,
                        view_id=f"ingest_state_metadata__{table_name}__{col}",
                        description=STATE_TABLE_NON_ENUM_COLUMN_DESCRIPTION_TEMPLATE.format(
                            col=col
                        ),
                        view_query_template=template,
                        table_name=table_name,
                        column_name=col,
                        should_build_predicate=table_column_checker.get_has_column_predicate(
                            col
                        ),
                    )
                )
        return builders


if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        collector = StateTableNonEnumCounterBigQueryViewCollector()
        for builder in collector.collect_view_builders():
            builder.build_and_print()
