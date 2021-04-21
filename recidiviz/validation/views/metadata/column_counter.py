# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2021 Recidiviz, Inc.
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
A view collector that generates BigQuery views for columns among our validation tables
and counts the number of NULL vs non-NULL values.

Its results are split by state.

NB: In the future, this functionality should be separated out based on the semantics
of the column. E.g. date counters should count non-nulls but also report min and
max dates in their ranges.
"""

from typing import List

from recidiviz.big_query.big_query_table_checker import BigQueryTableChecker
from recidiviz.big_query.big_query_view import SimpleBigQueryViewBuilder
from recidiviz.big_query.big_query_view_collector import BigQueryViewCollector
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override
from recidiviz.validation.views.dataset_config import METADATA_DATASET
from recidiviz.validation.views.metadata.validation_schema_config import (
    get_external_validation_schema,
    DatasetSchemaInfo,
)


COLUMN_COUNTER_STATE_QUERY_TEMPLATE = """
WITH table_rows AS (
  SELECT
    region_code as state_code,
    IF({column_name} IS NULL, 'NULL', 'NOT_NULL') AS {column_name}
  FROM
    `{project_id}.{validation_dataset}.{table_name}`
)
SELECT
  state_code,
  {column_name},
  COUNT(*) AS total_count
FROM table_rows
GROUP BY state_code, {column_name}
ORDER BY state_code;
"""


METADATA_EXCLUDED_PROPERTIES = ["state_code", "region_code"]


class ValidationTableColumnCounterBigQueryViewCollector(
    BigQueryViewCollector[SimpleBigQueryViewBuilder]
):
    """A BigQueryViewCollector that prepares view builders to produce metadata about
    non-enum columns on tables in the state schema."""

    def __init__(self, schema_config: DatasetSchemaInfo) -> None:
        self.schema_config = schema_config

    def collect_view_builders(self) -> List[SimpleBigQueryViewBuilder]:
        builders: List[SimpleBigQueryViewBuilder] = []
        for table in self.schema_config.tables:
            table_name = table.table_name

            table_column_checker = BigQueryTableChecker(
                self.schema_config.dataset, table_name
            )
            for col in table.columns:
                if col in METADATA_EXCLUDED_PROPERTIES:
                    continue
                builders.append(
                    SimpleBigQueryViewBuilder(
                        dataset_id=METADATA_DATASET,
                        view_id=f"validation_metadata__{table_name}__{col}",
                        description=f"Validation metadata for {table_name}",
                        view_query_template=COLUMN_COUNTER_STATE_QUERY_TEMPLATE,
                        validation_dataset=self.schema_config.dataset,
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
        collector = ValidationTableColumnCounterBigQueryViewCollector(
            schema_config=get_external_validation_schema()
        )
        for builder in collector.collect_view_builders():
            builder.build_and_print()
