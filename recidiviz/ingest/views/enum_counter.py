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
# TODO(#26022): Remove placeholder references in admin panel state table views
from typing import List

from recidiviz.big_query.big_query_view import SimpleBigQueryViewBuilder
from recidiviz.big_query.big_query_view_collector import BigQueryViewCollector
from recidiviz.ingest.views.dataset_config import (
    NORMALIZED_STATE_DATASET,
    VIEWS_DATASET,
)
from recidiviz.ingest.views.metadata_helpers import (
    METADATA_EXCLUDED_PROPERTIES,
    METADATA_TABLES_WITH_CUSTOM_COUNTERS,
    get_enum_property_names,
    get_state_tables,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override
from recidiviz.utils.string import StrictStringFormatter

ENUM_COUNTER_STATE_QUERY_TEMPLATE = """
SELECT
  state_code,
  IF({column_name} IS NULL, 'NULL', {column_name}) AS {column_name},
  COUNT(*) AS total_count,
  0 AS placeholder_count
FROM
  `{project_id}.{base_dataset}.{table_name}`
GROUP BY state_code, `{column_name}`
ORDER BY state_code, `{column_name}`
"""

ENUM_COUNTER_WITH_PLACEHOLDERS_STATE_QUERY_TEMPLATE = """
SELECT
  state_code,
  IF({column_name} IS NULL, 'NULL', {column_name}) AS {column_name},
  COUNT(*) AS total_count,
  IFNULL(SUM(CASE WHEN external_id IS NULL THEN 1 END), 0) AS placeholder_count
FROM
  `{project_id}.{base_dataset}.{table_name}`
GROUP BY state_code, `{column_name}`
ORDER BY state_code, `{column_name}`
"""

STATE_TABLE_ENUM_COLUMN_DESCRIPTION_TEMPLATE = """View that counts the instances of
 different enum values for column: [{col}] in table: [{table_name}]. Its results are
  split by state and by whether it is a placeholder object."""


class StateTableEnumCounterBigQueryViewCollector(
    BigQueryViewCollector[SimpleBigQueryViewBuilder]
):
    """A BigQueryViewCollector that prepares views builders to produce metadata about
    enums on tables in the state schema."""

    def collect_view_builders(self) -> List[SimpleBigQueryViewBuilder]:
        builders = []
        for entity, table_name in get_state_tables():
            if table_name in METADATA_TABLES_WITH_CUSTOM_COUNTERS:
                continue
            has_placeholders = "external_id" in entity.get_column_property_names()
            for col in get_enum_property_names(entity):
                if col in METADATA_EXCLUDED_PROPERTIES:
                    continue
                template = (
                    ENUM_COUNTER_WITH_PLACEHOLDERS_STATE_QUERY_TEMPLATE
                    if has_placeholders
                    else ENUM_COUNTER_STATE_QUERY_TEMPLATE
                )
                builders.append(
                    SimpleBigQueryViewBuilder(
                        dataset_id=VIEWS_DATASET,
                        view_id=f"ingest_state_metadata__{table_name}__{col}",
                        description=StrictStringFormatter().format(
                            STATE_TABLE_ENUM_COLUMN_DESCRIPTION_TEMPLATE,
                            col=col,
                            table_name=table_name,
                        ),
                        view_query_template=template,
                        table_name=table_name,
                        column_name=col,
                        base_dataset=NORMALIZED_STATE_DATASET,
                    )
                )
        return builders


if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        collector = StateTableEnumCounterBigQueryViewCollector()
        for builder in collector.collect_view_builders():
            builder.build_and_print()
