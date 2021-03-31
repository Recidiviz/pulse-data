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
different enum values as well as NULL vs non-NULL values for the state_person table.

Its results are split by state and by whether it is a placeholder object.
"""
from typing import List

from recidiviz.big_query.big_query_view import SimpleBigQueryViewBuilder
from recidiviz.big_query.big_query_view_collector import BigQueryViewCollector
from recidiviz.ingest.views.dataset_config import VIEWS_DATASET
from recidiviz.ingest.views.metadata_helpers import (
    METADATA_EXCLUDED_PROPERTIES,
    BigQueryTableChecker,
    get_enum_property_names,
    get_non_enum_property_names,
)
from recidiviz.persistence.database.schema_utils import (
    get_state_database_entity_with_name,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

STATE_PERSON_ENUM_QUERY_TEMPLATE = """
WITH person_ids AS (
  SELECT
    DISTINCT person_id AS matching_id
  FROM
    `{project_id}.state.state_person_external_id`
)
SELECT
  state_code,
  COALESCE({column_name}, 'NULL') AS {column_name},
  COUNT(*) AS total_count,
  IFNULL(SUM(CASE WHEN matching_id IS NULL THEN 1 END), 0) AS placeholder_count
FROM
  `{project_id}.state.state_person`
LEFT OUTER JOIN person_ids ON
  `{project_id}.state.state_person`.person_id = person_ids.matching_id
GROUP BY state_code, `{column_name}`
ORDER BY state_code, `{column_name}`;
"""

STATE_PERSON_NON_ENUM_QUERY_TEMPLATE = """
WITH table_rows AS (
  SELECT
    `{project_id}.state.state_person`.state_code,
    IF(`{project_id}.state.state_person`.{column_name} IS NULL, 'NULL', 'NOT_NULL') AS {column_name},
    `{project_id}.state.state_person_external_id` AS matching_id
  FROM
    `{project_id}.state.state_person`
  LEFT OUTER JOIN
    `{project_id}.state.state_person_external_id`
  ON `{project_id}.state.state_person`.person_id = `{project_id}.state.state_person_external_id`.person_id
)
SELECT
  state_code,
  {column_name},
  COUNT(*) AS total_count,
  IFNULL(SUM(CASE WHEN matching_id IS NULL THEN 1 END), 0) AS placeholder_count
FROM
  table_rows
GROUP BY state_code, `{column_name}`
ORDER BY state_code, `{column_name}`;
"""

STATE_PERSON_ENTITY_NAME = "StatePerson"
STATE_PERSON_TABLE_NAME = "state_person"

STATE_PERSON_ENUM_COLUMN_DESCRIPTION_TEMPLATE = """View that counts the instances of
 different enum values for column: [{col}], as well as as well as NULL vs non-NULL values
 for the state_person table"""

STATE_PERSON_NON_ENUM_COLUMN_DESCRIPTION_TEMPLATE = """View that counts the instances of
 different values for column: [{col}], as well as as well as NULL vs non-NULL values
 for the state_person table"""


class StatePersonBigQueryViewCollector(
    BigQueryViewCollector[SimpleBigQueryViewBuilder]
):
    """A BigQueryViewCollector that prepares view builders to produce metadata about
    enums on the StatePerson* tables."""

    def collect_view_builders(self) -> List[SimpleBigQueryViewBuilder]:
        table_column_checker = BigQueryTableChecker("state", STATE_PERSON_TABLE_NAME)
        entity = get_state_database_entity_with_name(STATE_PERSON_ENTITY_NAME)
        builders = [
            SimpleBigQueryViewBuilder(
                dataset_id=VIEWS_DATASET,
                view_id=f"ingest_state_metadata__{STATE_PERSON_TABLE_NAME}__{col}",
                description=STATE_PERSON_ENUM_COLUMN_DESCRIPTION_TEMPLATE.format(
                    col=col
                ),
                view_query_template=STATE_PERSON_ENUM_QUERY_TEMPLATE,
                table_name=STATE_PERSON_TABLE_NAME,
                column_name=col,
                should_build_predicate=table_column_checker.get_has_column_predicate(
                    col
                ),
            )
            for col in get_enum_property_names(entity)
            if col not in METADATA_EXCLUDED_PROPERTIES
        ]
        builders.extend(
            [
                SimpleBigQueryViewBuilder(
                    dataset_id=VIEWS_DATASET,
                    view_id=f"ingest_state_metadata__{STATE_PERSON_TABLE_NAME}__{col}",
                    description=STATE_PERSON_NON_ENUM_COLUMN_DESCRIPTION_TEMPLATE.format(
                        col=col
                    ),
                    view_query_template=STATE_PERSON_NON_ENUM_QUERY_TEMPLATE,
                    table_name=STATE_PERSON_TABLE_NAME,
                    column_name=col,
                    should_build_predicate=table_column_checker.get_has_column_predicate(
                        col
                    ),
                )
                for col in get_non_enum_property_names(entity)
                if col not in METADATA_EXCLUDED_PROPERTIES
            ]
        )

        return builders


if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        collector = StatePersonBigQueryViewCollector()
        for builder in collector.collect_view_builders():
            builder.build_and_print()
