# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2022 Recidiviz, Inc.
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
"""Utils for writing validations for the normalization pipelines."""
from typing import List

from recidiviz.calculator.pipeline.normalization.utils.normalized_entities_utils import (
    NORMALIZED_ENTITY_CLASSES,
)
from recidiviz.calculator.query.state import dataset_config
from recidiviz.persistence.database import schema_utils
from recidiviz.persistence.entity import entity_utils
from recidiviz.persistence.entity.state import entities as state_entities
from recidiviz.utils.string import StrictStringFormatter

UNIQUE_IDS_TEMPLATE = """
SELECT
    state_code as region_code,
    '{table_id}' as entity_name,
    COUNT(*) as total_count,
    COUNT(DISTINCT({id_column})) as distinct_id_count
FROM
    `{{project_id}}.{dataset_id}.{table_id}`
GROUP BY 1
"""


def unique_entity_id_values_query() -> str:
    """Builds a query to identify when entity normalization pipelines are producing
    entities with duplicate ID values."""
    entity_sub_queries: List[str] = []

    dataset = dataset_config.NORMALIZED_STATE_DATASET

    for entity_cls in NORMALIZED_ENTITY_CLASSES:
        base_class_name = entity_cls.base_class_name()
        base_schema_class = schema_utils.get_state_database_entity_with_name(
            base_class_name
        )
        base_entity_class = entity_utils.get_entity_class_in_module_with_name(
            entities_module=state_entities, class_name=base_class_name
        )
        table_id = base_schema_class.__tablename__
        id_column = base_entity_class.get_class_id_name()

        entity_sub_queries.append(
            StrictStringFormatter().format(
                UNIQUE_IDS_TEMPLATE,
                table_id=table_id,
                dataset_id=dataset,
                id_column=id_column,
            )
        )

    return "\nUNION ALL\n".join(entity_sub_queries)
