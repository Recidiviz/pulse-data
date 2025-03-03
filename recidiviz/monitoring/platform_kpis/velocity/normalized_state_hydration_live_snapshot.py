# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2025 Recidiviz, Inc.
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
"""Logic for generating a live snapshot of hydration percentage for the normalized state
dataset.
"""

from typing import Type

from recidiviz.big_query.big_query_view import (
    BigQueryViewBuilder,
    SimpleBigQueryViewBuilder,
)
from recidiviz.common.attr_mixins import attribute_field_type_reference_for_class
from recidiviz.common.attr_utils import is_optional_type
from recidiviz.ingest.views.dataset_config import NORMALIZED_STATE_DATASET
from recidiviz.monitoring.platform_kpis.dataset_config import PLATFORM_KPIS_DATASET
from recidiviz.persistence.entity.base_entity import Entity
from recidiviz.persistence.entity.entities_bq_schema import (
    get_bq_schema_for_entity_table,
)
from recidiviz.persistence.entity.entities_module_context import EntitiesModuleContext
from recidiviz.persistence.entity.entities_module_context_factory import (
    entities_module_context_for_module,
)
from recidiviz.persistence.entity.entity_field_index import EntityFieldType
from recidiviz.persistence.entity.entity_utils import (
    NormalizedStateEntity,
    get_all_entity_classes_in_module,
)
from recidiviz.persistence.entity.state import normalized_entities
from recidiviz.persistence.entity.state.state_entity_mixins import SequencedEntityMixin
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override
from recidiviz.utils.string import StrictStringFormatter

NORMALIZED_STATE_HYDRATION_LIVE_SNAPSHOT_VIEW_ID = (
    "normalized_state_hydration_live_snapshot"
)
NORMALIZED_STATE_HYDRATION_LIVE_SNAPSHOT_DESCRIPTION = (
    "A live, point-in-time snapshot of hydration percentage of the normalized_state "
    "dataset by state_code. This view will only reflect the CURRENT state of the world "
    "as of the last view update. To see an archival version of this view, see "
    "`hydration_archive.normalized_state_hydration_archive`."
)

SINGLE_COLUMN_COUNT_TEMPLATE = """          STRUCT('{column_name}' AS column_name, COUNTIF({column_name} IS NOT NULL) AS hydrated_count)"""
COLUMN_HYDRATION_SCORE_TEMPLATE = """1 + (SELECT COUNTIF(col.column_name in ({column_hydration_to_count}) and col.hydrated_count > 0) FROM UNNEST(column_hydration_counts) as col)"""

# n.b. if you are changing this schema, be sure to also update the yaml-managed config
# for `normalized_state_hydration_archive` as a scheduled BQ job will read from this
# view and append to that table!
SINGLE_TABLE_TEMPLATE = """
SELECT
    state_code,
    table_name,
    entity_count,
    column_hydration_counts,
    -- counts 1 for all required fields, and one additional point for each optional flat field that is hydrated
    {column_hydration_score} as column_hydration_score
FROM (
    SELECT
        state_code,
        '{table_name}' as table_name,
        count(*) as entity_count,
        [
{column_counts}
        ] as column_hydration_counts,
    FROM `{{project_id}}.{{normalized_state_dataset_id}}.{table_name}`
    GROUP BY state_code, table_name
)
"""


def _should_count_field_hydration(
    *, entity_cls: type[NormalizedStateEntity], field: str, field_type: Type | None
) -> bool:
    """Returns whether or not the hydration of |field| should be counted as extra
    column points, on top of hydrating the entity itself. THe idea behind this function
    is each entity should get 1 point for hydrating all required fields (necessary for
    entity creation), and then an additional point for each optional field that is
    hydrated.
    """
    # sequence_num is marked as Optional on SequencedEntityMixin as it can be null
    # in state, but *should* always be non-null in normalized_state
    if issubclass(entity_cls, SequencedEntityMixin) and field == "sequence_num":
        return False

    return is_optional_type(field_type)


def _get_functionally_non_optional_fields_for_normalized_entity(
    *,
    entity_cls: type[Entity],
    module_ctx: EntitiesModuleContext,
) -> list[str]:
    if not issubclass(entity_cls, NormalizedStateEntity):
        raise ValueError(
            "Found entity that is not a subclass of NormalizedEntity in the normalized_entities module"
        )

    field_to_attribute_info = attribute_field_type_reference_for_class(
        entity_cls
    ).field_to_attribute_info

    column_hydration_to_count = [
        field
        for field in sorted(
            module_ctx.field_index().get_all_entity_fields(
                entity_cls=entity_cls, entity_field_type=EntityFieldType.FLAT_FIELD
            )
        )
        if _should_count_field_hydration(
            entity_cls=entity_cls,
            field=field,
            field_type=field_to_attribute_info[field].attribute.type,
        )
    ]

    return column_hydration_to_count


def _build_query_for_entity_cls(
    *, entity_cls: type[Entity], module_ctx: EntitiesModuleContext
) -> str:
    """Builds a query for a single normalized state entity hydration snapshot, to be
    union-ed together to form the dataset-wide view.
    """
    if not issubclass(entity_cls, NormalizedStateEntity):
        raise ValueError(
            "Found entity that is not a subclass of NormalizedEntity in the normalized_entities module"
        )

    column_counts = ",\n".join(
        StrictStringFormatter().format(
            SINGLE_COLUMN_COUNT_TEMPLATE,
            column_name=field.name,
        )
        for field in get_bq_schema_for_entity_table(
            normalized_entities, entity_cls.get_table_id()
        )
    )

    functionally_non_optional_fields = (
        _get_functionally_non_optional_fields_for_normalized_entity(
            entity_cls=entity_cls, module_ctx=module_ctx
        )
    )

    column_hydration_score_stmt = (
        StrictStringFormatter().format(
            COLUMN_HYDRATION_SCORE_TEMPLATE,
            column_hydration_to_count=", ".join(
                f"'{col}'" for col in functionally_non_optional_fields
            ),
        )
        if functionally_non_optional_fields
        else "1"
    )

    return StrictStringFormatter().format(
        SINGLE_TABLE_TEMPLATE,
        table_name=entity_cls.get_table_id(),
        column_counts=column_counts,
        column_hydration_score=column_hydration_score_stmt,
    )


def get_normalized_state_hydration_live_snapshot_view_builder() -> BigQueryViewBuilder:
    normalized_state_entity_classes = get_all_entity_classes_in_module(
        normalized_entities
    )
    module_ctx = entities_module_context_for_module(normalized_entities)

    view_query = "\nUNION ALL\n".join(
        _build_query_for_entity_cls(
            entity_cls=normalized_state_entity_cls, module_ctx=module_ctx
        )
        for normalized_state_entity_cls in sorted(
            normalized_state_entity_classes, key=lambda x: x.get_table_id()
        )
    )

    return SimpleBigQueryViewBuilder(
        view_query_template=view_query,
        dataset_id=PLATFORM_KPIS_DATASET,
        view_id=NORMALIZED_STATE_HYDRATION_LIVE_SNAPSHOT_VIEW_ID,
        description=NORMALIZED_STATE_HYDRATION_LIVE_SNAPSHOT_DESCRIPTION,
        normalized_state_dataset_id=NORMALIZED_STATE_DATASET,
        # this table is just a pass through for the bq schedule query, but is quite large.
        # since deploys can happen more than once per day, we shouldn't materialize this
        # view more to ensure that it only needs to run 1/day.
        should_materialize=False,
    )


if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        get_normalized_state_hydration_live_snapshot_view_builder().build_and_print()
