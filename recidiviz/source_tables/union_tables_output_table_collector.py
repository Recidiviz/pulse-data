# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2024 Recidiviz, Inc.
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
"""Helpers for building source table collections for the outputs of processes that union
together a set of state-specific datasets.
"""
from recidiviz.calculator.query.state.dataset_config import (
    NORMALIZED_STATE_DATASET,
    STATE_BASE_DATASET,
)
from recidiviz.persistence.entity.entities_bq_schema import (
    get_bq_schema_for_entities_module,
)
from recidiviz.persistence.entity.state import entities as state_entities
from recidiviz.persistence.entity.state import normalized_entities
from recidiviz.source_tables.source_table_config import (
    NormalizedStateAgnosticEntitySourceTableLabel,
    SourceTableCollection,
    UnionedStateAgnosticSourceTableLabel,
)


def build_unioned_state_source_table_collection() -> SourceTableCollection:
    """Builds the source table collections for the outputs of the update_state Airflow
    step.
    """
    state_agnostic_collection = SourceTableCollection(
        dataset_id=STATE_BASE_DATASET,
        labels=[
            UnionedStateAgnosticSourceTableLabel(STATE_BASE_DATASET),
        ],
    )
    for table_id, schema_fields in get_bq_schema_for_entities_module(
        state_entities
    ).items():
        state_agnostic_collection.add_source_table(
            table_id=table_id, schema_fields=schema_fields
        )

    return state_agnostic_collection


def build_unioned_normalized_state_source_table_collection() -> SourceTableCollection:
    """Builds the source table collections for the outputs of the
    update_normalized_state Airflow step.
    """
    state_agnostic_collection = SourceTableCollection(
        dataset_id=NORMALIZED_STATE_DATASET,
        labels=[
            UnionedStateAgnosticSourceTableLabel(NORMALIZED_STATE_DATASET),
            NormalizedStateAgnosticEntitySourceTableLabel(),
        ],
    )
    for table_id, schema_fields in get_bq_schema_for_entities_module(
        normalized_entities
    ).items():
        state_agnostic_collection.add_source_table(
            table_id=table_id, schema_fields=schema_fields
        )

    return state_agnostic_collection
