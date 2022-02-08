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
"""Manages the structure of tables that store Dataflow output (metrics and normalized
state entities).

See recidiviz.tools.calculator.create_or_update_dataflow_metrics_sandbox.py for running
this locally to create a sandbox Dataflow metrics dataset.

See recidiviz.tools.calculator.create_or_update_normalized_state_sandbox for running
this locally to create sandbox normalized state datasets.
"""
import argparse
import logging
import sys
from typing import List, Optional, Set, Tuple

import attr

from recidiviz.big_query.big_query_client import BigQueryClientImpl
from recidiviz.calculator import dataflow_config
from recidiviz.calculator.pipeline.utils.entity_normalization.normalized_entities_utils import (
    NORMALIZED_ENTITY_CLASSES,
    bq_schema_for_normalized_state_entity,
)
from recidiviz.calculator.query.state.dataset_config import (
    DATAFLOW_METRICS_DATASET,
    normalized_state_dataset_for_state_code,
)
from recidiviz.common.constants.states import StateCode
from recidiviz.persistence.database import schema_utils
from recidiviz.utils.environment import GCP_PROJECT_PRODUCTION, GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override
from recidiviz.utils.yaml_dict import YAMLDict


def update_dataflow_metric_tables_schemas(
    dataflow_metrics_dataset_id: str = DATAFLOW_METRICS_DATASET,
) -> None:
    """For each table that stores Dataflow metric output in the
    |dataflow_metrics_dataset_id|, ensures that all attributes on the corresponding
    metric are present in the table in BigQuery. If no |dataflow_metrics_dataset_id| is
    provided, defaults to the DATAFLOW_METRICS_DATASET."""
    bq_client = BigQueryClientImpl()
    dataflow_metrics_dataset_ref = bq_client.dataset_ref_for_id(
        dataflow_metrics_dataset_id
    )

    bq_client.create_dataset_if_necessary(dataflow_metrics_dataset_ref)

    for metric_class, table_id in dataflow_config.DATAFLOW_METRICS_TO_TABLES.items():
        schema_for_metric_class = metric_class.bq_schema_for_metric_table()
        clustering_fields = None
        if all(
            cluster_field in attr.fields_dict(metric_class).keys()
            for cluster_field in dataflow_config.METRIC_CLUSTERING_FIELDS
        ):
            # Only apply clustering if the table has all of the metric clustering
            # fields
            clustering_fields = dataflow_config.METRIC_CLUSTERING_FIELDS

        if bq_client.table_exists(dataflow_metrics_dataset_ref, table_id):
            # Compare schema derived from metric class to existing dataflow views and
            # update if necessary.
            current_table = bq_client.get_table(dataflow_metrics_dataset_ref, table_id)
            if current_table.clustering_fields != clustering_fields:
                raise ValueError(
                    f"Existing table: {dataflow_metrics_dataset_id}.{table_id} "
                    f"has clustering fields {current_table.clustering_fields} that do "
                    f"not match {clustering_fields}"
                )

            bq_client.update_schema(
                dataflow_metrics_dataset_id,
                table_id,
                schema_for_metric_class,
            )
        else:
            bq_client.create_table_with_schema(
                dataflow_metrics_dataset_id,
                table_id,
                schema_for_metric_class,
                clustering_fields,
            )


def _get_pipeline_enabled_states() -> Set[StateCode]:
    """Returns all states that have scheduled pipelines that run."""
    pipeline_states: Set[StateCode] = set()

    pipeline_templates_yaml = YAMLDict.from_path(
        dataflow_config.PIPELINE_CONFIG_YAML_PATH
    )

    incremental_pipelines = pipeline_templates_yaml.pop_dicts("incremental_pipelines")
    historical_pipelines = pipeline_templates_yaml.pop_dicts("historical_pipelines")

    for pipeline in incremental_pipelines:
        pipeline_states.add(StateCode(pipeline.peek("state_code", str)))

    for pipeline in historical_pipelines:
        pipeline_states.add(StateCode(pipeline.peek("state_code", str)))

    return pipeline_states


def get_state_specific_normalized_state_dataset_for_state(
    state_code: StateCode,
    normalized_dataset_prefix: Optional[str] = None,
) -> str:
    """Returns the dataset_id for each normalized_state dataset that needs to
    exist in BigQuery; one for each pipeline-enabled state.

    All state-specific normalized_state datasets have the format:
        us_xx_normalized_state.

    Prefixes each dataset with the |normalized_dataset_prefix| if provided.
    """
    normalized_state_dataset_id = normalized_state_dataset_for_state_code(state_code)

    if normalized_dataset_prefix:
        normalized_state_dataset_id = (
            f"{normalized_dataset_prefix}_{normalized_state_dataset_id}"
        )

    return normalized_state_dataset_id


def update_normalized_state_schema(
    state_code: StateCode,
    normalized_dataset_prefix: Optional[str] = None,
) -> None:
    """For each table in each dataset that stores Dataflow normalized state entity
    output, ensures that all attributes on the corresponding normalized state entity
    are present in the table in BigQuery."""
    bq_client = BigQueryClientImpl()

    normalized_state_dataset_id = get_state_specific_normalized_state_dataset_for_state(
        state_code, normalized_dataset_prefix=normalized_dataset_prefix
    )

    normalized_state_dataset_ref = bq_client.dataset_ref_for_id(
        normalized_state_dataset_id
    )

    bq_client.create_dataset_if_necessary(
        normalized_state_dataset_ref,
    )

    for entity_cls in NORMALIZED_ENTITY_CLASSES:
        schema_for_entity_class = bq_schema_for_normalized_state_entity(entity_cls)
        # We store normalized entities in tables with the same names as the tables of
        # their underlying base entity classes.
        table_id = schema_utils.get_state_database_entity_with_name(
            entity_cls.base_class_name()
        ).__tablename__

        if bq_client.table_exists(normalized_state_dataset_ref, table_id):
            bq_client.update_schema(
                normalized_state_dataset_id,
                table_id,
                schema_for_entity_class,
            )
        else:
            bq_client.create_table_with_schema(
                normalized_state_dataset_id,
                table_id,
                schema_for_entity_class,
            )


def parse_arguments(argv: List[str]) -> Tuple[argparse.Namespace, List[str]]:
    """Parses the arguments needed to call the desired function."""
    parser = argparse.ArgumentParser()

    parser.add_argument(
        "--project_id",
        dest="project_id",
        type=str,
        choices=[GCP_PROJECT_STAGING, GCP_PROJECT_PRODUCTION],
        required=True,
    )

    return parser.parse_known_args(argv)


if __name__ == "__main__":
    logging.getLogger().setLevel(logging.INFO)
    known_args, _ = parse_arguments(sys.argv)

    with local_project_id_override(known_args.project_id):
        update_dataflow_metric_tables_schemas()
        for state in _get_pipeline_enabled_states():
            update_normalized_state_schema(state)
