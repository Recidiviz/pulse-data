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

See recidiviz.tools.calculator.create_or_update_dataflow_sandbox.py for running this
locally to create sandbox Dataflow datasets.
"""
import argparse
import logging
import sys
from typing import List, Tuple

import attr

from recidiviz.big_query.big_query_client import BigQueryClientImpl
from recidiviz.calculator import dataflow_config
from recidiviz.calculator.dataflow_orchestration_utils import (
    get_metric_pipeline_enabled_states,
)
from recidiviz.calculator.pipeline.normalization.utils.normalized_entities_utils import (
    NORMALIZED_ENTITY_CLASSES,
)
from recidiviz.calculator.pipeline.normalization.utils.normalized_entity_conversion_utils import (
    bq_schema_for_normalized_state_entity,
)
from recidiviz.calculator.pipeline.supplemental.base_supplemental_dataset_pipeline import (
    SupplementalDatasetPipelineRunDelegate,
)
from recidiviz.calculator.pipeline.supplemental.dataset_config import (
    SUPPLEMENTAL_DATA_DATASET,
)
from recidiviz.calculator.pipeline.utils.pipeline_run_delegate_utils import (
    collect_all_pipeline_run_delegate_classes,
)
from recidiviz.calculator.query.state import dataset_config
from recidiviz.calculator.query.state.dataset_config import (
    DATAFLOW_METRICS_DATASET,
    normalized_state_dataset_for_state_code,
)
from recidiviz.persistence.database import schema_utils
from recidiviz.persistence.database.bq_refresh.big_query_table_manager import (
    update_bq_schema_for_sqlalchemy_table,
)
from recidiviz.persistence.database.bq_refresh.cloud_sql_to_bq_refresh_config import (
    CloudSqlToBQConfig,
)
from recidiviz.persistence.database.schema_utils import SchemaType
from recidiviz.utils.environment import GCP_PROJECT_PRODUCTION, GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override


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


def update_normalized_table_schemas_in_dataset(
    normalized_state_dataset_id: str,
) -> List[str]:
    """For each table in the dataset, ensures that all expected attributes on the
    corresponding normalized state entity are present in the table in BigQuery.

    Returns the list of table_id values that were updated.
    """
    bq_client = BigQueryClientImpl()
    normalized_state_dataset_ref = bq_client.dataset_ref_for_id(
        normalized_state_dataset_id
    )

    bq_client.create_dataset_if_necessary(
        normalized_state_dataset_ref,
    )

    normalized_table_ids: List[str] = []

    for entity_cls in NORMALIZED_ENTITY_CLASSES:
        schema_for_entity_class = bq_schema_for_normalized_state_entity(entity_cls)
        # We store normalized entities in tables with the same names as the tables of
        # their underlying base entity classes.
        table_id = schema_utils.get_state_database_entity_with_name(
            entity_cls.base_class_name()
        ).__tablename__

        normalized_table_ids.append(table_id)

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

    return normalized_table_ids


def update_state_specific_normalized_state_schemas() -> None:
    """Updates the tables for each state-specific dataset that stores Dataflow
    normalized state entity output to match expected schemas."""
    for state_code in get_metric_pipeline_enabled_states():
        normalized_state_dataset_id = normalized_state_dataset_for_state_code(
            state_code
        )

        update_normalized_table_schemas_in_dataset(normalized_state_dataset_id)


def update_normalized_state_schema() -> None:
    """Updates each table in the normalized_state dataset to match expected schemas."""
    bq_client = BigQueryClientImpl()

    normalized_state_dataset_id = dataset_config.NORMALIZED_STATE_DATASET
    normalized_state_dataset_ref = bq_client.dataset_ref_for_id(
        normalized_state_dataset_id
    )

    bq_client.create_dataset_if_necessary(normalized_state_dataset_ref)

    # Update the tables in the normalized_state dataset for all entities that are
    # normalized, and get a list back of all tables updated
    normalized_table_ids = update_normalized_table_schemas_in_dataset(
        normalized_state_dataset_id
    )

    export_config = CloudSqlToBQConfig.for_schema_type(SchemaType.STATE)

    for table in export_config.get_tables_to_export():
        table_id = table.name

        if table_id not in normalized_table_ids:
            # Update the schema of the non-normalized entity to have all columns
            # expected for the table
            update_bq_schema_for_sqlalchemy_table(
                bq_client=bq_client,
                schema_type=SchemaType.STATE,
                dataset_id=normalized_state_dataset_id,
                table=table,
            )


def update_supplemental_dataset_schemas(
    supplemental_metrics_dataset_id: str = SUPPLEMENTAL_DATA_DATASET,
) -> None:
    """For each table in the supplemental data dataset, ensures that all attributes on
    the corresponding supplemental dataset are present in the table in BigQuery."""
    bq_client = BigQueryClientImpl()

    supplemental_metrics_dataset_ref = bq_client.dataset_ref_for_id(
        supplemental_metrics_dataset_id
    )

    bq_client.create_dataset_if_necessary(supplemental_metrics_dataset_ref)
    supplemental_data_pipeline_delegates = [
        pipeline_delegate
        for pipeline_delegate in collect_all_pipeline_run_delegate_classes()
        if issubclass(pipeline_delegate, SupplementalDatasetPipelineRunDelegate)
    ]

    for delegate in supplemental_data_pipeline_delegates:
        schema_for_supplemental_dataset = delegate.bq_schema_for_table()
        table_id = delegate.table_id()
        if bq_client.table_exists(supplemental_metrics_dataset_ref, table_id):
            bq_client.update_schema(
                supplemental_metrics_dataset_id,
                table_id,
                schema_for_supplemental_dataset,
            )
        else:
            bq_client.create_table_with_schema(
                supplemental_metrics_dataset_id,
                table_id,
                schema_for_supplemental_dataset,
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
        update_supplemental_dataset_schemas()
        update_state_specific_normalized_state_schemas()
        update_normalized_state_schema()
