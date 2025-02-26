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
import datetime
from concurrent import futures
from typing import Dict, List, Optional

import attr
from google.cloud import bigquery

from recidiviz.big_query.address_overrides import BigQueryAddressOverrides
from recidiviz.big_query.big_query_client import (
    BQ_CLIENT_MAX_POOL_SIZE,
    BigQueryClientImpl,
)
from recidiviz.big_query.big_query_utils import schema_for_sqlalchemy_table
from recidiviz.big_query.view_update_manager import (
    TEMP_DATASET_DEFAULT_TABLE_EXPIRATION_MS,
)
from recidiviz.calculator.query.state import dataset_config
from recidiviz.calculator.query.state.dataset_config import (
    DATAFLOW_METRICS_DATASET,
    normalized_state_dataset_for_state_code,
    state_dataset_for_state_code,
)
from recidiviz.common.constants.states import StateCode
from recidiviz.ingest.direct.controllers.direct_ingest_controller_factory import (
    DirectIngestControllerFactory,
)
from recidiviz.ingest.direct.dataset_config import (
    ingest_view_materialization_results_dataflow_dataset,
)
from recidiviz.ingest.direct.types.direct_ingest_instance import DirectIngestInstance
from recidiviz.ingest.direct.views.direct_ingest_view_query_builder import (
    DirectIngestViewQueryBuilder,
)
from recidiviz.persistence.database import schema_utils
from recidiviz.persistence.database.bq_refresh.big_query_table_manager import (
    update_bq_dataset_to_match_sqlalchemy_schema,
    update_bq_schema_for_sqlalchemy_table,
)
from recidiviz.persistence.database.bq_refresh.cloud_sql_to_bq_refresh_config import (
    CloudSqlToBQConfig,
)
from recidiviz.persistence.database.schema_type import SchemaType
from recidiviz.persistence.entity.normalized_entities_utils import (
    NORMALIZED_ENTITY_CLASSES,
)
from recidiviz.pipelines import dataflow_config
from recidiviz.pipelines.dataflow_orchestration_utils import (
    get_ingest_pipeline_enabled_states,
    get_normalization_pipeline_enabled_states,
)
from recidiviz.pipelines.ingest.state.generate_ingest_view_results import (
    ADDITIONAL_SCHEMA_COLUMNS,
)
from recidiviz.pipelines.normalization.utils.entity_normalization_manager_utils import (
    NORMALIZATION_MANAGERS,
)
from recidiviz.pipelines.normalization.utils.normalized_entity_conversion_utils import (
    bq_schema_for_normalized_state_entity,
)
from recidiviz.pipelines.supplemental.base_supplemental_dataset_pipeline import (
    SupplementalDatasetPipeline,
)
from recidiviz.pipelines.supplemental.dataset_config import SUPPLEMENTAL_DATA_DATASET
from recidiviz.pipelines.utils.pipeline_run_utils import collect_all_pipeline_classes


def update_dataflow_metric_tables_schemas(
    dataflow_metrics_dataset_id: str = DATAFLOW_METRICS_DATASET,
    sandbox_dataset_prefix: Optional[str] = None,
) -> None:
    """For each table that stores Dataflow metric output in the
    |dataflow_metrics_dataset_id|, ensures that all attributes on the corresponding
    metric are present in the table in BigQuery. If no |dataflow_metrics_dataset_id| is
    provided, defaults to the DATAFLOW_METRICS_DATASET. If a |sandbox_dataset_prefix| is
    provided, a temporary dataset will be created with the prefix."""
    bq_client = BigQueryClientImpl()

    if sandbox_dataset_prefix:
        dataflow_metrics_dataset_id = BigQueryAddressOverrides.format_sandbox_dataset(
            sandbox_dataset_prefix, dataflow_metrics_dataset_id
        )

    dataflow_metrics_dataset_ref = bq_client.dataset_ref_for_id(
        dataflow_metrics_dataset_id
    )

    bq_client.create_dataset_if_necessary(
        dataflow_metrics_dataset_ref,
        TEMP_DATASET_DEFAULT_TABLE_EXPIRATION_MS if sandbox_dataset_prefix else None,
    )

    for metric_class, table_id in dataflow_config.DATAFLOW_METRICS_TO_TABLES.items():
        schema_for_metric_class = metric_class.bq_schema_for_metric_table()
        clustering_fields = None
        if all(
            cluster_field in attr.fields_dict(metric_class).keys()  # type: ignore[arg-type]
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
    default_table_expiration_ms: Optional[int],
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
        normalized_state_dataset_ref, default_table_expiration_ms
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

    for manager in NORMALIZATION_MANAGERS:
        for child_cls, parent_cls in manager.normalized_entity_associations():
            association_table = schema_utils.get_state_database_association_with_names(
                child_cls.__name__, parent_cls.__name__
            )

            schema_for_association_table = schema_for_sqlalchemy_table(
                association_table, add_state_code_field=True
            )

            table_id = association_table.name
            normalized_table_ids.append(table_id)

            if bq_client.table_exists(normalized_state_dataset_ref, table_id):
                bq_client.update_schema(
                    normalized_state_dataset_id, table_id, schema_for_association_table
                )
            else:
                bq_client.create_table_with_schema(
                    normalized_state_dataset_id, table_id, schema_for_association_table
                )

    return normalized_table_ids


def update_state_specific_normalized_state_schemas(
    sandbox_dataset_prefix: Optional[str] = None,
) -> None:
    """Updates the tables for each state-specific dataset that stores Dataflow
    normalized state entity output to match expected schemas. If |sandbox_dataset_prefix|
    is provided, a temporary dataset will be created with the prefix."""
    for state_code in get_normalization_pipeline_enabled_states():
        normalized_state_dataset_id = normalized_state_dataset_for_state_code(
            state_code
        )

        if sandbox_dataset_prefix:
            normalized_state_dataset_id = (
                BigQueryAddressOverrides.format_sandbox_dataset(
                    sandbox_dataset_prefix, normalized_state_dataset_id
                )
            )

        update_normalized_table_schemas_in_dataset(
            normalized_state_dataset_id,
            TEMP_DATASET_DEFAULT_TABLE_EXPIRATION_MS
            if sandbox_dataset_prefix
            else None,
        )


def update_normalized_state_schema(
    sandbox_dataset_prefix: Optional[str] = None,
) -> None:
    """Updates each table in the normalized_state dataset to match expected schemas. If
    |sandbox_dataset_prefix| is provided, a temporary dataset will be created with the
    prefix."""
    bq_client = BigQueryClientImpl()

    normalized_state_dataset_id = dataset_config.NORMALIZED_STATE_DATASET

    if sandbox_dataset_prefix:
        normalized_state_dataset_id = BigQueryAddressOverrides.format_sandbox_dataset(
            sandbox_dataset_prefix, normalized_state_dataset_id
        )

    normalized_state_dataset_ref = bq_client.dataset_ref_for_id(
        normalized_state_dataset_id
    )

    bq_client.create_dataset_if_necessary(
        normalized_state_dataset_ref,
        TEMP_DATASET_DEFAULT_TABLE_EXPIRATION_MS if sandbox_dataset_prefix else None,
    )

    # Update the tables in the normalized_state dataset for all entities that are
    # normalized, and get a list back of all tables updated
    normalized_table_ids = update_normalized_table_schemas_in_dataset(
        normalized_state_dataset_id,
        TEMP_DATASET_DEFAULT_TABLE_EXPIRATION_MS if sandbox_dataset_prefix else None,
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
    sandbox_dataset_prefix: Optional[str] = None,
) -> None:
    """For each table in the supplemental data dataset, ensures that all attributes on
    the corresponding supplemental dataset are present in the table in BigQuery.
    If |sandbox_dataset_prefix| is provided, a temporary dataset will be created with
    the prefix."""
    bq_client = BigQueryClientImpl()

    if sandbox_dataset_prefix:
        supplemental_metrics_dataset_id = (
            BigQueryAddressOverrides.format_sandbox_dataset(
                sandbox_dataset_prefix, supplemental_metrics_dataset_id
            )
        )

    supplemental_metrics_dataset_ref = bq_client.dataset_ref_for_id(
        supplemental_metrics_dataset_id
    )

    bq_client.create_dataset_if_necessary(
        supplemental_metrics_dataset_ref,
        TEMP_DATASET_DEFAULT_TABLE_EXPIRATION_MS if sandbox_dataset_prefix else None,
    )
    supplemental_data_pipelines = [
        pipeline
        for pipeline in collect_all_pipeline_classes()
        if issubclass(pipeline, SupplementalDatasetPipeline)
    ]

    for pipeline in supplemental_data_pipelines:
        schema_for_supplemental_dataset = pipeline.bq_schema_for_table()
        table_id = pipeline.table_id()
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


def update_state_specific_ingest_state_schemas(
    sandbox_dataset_prefix: Optional[str] = None,
) -> None:
    """Updates the tables for each state-specific dataset that stores Dataflow
    state entity output to match expected schemas."""
    for state_code in get_ingest_pipeline_enabled_states():
        for ingest_instance in DirectIngestInstance:
            update_bq_dataset_to_match_sqlalchemy_schema(
                schema_type=SchemaType.STATE,
                dataset_id=state_dataset_for_state_code(
                    state_code, ingest_instance, sandbox_dataset_prefix
                ),
                default_table_expiration_ms=TEMP_DATASET_DEFAULT_TABLE_EXPIRATION_MS
                if sandbox_dataset_prefix
                else None,
            )


def update_state_specific_ingest_view_result_schema(
    ingest_view_dataset_id: str,
    state_code: StateCode,
    ingest_instance: DirectIngestInstance,
    default_table_expiration_ms: Optional[int] = None,
) -> None:
    """Updates each table in the ingest view dataset to match expected schemas."""
    bq_client = BigQueryClientImpl()

    ingest_view_dataset_ref = bq_client.dataset_ref_for_id(ingest_view_dataset_id)
    bq_client.create_dataset_if_necessary(
        ingest_view_dataset_ref, default_table_expiration_ms
    )

    controller = DirectIngestControllerFactory.build(
        region_code=state_code.value,
        ingest_instance=ingest_instance,
        allow_unlaunched=True,
    )

    ingest_view_builders = controller.view_collector.collect_query_builders()
    ingest_view_name_to_query_job: Dict[str, bigquery.QueryJob] = {}
    for ingest_view_builder in ingest_view_builders:
        ingest_view_name_to_query_job[
            ingest_view_builder.ingest_view_name
        ] = bq_client.run_query_async(
            query_str=ingest_view_builder.build_query(
                config=DirectIngestViewQueryBuilder.QueryStructureConfig(
                    raw_data_source_instance=ingest_instance,
                    raw_data_datetime_upper_bound=datetime.datetime.now(),
                    limit_zero=True,
                )
            ),
            use_query_cache=False,
        )

    with futures.ThreadPoolExecutor(
        max_workers=int(BQ_CLIENT_MAX_POOL_SIZE / 2)
    ) as executor:
        futures_to_ingest_view_name = {
            executor.submit(job.result): ingest_view_name
            for ingest_view_name, job in ingest_view_name_to_query_job.items()
        }
        for f in futures.as_completed(futures_to_ingest_view_name):
            ingest_view_name = futures_to_ingest_view_name[f]
            res = f.result()
            final_schema = res.schema + ADDITIONAL_SCHEMA_COLUMNS
            if bq_client.table_exists(ingest_view_dataset_ref, ingest_view_name):
                bq_client.update_schema(
                    dataset_id=ingest_view_dataset_ref.dataset_id,
                    table_id=ingest_view_name,
                    desired_schema_fields=final_schema,
                )
            else:
                bq_client.create_table_with_schema(
                    dataset_id=ingest_view_dataset_ref.dataset_id,
                    table_id=ingest_view_name,
                    schema_fields=final_schema,
                )


def update_state_specific_ingest_view_results_schemas(
    sandbox_dataset_prefix: Optional[str] = None,
) -> None:
    """Updates the datasets for ingest view results to match expected schemas."""
    for state_code in get_ingest_pipeline_enabled_states():
        for ingest_instance in DirectIngestInstance:
            update_state_specific_ingest_view_result_schema(
                ingest_view_materialization_results_dataflow_dataset(
                    state_code, ingest_instance, sandbox_dataset_prefix
                ),
                state_code,
                ingest_instance,
                TEMP_DATASET_DEFAULT_TABLE_EXPIRATION_MS
                if sandbox_dataset_prefix
                else None,
            )
