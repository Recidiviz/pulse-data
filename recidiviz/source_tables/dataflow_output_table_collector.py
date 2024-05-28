"""Contains utilities for collecting Dataflow output source tables"""

import attr

from recidiviz.airflow.dags.utils.dag_orchestration_utils import (
    get_ingest_pipeline_enabled_state_and_instance_pairs,
)
from recidiviz.big_query.big_query_utils import schema_for_sqlalchemy_table
from recidiviz.calculator.query.state.dataset_config import (
    DATAFLOW_METRICS_DATASET,
    NORMALIZED_STATE_DATASET,
)
from recidiviz.persistence.database import schema_utils
from recidiviz.persistence.database.bq_refresh.big_query_table_manager import (
    bq_schema_for_sqlalchemy_table,
)
from recidiviz.persistence.database.schema_type import SchemaType
from recidiviz.persistence.database.schema_utils import get_all_table_classes_in_schema
from recidiviz.persistence.entity.normalized_entities_utils import (
    NORMALIZED_ENTITY_CLASSES,
)
from recidiviz.pipelines import dataflow_config
from recidiviz.pipelines.ingest.dataset_config import state_dataset_for_state_code
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
from recidiviz.source_tables.source_table_config import (
    DataflowPipelineSourceTableLabel,
    SourceTableCollection,
)


def _build_normalized_state_entities() -> SourceTableCollection:
    """Builds the collection of normalization source tables"""
    normalized_state_entities = SourceTableCollection(
        labels=[DataflowPipelineSourceTableLabel("normalization")],
        dataset_id=NORMALIZED_STATE_DATASET,
    )

    for entity_cls in NORMALIZED_ENTITY_CLASSES:
        table_id = schema_utils.get_state_database_entity_with_name(
            entity_cls.base_class_name()
        ).__tablename__
        normalized_state_entities.add_source_table(
            table_id=table_id,
            schema_fields=bq_schema_for_normalized_state_entity(entity_cls),
        )

    for manager in NORMALIZATION_MANAGERS:
        for child_cls, parent_cls in manager.normalized_entity_associations():
            association_table = schema_utils.get_state_database_association_with_names(
                child_cls.__name__, parent_cls.__name__
            )

            normalized_state_entities.add_source_table(
                table_id=association_table.name,
                schema_fields=schema_for_sqlalchemy_table(
                    association_table,
                    add_state_code_field=True,
                ),
            )

    # Add definitions for non-normalized non-association state entities
    for table in get_all_table_classes_in_schema(SchemaType.STATE):
        if not normalized_state_entities.has_table(table.name):
            normalized_state_entities.add_source_table(
                table_id=table.name,
                schema_fields=bq_schema_for_sqlalchemy_table(SchemaType.STATE, table),
            )

    return normalized_state_entities


def get_dataflow_output_source_table_collections() -> list[SourceTableCollection]:
    """Collects all source tables that are populated by our Dataflow pipelines"""
    dataflow_metrics = SourceTableCollection(
        labels=[DataflowPipelineSourceTableLabel("metrics")],
        dataset_id=DATAFLOW_METRICS_DATASET,
    )

    for metric_class, table_id in dataflow_config.DATAFLOW_METRICS_TO_TABLES.items():
        schema = metric_class.bq_schema_for_metric_table()
        clustering_fields = (
            dataflow_config.METRIC_CLUSTERING_FIELDS
            if all(
                cluster_field in attr.fields_dict(metric_class).keys()  # type: ignore[arg-type]
                for cluster_field in dataflow_config.METRIC_CLUSTERING_FIELDS
            )
            else None
        )

        dataflow_metrics.add_source_table(
            table_id=table_id,
            schema_fields=schema,
            clustering_fields=clustering_fields,
        )

    supplemental_data = SourceTableCollection(
        labels=[DataflowPipelineSourceTableLabel("supplemental")],
        dataset_id=SUPPLEMENTAL_DATA_DATASET,
    )
    for pipeline in collect_all_pipeline_classes():
        if issubclass(pipeline, SupplementalDatasetPipeline):
            supplemental_data.add_source_table(
                table_id=pipeline.table_id(),
                schema_fields=pipeline.bq_schema_for_table(),
            )

    state_collections = []

    for (
        state_code,
        ingest_instance,
    ) in get_ingest_pipeline_enabled_state_and_instance_pairs():
        dataset_id = state_dataset_for_state_code(state_code, ingest_instance)
        state_collection = SourceTableCollection(
            labels=[DataflowPipelineSourceTableLabel("ingest")],
            dataset_id=dataset_id,
        )
        state_collections.append(state_collection)

        for table in list(get_all_table_classes_in_schema(SchemaType.STATE)):
            state_collection.add_source_table(
                table_id=table.name,
                schema_fields=bq_schema_for_sqlalchemy_table(SchemaType.STATE, table),
            )

    return [
        dataflow_metrics,
        supplemental_data,
        _build_normalized_state_entities(),
        *state_collections,
    ]
