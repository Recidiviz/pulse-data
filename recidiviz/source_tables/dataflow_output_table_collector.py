"""Contains utilities for collecting Dataflow output source tables"""

import attr

from recidiviz.big_query.big_query_utils import schema_field_for_type
from recidiviz.calculator.query.state.dataset_config import DATAFLOW_METRICS_DATASET
from recidiviz.pipelines import dataflow_config
from recidiviz.pipelines.pipeline_names import (
    METRICS_PIPELINE_NAME,
    SUPPLEMENTAL_PIPELINE_NAME,
)
from recidiviz.pipelines.supplemental.dataset_config import SUPPLEMENTAL_DATA_DATASET
from recidiviz.source_tables.ingest_pipeline_output_table_collector import (
    build_ingest_pipeline_output_source_table_collections,
)
from recidiviz.source_tables.source_table_config import (
    DataflowPipelineSourceTableLabel,
    SourceTableCollection,
    SourceTableCollectionUpdateConfig,
)


def get_dataflow_output_source_table_collections() -> list[SourceTableCollection]:
    """Collects all source tables that are populated by our Dataflow pipelines"""
    dataflow_metrics = SourceTableCollection(
        labels=[DataflowPipelineSourceTableLabel(METRICS_PIPELINE_NAME)],
        update_config=SourceTableCollectionUpdateConfig.regenerable(),
        dataset_id=DATAFLOW_METRICS_DATASET,
        description="Stores output of metric Dataflow pipeline jobs.",
    )

    for metric_class, table_id in dataflow_config.DATAFLOW_METRICS_TO_TABLES.items():
        schema = metric_class.bq_schema_for_metric_table()
        # cluster on all fields in dataflow_config.METRIC_CLUSTERING_FIELDS that are
        # present in metric table
        clustering_fields = [
            cluster_field
            for cluster_field in dataflow_config.METRIC_CLUSTERING_FIELDS
            if cluster_field in attr.fields_dict(metric_class).keys()  # type: ignore[arg-type]
        ] or None

        dataflow_metrics.add_source_table(
            table_id=table_id,
            schema_fields=schema,
            clustering_fields=clustering_fields,
        )

    supplemental_data = SourceTableCollection(
        labels=[DataflowPipelineSourceTableLabel(SUPPLEMENTAL_PIPELINE_NAME)],
        update_config=SourceTableCollectionUpdateConfig.regenerable(),
        dataset_id=SUPPLEMENTAL_DATA_DATASET,
        description=(
            "Stores output of supplemental Dataflow pipelines, one-off pipelines that "
            "are not standard ingest or metric pipelines."
        ),
    )

    for (
        table_id,
        table_fields,
    ) in dataflow_config.DATAFLOW_SUPPLEMENTAL_TABLE_TO_TABLE_FIELDS.items():
        supplemental_data.add_source_table(
            table_id=table_id,
            schema_fields=[
                schema_field_for_type(field_name=field_name, field_type=field_type)
                for field_name, field_type in table_fields.items()
            ],
        )

    return [
        dataflow_metrics,
        supplemental_data,
        *build_ingest_pipeline_output_source_table_collections(),
    ]
