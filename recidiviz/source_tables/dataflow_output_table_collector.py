"""Contains utilities for collecting Dataflow output source tables"""

import attr

from recidiviz.calculator.query.state.dataset_config import DATAFLOW_METRICS_DATASET
from recidiviz.pipelines import dataflow_config
from recidiviz.pipelines.pipeline_names import (
    METRICS_PIPELINE_NAME,
    SUPPLEMENTAL_PIPELINE_NAME,
)
from recidiviz.pipelines.supplemental.base_supplemental_dataset_pipeline import (
    SupplementalDatasetPipeline,
)
from recidiviz.pipelines.supplemental.dataset_config import SUPPLEMENTAL_DATA_DATASET
from recidiviz.pipelines.utils.pipeline_run_utils import collect_all_pipeline_classes
from recidiviz.source_tables.ingest_pipeline_output_table_collector import (
    build_ingest_pipeline_output_source_table_collections,
)
from recidiviz.source_tables.normalization_pipeline_output_table_collector import (
    build_normalization_pipeline_output_source_table_collections,
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
        labels=[DataflowPipelineSourceTableLabel(SUPPLEMENTAL_PIPELINE_NAME)],
        update_config=SourceTableCollectionUpdateConfig.regenerable(),
        dataset_id=SUPPLEMENTAL_DATA_DATASET,
    )
    for pipeline in collect_all_pipeline_classes():
        if issubclass(pipeline, SupplementalDatasetPipeline):
            supplemental_data.add_source_table(
                table_id=pipeline.table_id(),
                schema_fields=pipeline.bq_schema_for_table(),
            )

    return [
        dataflow_metrics,
        supplemental_data,
        *build_normalization_pipeline_output_source_table_collections(),
        *build_ingest_pipeline_output_source_table_collections(),
    ]
