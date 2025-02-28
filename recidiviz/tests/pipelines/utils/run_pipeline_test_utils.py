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
"""Helper that runs a test version of the pipeline in the provided module."""
from typing import Any, Callable, Dict, Iterable, List, Optional, Set, Type

import apache_beam
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.pipeline import Pipeline
from apache_beam.testing.test_pipeline import TestPipeline
from mock import patch

from recidiviz.persistence.database import schema_utils
from recidiviz.persistence.database.base_schema import StateBase
from recidiviz.persistence.database.schema_utils import (
    get_state_database_entity_with_name,
)
from recidiviz.persistence.entity.state.normalized_state_entity import (
    NormalizedStateEntity,
)
from recidiviz.pipelines.base_pipeline import BasePipeline
from recidiviz.pipelines.ingest.state import write_root_entities_to_bq
from recidiviz.pipelines.ingest.state.pipeline import StateIngestPipeline
from recidiviz.pipelines.metrics.base_metric_pipeline import MetricPipeline
from recidiviz.pipelines.supplemental.base_supplemental_dataset_pipeline import (
    SupplementalDatasetPipeline,
)
from recidiviz.pipelines.utils.execution_utils import RootEntityId
from recidiviz.tests.pipelines.fake_bigquery import (
    FakeReadFromBigQuery,
    FakeReadFromBigQueryWithEmulator,
    FakeWriteToBigQuery,
    FakeWriteToBigQueryEmulator,
)


def pipeline_constructor(project_id: str) -> Callable[[PipelineOptions], Pipeline]:
    def _inner_pipeline_constructor(options: PipelineOptions) -> Pipeline:
        non_default_options = options.get_all_options(drop_default=True)
        expected_non_default_options = {
            "project": project_id,
            "save_main_session": False,
        }

        for option in expected_non_default_options:
            if option not in non_default_options.keys():
                raise ValueError(
                    f"Expected non-default option [{option}] not found in"
                    f"non-default options [{non_default_options}]"
                )
        return TestPipeline()

    return _inner_pipeline_constructor


def get_job_id(project_id: str, region: str, job_name: str) -> str:
    return f"{project_id}-{region}-{job_name}"


def run_test_pipeline(
    pipeline_cls: Type[BasePipeline],
    state_code: str,
    project_id: str,
    read_from_bq_constructor: Callable[
        [str, bool, bool, dict[str, str]],
        FakeReadFromBigQuery | FakeReadFromBigQueryWithEmulator,
    ],
    write_to_bq_constructor: Callable[
        [str, str, apache_beam.io.BigQueryDisposition],
        FakeWriteToBigQuery | FakeWriteToBigQueryEmulator,
    ],
    root_entity_id_filter_set: Optional[Set[RootEntityId]] = None,
    **additional_pipeline_args: Any,
) -> None:
    """Runs a test version of the pipeline in the provided module with BQ I/O mocked out."""

    pipeline_args = default_arg_list_for_pipeline(
        pipeline=pipeline_cls,
        state_code=state_code,
        project_id=project_id,
        root_entity_id_filter_set=root_entity_id_filter_set,
        **additional_pipeline_args,
    )

    if issubclass(pipeline_cls, MetricPipeline):
        write_to_bq_class = (
            "recidiviz.pipelines.metrics.base_metric_pipeline.WriteToBigQuery"
        )
    elif issubclass(pipeline_cls, SupplementalDatasetPipeline):
        class_name = pipeline_cls.pipeline_name().lower().replace("_supplemental", "")
        write_to_bq_class = (
            f"recidiviz.pipelines.supplemental.{class_name}.pipeline.WriteToBigQuery"
        )
    elif issubclass(pipeline_cls, StateIngestPipeline):
        write_to_bq_class = "recidiviz.pipelines.ingest.state.pipeline.WriteToBigQuery"
    else:
        raise ValueError(f"Pipeline class not recognized: {pipeline_cls}")

    with patch(
        "apache_beam.io.ReadFromBigQuery",
        read_from_bq_constructor,
    ):
        with patch(
            f"{write_root_entities_to_bq.__name__}.WriteToBigQuery",
            write_to_bq_constructor,
        ), patch(write_to_bq_class, write_to_bq_constructor):
            with patch(
                "recidiviz.pipelines.base_pipeline.Pipeline",
                pipeline_constructor(project_id),
            ):
                with patch(
                    "recidiviz.pipelines.metrics.base_metric_pipeline.job_id",
                    get_job_id,
                ):
                    pipeline_cls.build_from_args(pipeline_args).run()


def default_data_dict_for_root_schema_classes(
    root_schema_classes: Iterable[Type[StateBase]],
) -> Dict[str, Iterable]:
    """Helper function for running test pipelines that determines the set of tables
    required for hydrating the list of root schema classes and their related classes.

    Returns a dictionary where the keys are the required tables, and the values are
    empty lists.
    """
    root_table_names = {
        schema_class.__tablename__ for schema_class in root_schema_classes
    }

    relationship_properties = {
        property_object
        for schema_class in root_schema_classes
        for property_object in schema_class.get_relationship_property_names_and_properties().values()
    }

    related_class_tables = {
        schema_utils.get_state_database_entity_with_name(
            property_object.mapper.class_.__name__
        ).__tablename__
        for property_object in relationship_properties
    }

    association_tables = {
        property_object.secondary.name
        for property_object in relationship_properties
        if property_object.secondary is not None
    }

    return {
        table: []
        for table in root_table_names.union(
            related_class_tables.union(association_tables)
        )
    }


def default_data_dict_for_pipeline_class(
    pipeline_class: Type[MetricPipeline],
) -> Dict[str, Iterable]:
    """Helper function for running test pipelines that determines the set of tables
    required for hydrating the list of entities required by the the run delegate.

    Returns a dictionary where the keys are the required tables, and the values are
    empty lists.
    """
    required_base_entity_class_names = [
        (
            entity_class.base_class_name()
            if issubclass(entity_class, NormalizedStateEntity)
            else entity_class.__name__
        )
        for entity_class in pipeline_class.required_entities()
    ]

    return default_data_dict_for_root_schema_classes(
        [
            get_state_database_entity_with_name(base_entity_class_name)
            for base_entity_class_name in required_base_entity_class_names
        ]
    )


DEFAULT_TEST_PIPELINE_OUTPUT_SANDBOX_PREFIX = "sandbox"


def default_arg_list_for_pipeline(
    pipeline: Type[BasePipeline],
    state_code: str,
    project_id: str,
    root_entity_id_filter_set: Optional[Set[RootEntityId]] = None,
    **additional_pipeline_args: Any,
) -> List[str]:
    """Constructs the list of default arguments that should be used for a test
    pipeline."""
    pipeline_args: List[str] = [
        "--project",
        project_id,
        "--state_code",
        state_code,
        "--pipeline",
        pipeline.pipeline_name().lower(),
        "--output_sandbox_prefix",
        DEFAULT_TEST_PIPELINE_OUTPUT_SANDBOX_PREFIX,
    ]

    if root_entity_id_filter_set:
        pipeline_args.extend(
            [
                "--person_filter_ids",
                (", ".join([str(id_value) for id_value in root_entity_id_filter_set])),
            ]
        )

    if issubclass(pipeline, MetricPipeline):
        pipeline_args.extend(
            _additional_default_args_for_metrics_pipeline(
                include_calculation_limit_args=additional_pipeline_args.get(
                    "include_calculation_limit_args", True
                ),
                metric_types_filter=additional_pipeline_args.get("metric_types_filter"),
            )
        )
    elif issubclass(pipeline, SupplementalDatasetPipeline):
        pass
    elif issubclass(pipeline, StateIngestPipeline):
        if ingest_view_results_only := additional_pipeline_args.get(
            "ingest_view_results_only"
        ):
            pipeline_args.extend(
                [
                    "--ingest_view_results_only",
                    str(ingest_view_results_only),
                ]
            )
        if pre_normalization_only := additional_pipeline_args.get(
            "pre_normalization_only"
        ):
            pipeline_args.extend(
                [
                    "--pre_normalization_only",
                    str(pre_normalization_only),
                ]
            )
        if ingest_views_to_run := additional_pipeline_args.get("ingest_views_to_run"):
            pipeline_args.extend(["--ingest_views_to_run", ingest_views_to_run])
        if not (
            raw_data_upper_bound_dates_json := additional_pipeline_args.get(
                "raw_data_upper_bound_dates_json"
            )
        ):
            raw_data_upper_bound_dates_json = "{}"

        pipeline_args.extend(
            ["--raw_data_upper_bound_dates_json", raw_data_upper_bound_dates_json]
        )

    else:
        raise ValueError(f"Unexpected Pipeline type: {type(pipeline)}.")

    return pipeline_args


def _additional_default_args_for_metrics_pipeline(
    include_calculation_limit_args: bool = True,
    metric_types_filter: Optional[Set[str]] = None,
) -> List[str]:
    """Returns the additional default arguments that should be used for testing a
    metrics pipeline."""
    additional_args: List[str] = []

    if include_calculation_limit_args:
        additional_args.extend(["--calculation_month_count", "-1"])

    additional_args.append("--metric_types")
    if metric_types_filter:
        additional_args.append(f"{' '.join(list(metric_types_filter))}")
    else:
        additional_args.append("ALL")

    return additional_args
