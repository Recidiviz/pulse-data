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
from typing import Any, Callable, Dict, List, Optional, Set, Type

import apache_beam
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.testing.test_pipeline import TestPipeline
from mock import patch

from recidiviz.calculator.pipeline.base_pipeline import (
    BasePipeline,
    PipelineRunDelegate,
)
from recidiviz.calculator.pipeline.metrics.base_metric_pipeline import (
    MetricPipelineRunDelegate,
)
from recidiviz.calculator.pipeline.normalization.base_normalization_pipeline import (
    NormalizationPipelineRunDelegate,
)
from recidiviz.calculator.pipeline.supplemental.base_supplemental_dataset_pipeline import (
    SupplementalDatasetPipelineRunDelegate,
)
from recidiviz.calculator.pipeline.utils.entity_normalization.normalized_entities_utils import (
    state_base_entity_class_for_entity_class,
)
from recidiviz.persistence.database import schema_utils
from recidiviz.persistence.database.base_schema import StateBase
from recidiviz.persistence.database.schema_utils import (
    get_state_database_entity_with_name,
)
from recidiviz.tests.calculator.pipeline.fake_bigquery import (
    FakeReadFromBigQuery,
    FakeWriteToBigQuery,
)


def run_test_pipeline(
    run_delegate: Type[PipelineRunDelegate],
    state_code: str,
    project_id: str,
    dataset_id: str,
    read_from_bq_constructor: Callable[[str], FakeReadFromBigQuery],
    write_to_bq_constructor: Callable[
        [str, str, apache_beam.io.BigQueryDisposition], FakeWriteToBigQuery
    ],
    unifying_id_field_filter_set: Optional[Set[int]] = None,
    **additional_pipeline_args: Any,
) -> None:
    """Runs a test version of the pipeline in the provided module with BQ I/O mocked out."""

    def pipeline_constructor(options: PipelineOptions) -> TestPipeline:
        non_default_options = options.get_all_options(drop_default=True)
        expected_non_default_options = {
            "project": project_id,
            "save_main_session": True,
        }

        for option in expected_non_default_options:
            if option not in non_default_options.keys():
                raise ValueError(
                    f"Expected non-default option [{option}] not found in"
                    f"non-default options [{non_default_options}]"
                )
        return TestPipeline()

    def get_job_id(project_id: str, region: str, job_name: str) -> str:
        return f"{project_id}-{region}-{job_name}"

    pipeline_args = default_arg_list_for_pipeline(
        run_delegate=run_delegate,
        state_code=state_code,
        project_id=project_id,
        dataset_id=dataset_id,
        unifying_id_field_filter_set=unifying_id_field_filter_set,
        **additional_pipeline_args,
    )

    with patch(
        "recidiviz.calculator.pipeline.utils.beam_utils.extractor_utils.ReadFromBigQuery",
        read_from_bq_constructor,
    ):
        with patch(
            "recidiviz.calculator.pipeline.utils.beam_utils"
            ".bigquery_io_utils.ReadFromBigQuery",
            read_from_bq_constructor,
        ):
            with patch(
                "recidiviz.calculator.pipeline.metrics.base_metric_pipeline"
                ".WriteToBigQuery",
                write_to_bq_constructor,
            ):
                with patch(
                    "recidiviz.calculator.pipeline.normalization"
                    ".base_normalization_pipeline"
                    ".WriteToBigQuery",
                    write_to_bq_constructor,
                ):
                    with patch(
                        "recidiviz.calculator.pipeline.supplemental"
                        ".us_id_case_note_extracted_entities.pipeline"
                        ".WriteToBigQuery",
                        write_to_bq_constructor,
                    ):
                        with patch(
                            "recidiviz.calculator.pipeline.base_pipeline.beam.Pipeline",
                            pipeline_constructor,
                        ):
                            with patch(
                                "recidiviz.calculator.pipeline.metrics.base_metric_pipeline.job_id",
                                get_job_id,
                            ):
                                pipeline = BasePipeline(
                                    pipeline_run_delegate=run_delegate.build_from_args(
                                        pipeline_args
                                    )
                                )
                                pipeline.run()


def default_data_dict_for_root_schema_classes(
    root_schema_classes: List[Type[StateBase]],
) -> Dict[str, List]:
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


def default_data_dict_for_run_delegate(
    run_delegate: Type[PipelineRunDelegate],
) -> Dict[str, List]:
    """Helper function for running test pipelines that determines the set of tables
    required for hydrating the list of entities required by the the run delegate.

    Returns a dictionary where the keys are the required tables, and the values are
    empty lists.
    """
    required_base_entities = [
        state_base_entity_class_for_entity_class(entity_class)
        for entity_class in run_delegate.pipeline_config().required_entities
    ]

    return default_data_dict_for_root_schema_classes(
        [
            get_state_database_entity_with_name(base_entity_class.__name__)
            for base_entity_class in required_base_entities
        ]
    )


def default_arg_list_for_pipeline(
    run_delegate: Type[PipelineRunDelegate],
    state_code: str,
    project_id: str,
    dataset_id: str,
    unifying_id_field_filter_set: Optional[Set[int]] = None,
    **additional_pipeline_args: Any,
) -> List[str]:
    """Constructs the list of default arguments that should be used for a test
    pipeline."""
    pipeline_args: List[str] = [
        "--project",
        project_id,
        "--data_input",
        dataset_id,
        "--reference_view_input",
        dataset_id,
        "--output",
        dataset_id,
        "--state_code",
        state_code,
        "--job_name",
        "test_job",
    ]

    if unifying_id_field_filter_set:
        pipeline_args.extend(
            [
                "--person_filter_ids",
                (
                    ", ".join(
                        [str(id_value) for id_value in unifying_id_field_filter_set]
                    )
                ),
            ]
        )

    if issubclass(run_delegate, MetricPipelineRunDelegate):
        pipeline_args.extend(
            _additional_default_args_for_metrics_pipeline(
                dataset_id=dataset_id,
                include_calculation_limit_args=additional_pipeline_args.get(
                    "include_calculation_limit_args", True
                ),
                metric_types_filter=additional_pipeline_args.get("metric_types_filter"),
            )
        )
    elif issubclass(run_delegate, NormalizationPipelineRunDelegate):
        pass
    elif issubclass(run_delegate, SupplementalDatasetPipelineRunDelegate):
        pass
    else:
        raise ValueError(f"Unexpected PipelineRunDelegate type: {type(run_delegate)}.")

    return pipeline_args


def _additional_default_args_for_metrics_pipeline(
    dataset_id: str,
    include_calculation_limit_args: bool = True,
    metric_types_filter: Optional[Set[str]] = None,
) -> List[str]:
    """Returns the additional default arguments that should be used for testing a
    metrics pipeline."""
    additional_args: List[str] = []

    additional_args.extend(
        [
            "--static_reference_input",
            dataset_id,
        ]
    )

    if include_calculation_limit_args:
        additional_args.extend(["--calculation_month_count", "-1"])

    additional_args.append("--metric_types")
    if metric_types_filter:
        for metric in metric_types_filter:
            additional_args.append(metric)
    else:
        additional_args.append("ALL")

    return additional_args
