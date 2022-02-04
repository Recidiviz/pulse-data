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
from typing import Callable, Dict, List, Optional, Set, Type

from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.testing.test_pipeline import TestPipeline
from mock import patch

from recidiviz.calculator.pipeline.base_pipeline import BasePipeline
from recidiviz.calculator.pipeline.calculation_pipeline import (
    CalculationPipelineRunDelegate,
)
from recidiviz.persistence.database import schema_utils
from recidiviz.persistence.database.base_schema import StateBase
from recidiviz.tests.calculator.pipeline.fake_bigquery import (
    FakeReadFromBigQuery,
    FakeWriteToBigQuery,
)


def run_test_pipeline(
    run_delegate: Type[CalculationPipelineRunDelegate],
    state_code: str,
    project_id: str,
    dataset_id: str,
    read_from_bq_constructor: Callable[[str], FakeReadFromBigQuery],
    write_to_bq_constructor: Callable[[str, str], FakeWriteToBigQuery],
    include_calculation_limit_args: bool = True,
    unifying_id_field_filter_set: Optional[Set[int]] = None,
    metric_types_filter: Optional[Set[str]] = None,
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

    pipeline_args: List[str] = [
        "--project",
        project_id,
        "--data_input",
        dataset_id,
        "--reference_view_input",
        dataset_id,
        "--static_reference_input",
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

    if include_calculation_limit_args:
        pipeline_args.extend(["--calculation_month_count", "-1"])

    pipeline_args.append("--metric_types")
    if metric_types_filter:
        for metric in metric_types_filter:
            pipeline_args.append(metric)
    else:
        pipeline_args.append("ALL")

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
                "recidiviz.calculator.pipeline.calculation_pipeline"
                ".WriteAppendToBigQuery",
                write_to_bq_constructor,
            ):
                with patch(
                    "recidiviz.calculator.pipeline.base_pipeline.beam.Pipeline",
                    pipeline_constructor,
                ):
                    with patch(
                        "recidiviz.calculator.pipeline.calculation_pipeline.job_id",
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
