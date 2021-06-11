# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2021 Recidiviz, Inc.
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
"""Abstract class for all calculation pipelines."""
import abc
import argparse
import logging
from typing import Any, Dict, List, Optional, Set, Type

import apache_beam as beam
import attr
from apache_beam.options.pipeline_options import PipelineOptions, SetupOptions

from recidiviz.calculator.dataflow_config import (
    DATAFLOW_METRICS_TO_TABLES,
    DATAFLOW_TABLES_TO_METRIC_TYPES,
)
from recidiviz.calculator.pipeline.utils.beam_utils import RecidivizMetricWritableDict
from recidiviz.calculator.pipeline.utils.execution_utils import get_job_id
from recidiviz.calculator.pipeline.utils.extractor_utils import WriteAppendToBigQuery
from recidiviz.calculator.pipeline.utils.metric_utils import (
    RecidivizMetric,
    RecidivizMetricType,
)
from recidiviz.calculator.pipeline.utils.pipeline_args_utils import (
    add_shared_pipeline_arguments,
)
from recidiviz.persistence.database.schema.state import schema
from recidiviz.utils import environment

# Cached job_id value
_job_id = None


def job_id(pipeline_options: Dict[str, str]) -> str:
    global _job_id
    if not _job_id:
        _job_id = get_job_id(pipeline_options)
    return _job_id


@environment.test_only
def clear_job_id() -> None:
    global _job_id
    _job_id = None


@attr.s
class BasePipeline(abc.ABC):
    """A base class defining a calculation pipeline."""

    metric_type_class: Type[RecidivizMetricType] = attr.ib()
    metric_class: Type[RecidivizMetric[RecidivizMetricType]] = attr.ib()
    name: str = attr.ib()
    include_calculation_limit_args: bool = attr.ib(default=False)

    @abc.abstractmethod
    def execute_pipeline(
        self,
        pipeline: beam.Pipeline,
        all_pipeline_options: Dict[str, Any],
        state_code: str,
        input_dataset: str,
        reference_dataset: str,
        static_reference_dataset: str,
        metric_types: List[str],
        person_id_filter_set: Optional[Set[int]],
        calculation_month_count: int = -1,
        calculation_end_month: Optional[str] = None,
    ) -> beam.Pipeline:
        """Define the specific pipeline steps here and returns the last step of the pipeline as the output before writing metrics."""

    @property
    def _metric_type_values(self) -> List[str]:
        return [metric_type.value for metric_type in self.metric_type_class]

    @property
    def _metric_subclasses(self) -> Set[Type[RecidivizMetric]]:
        subclasses = set()
        path = [self.metric_class]
        while path:
            parent = path.pop()
            for child in parent.__subclasses__():
                if child not in subclasses:
                    subclasses.add(child)
                    path.append(child)
        return subclasses

    def get_arg_parser(self) -> argparse.ArgumentParser:
        parser = argparse.ArgumentParser()

        add_shared_pipeline_arguments(parser, self.include_calculation_limit_args)

        metric_type_options: List[str] = self._metric_type_values
        metric_type_options.append("ALL")

        parser.add_argument(
            "--metric_types",
            dest="metric_types",
            type=str,
            nargs="+",
            choices=metric_type_options,
            help="A list of the types of metric to calculate.",
            default={"ALL"},
        )

        return parser

    def write_to_metrics(
        self, metrics_output_pipeline: beam.Pipeline, output: str
    ) -> None:
        """Takes the output from metrics_output_pipeline and writes it into a beam.pvalue.TaggedOutput
        dictionary-like object. This is then written to appropriate BigQuery tables under the appropriate
        Dataflow metrics table and namespace.

        Each metric type is a tag in the TaggedOutput and is accessed individually to be written to a
        separate table in BigQuery."""
        writable_metrics = (
            metrics_output_pipeline
            | "Convert to dict to be written to BQ"
            >> beam.ParDo(RecidivizMetricWritableDict()).with_outputs(
                *self._metric_type_values
            )
        )

        for metric_subclass in self._metric_subclasses:
            table_id = DATAFLOW_METRICS_TO_TABLES[metric_subclass]
            metric_type = DATAFLOW_TABLES_TO_METRIC_TYPES[table_id]
            _ = writable_metrics.__getattr__(
                metric_type.value
            ) | f"Write {metric_type.value} metrics to BQ table: {table_id}" >> WriteAppendToBigQuery(
                output_table=table_id, output_dataset=output
            )

    def run(
        self,
        apache_beam_pipeline_options: PipelineOptions,
        data_input: str,
        reference_view_input: str,
        static_reference_input: str,
        output: str,
        metric_types: List[str],
        state_code: str,
        person_filter_ids: Optional[List[int]],
        calculation_month_count: int = -1,
        calculation_end_month: Optional[str] = None,
    ) -> None:
        """Runs the designated pipeline."""

        # Workaround to load SQLAlchemy objects at start of pipeline. This is necessary because the BuildRootEntity
        # function tries to access attributes of relationship properties on the SQLAlchemy room_schema_class before they
        # have been loaded. However, if *any* SQLAlchemy objects have been instantiated, then the relationship properties
        # are loaded and their attributes can be successfully accessed.
        _ = schema.StatePerson()

        apache_beam_pipeline_options.view_as(SetupOptions).save_main_session = True

        # Get pipeline job details
        all_pipeline_options = apache_beam_pipeline_options.get_all_options()
        project_id = all_pipeline_options["project"]

        if project_id is None:
            raise ValueError(
                f"No project set in pipeline options: {all_pipeline_options}"
            )

        if state_code is None:
            raise ValueError("No state_code set for pipeline")

        input_dataset = project_id + "." + data_input
        reference_dataset = project_id + "." + reference_view_input
        static_reference_dataset = project_id + "." + static_reference_input

        person_id_filter_set = set(person_filter_ids) if person_filter_ids else None

        with beam.Pipeline(options=apache_beam_pipeline_options) as p:
            metrics = self.execute_pipeline(
                p,
                all_pipeline_options,
                state_code,
                input_dataset,
                reference_dataset,
                static_reference_dataset,
                metric_types,
                person_id_filter_set,
                calculation_month_count,
                calculation_end_month,
            )

            if person_id_filter_set:
                logging.warning(
                    "Non-empty person filter set - returning before writing metrics."
                )
                return

            self.write_to_metrics(metrics, output)
