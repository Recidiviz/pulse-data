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
"""Abstract class for all pipelines that operate on state entities."""
import abc
import argparse
import logging
from typing import Dict, Generic, List, Optional, Set, Type, TypeVar

import apache_beam as beam
import attr
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.pvalue import PBegin

from recidiviz.calculator.pipeline.utils.beam_utils.entity_hydration_utils import (
    ConvertEntitiesToStateSpecificTypes,
)
from recidiviz.calculator.pipeline.utils.beam_utils.extractor_utils import (
    ExtractDataForPipeline,
)
from recidiviz.calculator.pipeline.utils.beam_utils.pipeline_args_utils import (
    get_apache_beam_pipeline_options_from_args,
)
from recidiviz.calculator.pipeline.utils.state_utils.state_specific_delegate import (
    StateSpecificDelegate,
)
from recidiviz.calculator.query.state.dataset_config import (
    REFERENCE_VIEWS_DATASET,
    STATE_BASE_DATASET,
)
from recidiviz.common.constants.states import StateCode
from recidiviz.persistence.database.schema.state import schema
from recidiviz.persistence.entity.base_entity import Entity
from recidiviz.persistence.entity.state import entities


@attr.s(frozen=True)
class PipelineJobArgs:
    """Stores information about the arguments provided to trigger the pipeline job
    being run."""

    # The state_code of the current pipeline
    state_code: str = attr.ib()

    # The project the pipeline is running in
    project_id: str = attr.ib()

    # Which dataset to query from for the data input
    input_dataset: str = attr.ib()

    # Which dataset in BigQuery to write output
    output_dataset: str = attr.ib()

    # Which dataset to query from for the reference views
    reference_dataset: str = attr.ib()

    # Options needed to create an apache-beam pipeline
    apache_beam_pipeline_options: PipelineOptions = attr.ib()

    # An optional set of person_ids. When present, the pipeline will only operate on
    # data for these people and will not produce any output. Should be used for
    # debugging purposes only.
    person_id_filter_set: Optional[Set[int]] = attr.ib()


@attr.s(frozen=True)
class PipelineConfig:
    """Configuration needed for a calculation pipeline. This configuration is unique
    to each pipeline type, and is consistent for all pipeline jobs of the same
    |pipeline_name|."""

    # The name of the pipeline, which is a unique identifier for the pipeline run
    # delegate
    pipeline_name: str = attr.ib()

    # The list of entities required for the pipeline
    required_entities: List[Type[Entity]] = attr.ib()

    # The list of reference tables required for the pipeline
    required_reference_tables: List[str] = attr.ib()

    state_specific_required_delegates: List[Type[StateSpecificDelegate]] = attr.ib()

    # A dictionary mapping state codes to the names of state-specific tables required
    # to run pipelines in the state
    state_specific_required_reference_tables: Dict[StateCode, List[str]] = attr.ib()


PipelineJobArgsT = TypeVar("PipelineJobArgsT", bound=PipelineJobArgs)
PipelineRunDelegateT = TypeVar("PipelineRunDelegateT", bound="PipelineRunDelegate")


class PipelineRunDelegate(abc.ABC, Generic[PipelineJobArgsT]):
    """Base delegate interface required for running a pipeline."""

    def __init__(
        self,
        pipeline_job_args: PipelineJobArgsT,
    ) -> None:
        self.pipeline_job_args = pipeline_job_args
        self._validate_pipeline_config()

    ##### VALIDATION #####
    @abc.abstractmethod
    def _validate_pipeline_config(self) -> None:
        """Validates the contents of the PipelineConfig."""

    ##### FACTORY #####
    @classmethod
    def build_from_args(
        cls: Type[PipelineRunDelegateT],
        argv: List[str],
    ) -> PipelineRunDelegateT:
        """Builds a PipelineRunDelegate from the provided arguments."""
        parser = argparse.ArgumentParser()
        cls.add_pipeline_job_args_to_parser(parser)

        pipeline_job_args = cls._build_pipeline_job_args(parser, argv)

        return cls(
            pipeline_job_args=pipeline_job_args,
        )

    @classmethod
    def add_pipeline_job_args_to_parser(cls, parser: argparse.ArgumentParser) -> None:
        """Adds argument configs to the |parser| for base pipeline args."""
        parser.add_argument(
            "--state_code",
            dest="state_code",
            required=True,
            type=str,
            help="The state_code to include in the calculations.",
        )

        parser.add_argument(
            "--data_input",
            type=str,
            help="BigQuery dataset to query.",
            default=STATE_BASE_DATASET,
        )

        parser.add_argument(
            "--output",
            type=str,
            help="Output dataset to write results to. Pipelines must specify a default "
            "via the default_output_dataset() function on the delegate.",
            required=False,
        )

        parser.add_argument(
            "--reference_view_input",
            type=str,
            help="BigQuery reference view dataset to query.",
            default=REFERENCE_VIEWS_DATASET,
        )

        parser.add_argument(
            "--person_filter_ids",
            type=int,
            nargs="+",
            help="An optional list of DB person_id values. When present, the pipeline "
            "will only calculate metrics for these people and will not output to BQ.",
        )

    @classmethod
    @abc.abstractmethod
    def _build_pipeline_job_args(
        cls,
        parser: argparse.ArgumentParser,
        argv: List[str],
    ) -> PipelineJobArgsT:
        """Builds the PipelineJobArgs object from the provided args."""

    @classmethod
    def _get_base_pipeline_job_args(
        cls,
        parser: argparse.ArgumentParser,
        argv: List[str],
    ) -> PipelineJobArgs:
        (
            known_args,
            remaining_args,
        ) = parser.parse_known_args(argv)
        apache_beam_pipeline_options = get_apache_beam_pipeline_options_from_args(
            remaining_args
        )

        if person_filter_ids := known_args.person_filter_ids:
            person_id_filter_set = {int(person_id) for person_id in person_filter_ids}
        else:
            person_id_filter_set = None

        return PipelineJobArgs(
            state_code=known_args.state_code,
            project_id=apache_beam_pipeline_options.get_all_options()["project"],
            input_dataset=known_args.data_input,
            output_dataset=known_args.output
            or cls.default_output_dataset(known_args.state_code),
            reference_dataset=known_args.reference_view_input,
            apache_beam_pipeline_options=apache_beam_pipeline_options,
            person_id_filter_set=person_id_filter_set,
        )

    @classmethod
    @abc.abstractmethod
    def default_output_dataset(cls, state_code: str) -> str:
        """The default output dataset for the pipeline job. Must be implemented by
        subclasses."""

    ##### STATIC / CLASS CONFIGURATION #####

    @classmethod
    @abc.abstractmethod
    def pipeline_config(cls) -> PipelineConfig:
        """Static pipeline configuration for this pipeline that remains the same for
        all pipeline runs.
        """

    #### PIPELINE RUNTIME IMPLEMENTATION ####

    @abc.abstractmethod
    def run_data_transforms(
        self, p: PBegin, pipeline_data: beam.Pipeline
    ) -> beam.Pipeline:
        """Runs the data transforms of the pipeline."""

    @abc.abstractmethod
    def write_output(self, pipeline: beam.Pipeline) -> None:
        """Write the output to the output_dataset."""


@attr.s
class BasePipeline:
    """A base class defining a pipeline."""

    pipeline_run_delegate: PipelineRunDelegate = attr.ib()

    def run(self) -> None:
        """Runs the designated pipeline."""
        # Workaround to load SQLAlchemy objects at start of pipeline. This is
        # necessary because the BuildRootEntity function tries to access attributes
        # of relationship properties on the SQLAlchemy room_schema_class before they
        # have been loaded. However, if *any* SQLAlchemy objects have been instantiated,
        # then the relationship properties are loaded and their attributes can be
        # successfully accessed.
        _ = schema.StatePerson()

        pipeline_job_args = self.pipeline_run_delegate.pipeline_job_args
        state_code = pipeline_job_args.state_code
        person_id_filter_set = pipeline_job_args.person_id_filter_set

        with beam.Pipeline(
            options=self.pipeline_run_delegate.pipeline_job_args.apache_beam_pipeline_options
        ) as p:
            required_reference_tables = (
                self.pipeline_run_delegate.pipeline_config().required_reference_tables.copy()
            )

            required_reference_tables.extend(
                self.pipeline_run_delegate.pipeline_config().state_specific_required_reference_tables.get(
                    StateCode(state_code.upper()), []
                )
            )

            # Get all required entities and reference data
            pipeline_data = p | "Load required data" >> ExtractDataForPipeline(
                state_code=state_code,
                project_id=pipeline_job_args.project_id,
                dataset=pipeline_job_args.input_dataset,
                reference_dataset=pipeline_job_args.reference_dataset,
                required_entity_classes=self.pipeline_run_delegate.pipeline_config().required_entities,
                required_reference_tables=required_reference_tables,
                unifying_class=entities.StatePerson,
                unifying_id_field_filter_set=person_id_filter_set,
            )

            # Update entities to have state-specific types, where applicable
            pipeline_data = (
                pipeline_data
                | "Convert entities to state-specific type"
                >> beam.ParDo(
                    ConvertEntitiesToStateSpecificTypes(), state_code=state_code
                )
            )

            pipeline_output = self.pipeline_run_delegate.run_data_transforms(
                p, pipeline_data
            )

            if person_id_filter_set:
                logging.warning(
                    "Non-empty person filter set - returning before writing output."
                )
                return

            self.pipeline_run_delegate.write_output(pipeline_output)
