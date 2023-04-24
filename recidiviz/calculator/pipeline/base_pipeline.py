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
from typing import Dict, Generic, List, Type, TypeVar, Union

import apache_beam as beam
import attr
from apache_beam.pvalue import PBegin

from recidiviz.calculator.pipeline.normalization.utils.normalized_entities import (
    NormalizedStateEntity,
)
from recidiviz.calculator.pipeline.pipeline_parameters import PipelineParametersT
from recidiviz.calculator.pipeline.utils.beam_utils.extractor_utils import (
    ExtractDataForPipeline,
)
from recidiviz.calculator.pipeline.utils.state_utils.state_specific_delegate import (
    StateSpecificDelegate,
)
from recidiviz.common.constants.states import StateCode
from recidiviz.persistence.database.schema.state import schema
from recidiviz.persistence.entity.base_entity import Entity
from recidiviz.persistence.entity.state import entities


@attr.s(frozen=True)
class PipelineConfig:
    """Configuration needed for a calculation pipeline. This configuration is unique
    to each pipeline type, and is consistent for all pipeline jobs of the same
    |pipeline_name|."""

    # The name of the pipeline, which is a unique identifier for the pipeline run
    # delegate
    pipeline_name: str = attr.ib()

    # The list of entities required for the pipeline
    required_entities: List[
        Union[Type[Entity], Type[NormalizedStateEntity]]
    ] = attr.ib()

    # The list of reference tables required for the pipeline that are person-id based
    required_reference_tables: List[str] = attr.ib()

    # The list of reference tables required for the pipeline that are state-code based
    required_state_based_reference_tables: List[str] = attr.ib()

    state_specific_required_delegates: List[Type[StateSpecificDelegate]] = attr.ib()

    # A dictionary mapping state codes to the names of state-specific tables required
    # to run pipelines in the state
    state_specific_required_reference_tables: Dict[StateCode, List[str]] = attr.ib()


PipelineRunDelegateT = TypeVar("PipelineRunDelegateT", bound="PipelineRunDelegate")


class PipelineRunDelegate(abc.ABC, Generic[PipelineParametersT]):
    """Base delegate interface required for running a pipeline."""

    def __init__(
        self,
        pipeline_parameters: PipelineParametersT,
    ) -> None:
        self.pipeline_parameters = pipeline_parameters
        self._validate_pipeline_config()

    ##### VALIDATION #####
    @abc.abstractmethod
    def _validate_pipeline_config(self) -> None:
        """Validates the contents of the PipelineConfig."""

    ##### FACTORY #####
    @classmethod
    def build_from_args(
        cls: Type[PipelineRunDelegateT], argv: List[str]
    ) -> PipelineRunDelegateT:
        """Builds a PipelineRunDelegate from the provided arguments."""
        return cls(
            pipeline_parameters=cls.parameters_type().parse_from_args(
                argv=argv,
                sandbox_pipeline=False,
            )
        )

    @classmethod
    @abc.abstractmethod
    def parameters_type(cls) -> Type[PipelineParametersT]:
        """Defines the PipelineParameters class needed for this PipelineRunDelegate."""

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

        pipeline_parameters = self.pipeline_run_delegate.pipeline_parameters
        state_code = pipeline_parameters.state_code
        person_id_filter_set = (
            {
                int(person_id)
                for person_id in pipeline_parameters.person_filter_ids.split(" ")
            }
            if pipeline_parameters.person_filter_ids
            else None
        )

        options = pipeline_parameters.apache_beam_pipeline_options
        if not options:
            raise ValueError("Expected nonnull apache_beam_pipeline_options")
        with beam.Pipeline(options=options) as p:
            required_reference_tables = (
                self.pipeline_run_delegate.pipeline_config().required_reference_tables.copy()
            )

            required_state_based_reference_tables = (
                self.pipeline_run_delegate.pipeline_config().required_state_based_reference_tables.copy()
            )

            required_reference_tables.extend(
                self.pipeline_run_delegate.pipeline_config().state_specific_required_reference_tables.get(
                    StateCode(state_code.upper()), []
                )
            )

            # Get all required entities and reference data
            pipeline_data = p | "Load required data" >> ExtractDataForPipeline(
                state_code=state_code,
                project_id=pipeline_parameters.project,
                entities_dataset=pipeline_parameters.data_input,
                normalized_entities_dataset=pipeline_parameters.normalized_input,
                reference_dataset=pipeline_parameters.reference_view_input,
                required_entity_classes=self.pipeline_run_delegate.pipeline_config().required_entities,
                required_reference_tables=required_reference_tables,
                required_state_based_reference_tables=required_state_based_reference_tables,
                unifying_class=entities.StatePerson,
                unifying_id_field_filter_set=person_id_filter_set,
            )

            pipeline_output = self.pipeline_run_delegate.run_data_transforms(
                p, pipeline_data
            )

            self.pipeline_run_delegate.write_output(pipeline_output)
