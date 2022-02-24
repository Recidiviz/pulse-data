# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2022 Recidiviz, Inc.
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
"""Classes for running all normalization calculation pipelines."""
import abc
import argparse
from typing import Any, Dict, Generator, Iterable, List, Sequence, Tuple, Union

import apache_beam as beam
from apache_beam.pvalue import PBegin
from apache_beam.runners.pipeline_context import PipelineContext
from apache_beam.typehints import with_input_types, with_output_types

from recidiviz.calculator.pipeline.base_pipeline import (
    PipelineConfig,
    PipelineJobArgs,
    PipelineRunDelegate,
)
from recidiviz.calculator.pipeline.normalization.base_entity_normalizer import (
    BaseEntityNormalizer,
)
from recidiviz.calculator.pipeline.utils.entity_normalization.normalized_entities import (
    NormalizedStateEntity,
)
from recidiviz.calculator.pipeline.utils.execution_utils import (
    TableRow,
    kwargs_for_entity_lists,
)
from recidiviz.calculator.pipeline.utils.state_utils.state_calculation_config_manager import (
    get_required_state_specific_delegates,
)
from recidiviz.calculator.pipeline.utils.state_utils.state_specific_delegate import (
    StateSpecificDelegate,
)
from recidiviz.calculator.query.state.dataset_config import (
    normalized_state_dataset_for_state_code,
)
from recidiviz.common.constants.states import StateCode
from recidiviz.persistence.entity.base_entity import Entity


class NormalizationPipelineRunDelegate(PipelineRunDelegate):
    """Delegate for running an entity normalization pipeline."""

    @classmethod
    @abc.abstractmethod
    def pipeline_config(cls) -> PipelineConfig:
        pass

    @classmethod
    @abc.abstractmethod
    def entity_normalizer(cls) -> BaseEntityNormalizer:
        """Returns the entity normalizer for this pipeline."""

    def _validate_pipeline_config(self) -> None:
        if "NORMALIZATION" not in self.pipeline_config().pipeline_name:
            raise ValueError(
                "Can only use the NormalizationPipelineRunDelegate for "
                "a normalization pipeline. Trying to run a "
                f"{self.pipeline_config().pipeline_name} pipeline."
            )

    @classmethod
    def _build_pipeline_job_args(
        cls,
        parser: argparse.ArgumentParser,
        argv: List[str],
    ) -> PipelineJobArgs:
        """Builds the PipelineJobArgs object from the provided args."""
        return cls._get_base_pipeline_job_args(parser, argv)

    @classmethod
    def default_output_dataset(cls, state_code: str) -> str:
        return normalized_state_dataset_for_state_code(StateCode(state_code))

    def run_data_transforms(
        self, p: PBegin, pipeline_data: beam.Pipeline
    ) -> beam.Pipeline:
        """Runs the data transforms of an entity normalization calculation pipeline.
        Returns the normalized entities to be written to BigQuery."""
        normalized_entities = pipeline_data | "Normalize entities" >> beam.ParDo(
            NormalizeEntities(),
            state_code=self.pipeline_job_args.state_code,
            entity_normalizer=self.entity_normalizer(),
            pipeline_config=self.pipeline_config(),
        )

        return normalized_entities

    @abc.abstractmethod
    def write_output(self, pipeline: beam.Pipeline) -> None:
        # TODO(#10724): This needs to be implemented for output from the
        #  normalization pipelines to be written to BigQuery
        pass


@with_input_types(
    beam.typehints.Tuple[int, Dict[str, Iterable[Any]]],
    str,
    BaseEntityNormalizer,
    PipelineConfig,
)
@with_output_types(
    beam.typehints.Dict[str, Sequence[NormalizedStateEntity]],
)
class NormalizeEntities(beam.DoFn):
    """Normalizes entities."""

    # pylint: disable=arguments-differ
    def process(
        self,
        element: Tuple[int, Dict[str, Iterable[Any]]],
        state_code: str,
        entity_normalizer: BaseEntityNormalizer,
        pipeline_config: PipelineConfig,
    ) -> Generator[Dict[str, Sequence[NormalizedStateEntity]], None, None]:
        """Runs the entities through normalization."""
        _, person_entities = element

        entity_kwargs = kwargs_for_entity_lists(person_entities)

        required_delegates = get_required_state_specific_delegates(
            state_code=state_code,
            required_delegates=pipeline_config.state_specific_required_delegates,
        )

        all_kwargs: Dict[
            str, Union[List[Entity], List[TableRow], StateSpecificDelegate]
        ] = {
            **entity_kwargs,
            **required_delegates,
        }

        normalized_entities = entity_normalizer.normalize_entities(all_kwargs)

        yield normalized_entities

    def to_runner_api_parameter(
        self, _unused_context: PipelineContext
    ) -> Tuple[str, Any]:
        pass
