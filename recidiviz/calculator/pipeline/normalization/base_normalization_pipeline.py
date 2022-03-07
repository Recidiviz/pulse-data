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
from typing import (
    Any,
    Dict,
    Generator,
    Iterable,
    List,
    Sequence,
    Set,
    Tuple,
    Type,
    Union,
)

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
from recidiviz.calculator.pipeline.utils.beam_utils.bigquery_io_utils import (
    WriteToBigQuery,
    json_serializable_dict,
)
from recidiviz.calculator.pipeline.utils.entity_normalization.entity_normalization_manager import (
    EntityNormalizationManager,
)
from recidiviz.calculator.pipeline.utils.entity_normalization.normalized_entities_utils import (
    AdditionalAttributesMap,
    convert_entities_to_normalized_dicts,
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
from recidiviz.persistence.database import schema_utils
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

    @classmethod
    @abc.abstractmethod
    def required_entity_normalization_managers(
        cls,
    ) -> List[Type[EntityNormalizationManager]]:
        """Returns the entity normalization managers used by the entity normalizer."""

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

    def write_output(self, pipeline: beam.Pipeline) -> None:
        normalized_entity_types: Set[Type[Entity]] = set()
        normalized_entity_class_names: Set[str] = set()

        for manager in self.required_entity_normalization_managers():
            for normalized_entity_class in manager.normalized_entity_classes():
                normalized_entity_types.add(normalized_entity_class)
                normalized_entity_class_names.add(normalized_entity_class.__name__)

        writable_metrics = (
            pipeline
            | "Convert to dict to be written to BQ"
            >> beam.ParDo(NormalizedEntityTreeWritableDicts()).with_outputs(
                *normalized_entity_class_names
            )
        )

        for entity_class_name in normalized_entity_class_names:
            table_id = schema_utils.get_state_database_entity_with_name(
                entity_class_name
            ).__tablename__

            _ = writable_metrics.__getattr__(entity_class_name) | (
                f"Write Normalized{entity_class_name} to BQ table: "
                f"{self.pipeline_job_args.output_dataset}.{table_id}"
            ) >> WriteToBigQuery(
                output_table=table_id,
                output_dataset=self.pipeline_job_args.output_dataset,
                write_disposition=beam.io.BigQueryDisposition.WRITE_TRUNCATE,
            )


@with_input_types(
    beam.typehints.Tuple[int, Dict[str, Iterable[Any]]],
    str,
    BaseEntityNormalizer,
    PipelineConfig,
)
@with_output_types(
    beam.typehints.Tuple[int, Dict[str, Sequence[Entity]], AdditionalAttributesMap],
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
    ) -> Generator[
        Tuple[int, Dict[str, Sequence[Entity]], AdditionalAttributesMap],
        None,
        None,
    ]:
        """Runs the entities through normalization."""
        person_id, person_entities = element

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

        (
            normalized_entities,
            additional_attributes_map,
        ) = entity_normalizer.normalize_entities(all_kwargs)

        yield person_id, normalized_entities, additional_attributes_map

    def to_runner_api_parameter(
        self, _unused_context: PipelineContext
    ) -> Tuple[str, Any]:
        pass


@with_input_types(
    beam.typehints.Tuple[int, Dict[str, Sequence[Entity]], AdditionalAttributesMap]
)
@with_output_types(beam.typehints.Dict[str, Any])
class NormalizedEntityTreeWritableDicts(beam.DoFn):
    """Builds a dictionary in the format necessary to write the output to BigQuery."""

    # pylint: disable=arguments-differ
    def process(
        self,
        element: Tuple[int, Dict[str, Sequence[Entity]], AdditionalAttributesMap],
    ) -> Generator[Dict[str, Any], None, None,]:
        """The beam.io.WriteToBigQuery transform requires elements to be in dictionary
        form, where the values are in formats as required by BigQuery I/O connector.

        For a list of required formats, see the "Data types" section of:
            https://beam.apache.org/documentation/io/built-in/google-bigquery/

        Args:
            element: A tuple containing the person_id of a single person,
                a dictionary with all normalized entities indexed by the name of the
                entity, and an AdditionalAttributesMap storing the attributes
                unique to the Normalized version of each entity that will be
                written in the output

        Yields:
            A dictionary representation of the normalized entity in the format
                Dict[str, Any] so that it can be written to BigQuery.
        """
        person_id, normalized_entities, additional_attributes_map = element

        for normalized_entity_list in normalized_entities.values():
            tagged_entity_dict_outputs = convert_entities_to_normalized_dicts(
                person_id=person_id,
                entities=normalized_entity_list,
                additional_attributes_map=additional_attributes_map,
            )

            for entity_name, entity_dict in tagged_entity_dict_outputs:
                yield beam.pvalue.TaggedOutput(
                    entity_name, json_serializable_dict(entity_dict)
                )

    def to_runner_api_parameter(
        self, _unused_context: PipelineContext
    ) -> Tuple[str, Any]:
        pass
