# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2023 Recidiviz, Inc.
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
"""Classes and configurations for all Dataflow pipelines."""
import abc
from typing import Dict, Generic, List, Type, TypeVar

from apache_beam import Pipeline

from recidiviz.big_query.address_overrides import BigQueryAddressOverrides
from recidiviz.big_query.big_query_query_provider import StateFilteredQueryProvider
from recidiviz.common.constants.states import StateCode
from recidiviz.pipelines.pipeline_parameters import PipelineParametersT

PipelineT = TypeVar("PipelineT", bound="BasePipeline")


class BasePipeline(abc.ABC, Generic[PipelineParametersT]):
    """A base interface describing a pipeline."""

    def __init__(self, pipeline_parameters: PipelineParametersT) -> None:
        self.pipeline_parameters = pipeline_parameters
        if not self.pipeline_parameters.apache_beam_pipeline_options:
            raise ValueError("Expected nonnull apache_beam_pipeline_options")

    @classmethod
    @abc.abstractmethod
    def pipeline_name(cls) -> str:
        """Static pipeline name for all pipeline runs."""

    @classmethod
    @abc.abstractmethod
    def all_input_reference_query_providers(
        cls, state_code: StateCode, address_overrides: BigQueryAddressOverrides | None
    ) -> Dict[str, StateFilteredQueryProvider]:
        """The query providers for all queries that are executed by this pipeline for
        the provided state (i.e. includes all state-specific query providers for that
        state), keyed by provider name.
        """

    @classmethod
    @abc.abstractmethod
    def parameters_type(cls) -> Type[PipelineParametersT]:
        """Defines the PipelineParameters class needed for this Pipeline."""

    @classmethod
    def build_from_args(cls: Type[PipelineT], argv: List[str]) -> PipelineT:
        """Builds a Pipeline from the provided arguments."""
        return cls(
            pipeline_parameters=cls.parameters_type().parse_from_args(
                argv=argv, sandbox_pipeline=False
            )
        )

    @abc.abstractmethod
    def run_pipeline(self, p: Pipeline) -> None:
        """Houses the actual logic of the pipeline."""

    def run(self) -> None:
        """Runs the designated pipeline."""
        with Pipeline(
            options=self.pipeline_parameters.apache_beam_pipeline_options
        ) as p:
            self.run_pipeline(p)
