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
"""Classes for running all supplemental dataset calculation pipelines."""
import abc
import argparse
from typing import List

from recidiviz.calculator.pipeline.base_pipeline import (
    PipelineConfig,
    PipelineJobArgs,
    PipelineRunDelegate,
)
from recidiviz.calculator.pipeline.supplemental.dataset_config import (
    SUPPLEMENTAL_DATA_DATASET,
)


class SupplementalDatasetPipelineRunDelegate(PipelineRunDelegate):
    """Delegate for running a supplemental dataset pipeline."""

    @classmethod
    @abc.abstractmethod
    def table_id(cls) -> str:
        """Table_id of the output table for the supplemental dataset. Must be overwritten
        by subclasses."""

    @classmethod
    @abc.abstractmethod
    def pipeline_config(cls) -> PipelineConfig:
        pass

    def _validate_pipeline_config(self) -> None:
        if "SUPPLEMENTAL" not in self.pipeline_config().pipeline_name:
            raise ValueError(
                "Can only use SupplementalDatasetPipelineRunDelegate for a supplemental"
                f"dataset pipeline. Trying to run a {self.pipeline_config().pipeline_name} pipeline."
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
        return SUPPLEMENTAL_DATA_DATASET
