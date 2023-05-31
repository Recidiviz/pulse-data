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
"""The ingest pipeline. See recidiviz/tools/calculator/run_sandbox_calculation_pipeline.py for details
on how to launch a local run."""
from typing import Type

import apache_beam as beam
from apache_beam import Pipeline

from recidiviz.pipelines.base_pipeline import BasePipeline
from recidiviz.pipelines.ingest.pipeline_parameters import IngestPipelineParameters


class StateIngestPipeline(BasePipeline[IngestPipelineParameters]):
    """Defines the ingest pipeline that reads from raw data, creates ingest view results
    and then creates state entities based on those ingest view results."""

    @classmethod
    def parameters_type(cls) -> Type[IngestPipelineParameters]:
        return IngestPipelineParameters

    @classmethod
    def pipeline_name(cls) -> str:
        return "INGEST"

    # TODO(#20928) Replace with actual pipeline logic.
    def run_pipeline(self, p: Pipeline) -> None:
        _ = (
            p
            | "Dataset" >> beam.Create(["Ingest View 1", "Ingest View 2"])
            | "Print" >> beam.Map(print)
        )
