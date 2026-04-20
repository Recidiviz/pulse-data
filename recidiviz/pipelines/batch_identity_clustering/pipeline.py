# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2026 Recidiviz, Inc.
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
"""The batch identity clustering pipeline.

This pipeline reads raw data from us_xx_raw_data, clusters external IDs using
graph traversal, and writes results to the batch_identity_clustering.* BigQuery
dataset for the Identity Service to consume via POST /import.

See recidiviz/tools/calculator/run_sandbox_dataflow_pipeline_utils.py for details
on how to launch a local run.
"""

from typing import Dict, Type

from apache_beam import Pipeline

from recidiviz.big_query.address_overrides import BigQueryAddressOverrides
from recidiviz.big_query.big_query_query_provider import StateFilteredQueryProvider
from recidiviz.common.constants.states import StateCode
from recidiviz.pipelines.base_pipeline import BasePipeline
from recidiviz.pipelines.batch_identity_clustering.pipeline_parameters import (
    BatchIdentityClusteringPipelineParameters,
)


class BatchIdentityClusteringPipeline(
    BasePipeline[BatchIdentityClusteringPipelineParameters]
):
    """Pipeline that clusters external IDs from raw data for the Identity Service."""

    @classmethod
    def parameters_type(cls) -> Type[BatchIdentityClusteringPipelineParameters]:
        return BatchIdentityClusteringPipelineParameters

    @classmethod
    def pipeline_name(cls) -> str:
        return "BATCH_IDENTITY_CLUSTERING"

    @classmethod
    def all_input_reference_query_providers(
        cls, state_code: StateCode, address_overrides: BigQueryAddressOverrides | None
    ) -> Dict[str, StateFilteredQueryProvider]:
        return {}

    def run_pipeline(self, p: Pipeline) -> None:
        # TODO(#71769): Implement clustering pipeline steps
        pass
