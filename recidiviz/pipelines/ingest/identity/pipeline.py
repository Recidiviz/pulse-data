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
"""The identity ingest pipeline.

This pipeline reads raw data from us_xx_raw_data, clusters external IDs using
graph traversal, and writes results to the identity_ingest_results.* BigQuery
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
from recidiviz.pipelines.ingest.identity.pipeline_parameters import (
    IdentityIngestPipelineParameters,
)


class IdentityIngestPipeline(BasePipeline[IdentityIngestPipelineParameters]):
    """Pipeline that clusters external IDs from raw data for the Identity Service."""

    @classmethod
    def parameters_type(cls) -> Type[IdentityIngestPipelineParameters]:
        return IdentityIngestPipelineParameters

    @classmethod
    def pipeline_name(cls) -> str:
        return "IDENTITY_INGEST"

    @classmethod
    def all_input_reference_query_providers(
        cls, state_code: StateCode, address_overrides: BigQueryAddressOverrides | None
    ) -> Dict[str, StateFilteredQueryProvider]:
        return {}

    def run_pipeline(self, p: Pipeline) -> None:
        # TODO(#71769): Implement identity ingest pipeline steps.
        #
        # Pipeline stages implemented so far (in order):
        #
        # 1. GenerateIdentityFragments
        #    Read ingest view results from BQ
        #    → PCollection[(UpperBoundDate, IdentityFragment)]
        #
        # 2. MergeIngestViewRootEntityTrees[IdentityFragment]
        #    Merge fragments sharing an external ID key and date within a single ingest view
        #    → PCollection[(ExternalIdKey, (UpperBoundDate, IngestViewName, IdentityFragment))]
        #
        # 3. GetRootExternalIdClusterEdges
        #    Extract co-occurrence edges from merged fragments
        #    → PCollection[(ExternalIdKey, ExternalIdKey | None)]
        #
        # 4. ClusterRootExternalIds
        #    Compute connected components from edges
        #    → PCollection[(ExternalIdKey, set[ExternalIdKey])]
        pass
