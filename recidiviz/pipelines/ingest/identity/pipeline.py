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
graph traversal, and writes results to the {tenant}_identity_cluster.* BigQuery
dataset for the Identity Service to consume via POST /import.

See recidiviz/tools/calculator/run_sandbox_dataflow_pipeline_utils.py for details
on how to launch a local run.
"""
import apache_beam as beam
from apache_beam import Pipeline

from recidiviz.big_query.address_overrides import BigQueryAddressOverrides
from recidiviz.big_query.big_query_query_provider import StateFilteredQueryProvider
from recidiviz.common.constants.states import StateCode
from recidiviz.common.constants.tenants import Tenant
from recidiviz.ingest.direct import direct_ingest_regions
from recidiviz.ingest.direct.ingest_mappings.identity_ingest_view_manifest_compiler_delegate import (
    IdentityIngestViewManifestCompilerDelegate,
)
from recidiviz.ingest.direct.ingest_mappings.ingest_view_contents_context import (
    IngestViewContentsContext,
)
from recidiviz.ingest.direct.ingest_mappings.ingest_view_manifest_collector import (
    IngestViewManifestCollector,
)
from recidiviz.ingest.direct.types.ingest_pipeline_type import IngestPipelineType
from recidiviz.persistence.entity.entities_bq_schema import (
    get_bq_schema_for_entities_module,
)
from recidiviz.persistence.entity.identity import identity_cluster_entities
from recidiviz.persistence.entity.identity.identity_cluster_entities import (
    IdentityCluster,
)
from recidiviz.pipelines.base_pipeline import BasePipeline
from recidiviz.pipelines.ingest.identity.build_identity_clusters import (
    CLUSTER_MEMBERSHIPS,
    FRAGMENTS_WITH_DATES,
    BuildIdentityClusters,
)
from recidiviz.pipelines.ingest.identity.pipeline_parameters import (
    IdentityIngestPipelineParameters,
)
from recidiviz.pipelines.ingest.identity.process_all_identity_ingest_views import (
    ProcessAllIdentityIngestViews,
)
from recidiviz.pipelines.ingest.transforms.cluster_root_external_ids import (
    ClusterRootExternalIds,
)
from recidiviz.pipelines.ingest.transforms.get_root_external_ids import (
    GetRootExternalIdClusterEdges,
)
from recidiviz.pipelines.ingest.transforms.types import ClusterKey
from recidiviz.pipelines.ingest.transforms.write_root_entities_to_bq import (
    WriteRootEntitiesToBQ,
)
from recidiviz.pipelines.ingest.types import ExternalIdKey
from recidiviz.pipelines.pipeline_names import IDENTITY_INGEST_PIPELINE_NAME


class IdentityIngestPipeline(BasePipeline[IdentityIngestPipelineParameters]):
    """Pipeline that clusters external IDs from raw data for the Identity Service."""

    @classmethod
    def parameters_type(cls) -> type[IdentityIngestPipelineParameters]:
        return IdentityIngestPipelineParameters

    @classmethod
    def pipeline_name(cls) -> str:
        return IDENTITY_INGEST_PIPELINE_NAME

    @classmethod
    def all_input_reference_query_providers(
        cls, state_code: StateCode, address_overrides: BigQueryAddressOverrides | None
    ) -> dict[str, StateFilteredQueryProvider]:
        return {}

    def run_pipeline(self, p: Pipeline) -> None:
        tenant = Tenant(self.pipeline_parameters.tenant)
        # v1 only supports tenants that are state codes; non-state tenants are
        # rejected by `IdentityIngestPipelineParameters.raw_data_input_dataset`.
        state_code = tenant.to_state_code()
        region = direct_ingest_regions.get_direct_ingest_region(
            region_code=state_code.value
        )
        ingest_manifest_collector = IngestViewManifestCollector(
            region=region,
            delegate=IdentityIngestViewManifestCompilerDelegate(region=region),
            ingest_pipeline_type=IngestPipelineType.IDENTITY,
        )
        ingest_view_context = IngestViewContentsContext.build_for_project(
            project_id=self.pipeline_parameters.project,
            is_sandbox=self.pipeline_parameters.is_sandbox_pipeline,
            state_code=state_code,
        )
        identity_view_names = ingest_manifest_collector.launchable_ingest_views(
            ingest_view_context
        )

        if not identity_view_names:
            return

        merged_identity_fragments = (
            p
            | "Process identity views"
            >> ProcessAllIdentityIngestViews(
                pipeline_parameters=self.pipeline_parameters
            )
        )

        # Extract co-occurrence edges from fragments, cluster external IDs via
        # connected components, and emit a sorted cluster tuple keyed by each
        # external ID so downstream stages can use the cluster as a
        # deterministic key.
        # Silence `No value for argument 'pcoll' in function call (no-value-for-parameter)`
        # pylint: disable=E1120
        cluster_memberships: beam.PCollection[tuple[ExternalIdKey, ClusterKey]] = (
            merged_identity_fragments
            | "Drop external_id keys before clustering" >> beam.Values()
            | "Drop dates and view names from fragments"
            >> beam.Map(lambda item: item[2])
            | "Extract cluster edges" >> beam.ParDo(GetRootExternalIdClusterEdges())
            | "Cluster external_ids" >> ClusterRootExternalIds()
            | "Sort each cluster for deterministic key"
            >> beam.MapTuple(lambda eid, cluster: (eid, tuple(sorted(cluster))))
        )

        identity_clusters: beam.PCollection[IdentityCluster] = {
            CLUSTER_MEMBERSHIPS: cluster_memberships,
            FRAGMENTS_WITH_DATES: merged_identity_fragments,
        } | "Build IdentityClusters" >> BuildIdentityClusters(tenant=tenant)

        # Write each IdentityCluster's serialized rows to the per-tenant
        # {tenant}_identity_cluster.* tables.
        _ = (
            identity_clusters
            | f"Write IdentityClusters to {self.pipeline_parameters.clustering_output_dataset}"
            >> WriteRootEntitiesToBQ(
                output_dataset=self.pipeline_parameters.clustering_output_dataset,
                output_table_ids=get_bq_schema_for_entities_module(
                    identity_cluster_entities
                ).keys(),
                entities_module=identity_cluster_entities,
            )
        )
