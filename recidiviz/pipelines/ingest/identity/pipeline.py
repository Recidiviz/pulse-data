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
dataset for the Identity Service to consume via POST /trigger_import.

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
from recidiviz.persistence.entity.identity import (
    identity_cluster_entities,
    identity_fragment_entities,
)
from recidiviz.persistence.entity.identity.identity_cluster_entities import (
    IdentityCluster,
)
from recidiviz.persistence.entity.identity.identity_fragment_entities import (
    IdentityFragment,
)
from recidiviz.pipelines.base_pipeline import BasePipeline
from recidiviz.pipelines.ingest.identity.assign_identity_fragment_ids import (
    AssignIdentityFragmentIds,
)
from recidiviz.pipelines.ingest.identity.build_identity_clusters import (
    CLUSTER_MEMBERSHIPS,
    FRAGMENTS_WITH_DATES,
    BuildIdentityClusters,
)
from recidiviz.pipelines.ingest.identity.expected_output_helpers import (
    get_valid_external_id_types,
)
from recidiviz.pipelines.ingest.identity.identity_ingest_pipeline_config import (
    IdentityIngestPipelineConfig,
)
from recidiviz.pipelines.ingest.identity.pipeline_parameters import (
    IdentityIngestPipelineParameters,
)
from recidiviz.pipelines.ingest.identity.process_all_identity_ingest_views import (
    ProcessAllIdentityIngestViews,
)
from recidiviz.pipelines.ingest.identity.validate_identity_cluster_collection import (
    ValidateIdentityClusterCollection,
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
        # v1 only supports tenants that are state codes;
        # IdentityIngestPipelineParameters.raw_data_input_dataset rejects
        # non-state tenants.
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

        identity_config = IdentityIngestPipelineConfig.load_clustering_config()

        merged_identity_fragments = (
            p
            | "Process identity views"
            >> ProcessAllIdentityIngestViews(
                pipeline_parameters=self.pipeline_parameters,
                identity_config=identity_config,
            )
        )

        # The pre-clustering fragments feed two branches: clustering (below) and
        # the debug {tenant}_identity_fragment output. MergeIngestViewRootEntityTrees
        # emits each fragment once per external ID it carries.
        # Silence No value for argument 'pcoll' in function call (no-value-for-parameter)
        # pylint: disable=E1120
        fragments: beam.PCollection[IdentityFragment] = (
            merged_identity_fragments
            | "Drop external_id keys" >> beam.Values()
            | "Drop dates and view names from fragments"
            >> beam.Map(lambda item: item[2])
        )

        # Extract co-occurrence edges from fragments, cluster external IDs via
        # connected components, and emit a sorted cluster tuple keyed by each
        # external ID so downstream stages can use the cluster as a
        # deterministic key.
        cluster_memberships: beam.PCollection[tuple[ExternalIdKey, ClusterKey]] = (
            fragments
            | "Extract cluster edges" >> beam.ParDo(GetRootExternalIdClusterEdges())
            | "Cluster external_ids" >> ClusterRootExternalIds()
            | "Sort each cluster for deterministic key"
            >> beam.MapTuple(lambda eid, cluster: (eid, tuple(sorted(cluster))))
        )

        valid_id_types = get_valid_external_id_types(
            ingest_manifest_collector=ingest_manifest_collector,
            ingest_view_names=identity_view_names,
        )

        identity_clusters: beam.PCollection[IdentityCluster] = (
            {
                CLUSTER_MEMBERSHIPS: cluster_memberships,
                FRAGMENTS_WITH_DATES: merged_identity_fragments,
            }
            | "Build IdentityClusters"
            >> BuildIdentityClusters(
                tenant=tenant,
                valid_id_types=valid_id_types,
                identity_config=identity_config,
            )
            | "Validate IdentityClusterCollection"
            >> ValidateIdentityClusterCollection()
        )

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

        # Write each pre-clustering IdentityFragment's serialized rows to the
        # debug {tenant}_identity_fragment.* tables, deduplicating on the
        # content-hash id: the ingest-view merge emits each fragment once per
        # external ID it carries, so byte-identical copies collapse to one row,
        # while genuinely distinct per-external-ID merges (different content,
        # different id) each keep their own row.
        _ = (
            fragments
            | "Assign identity fragment ids" >> AssignIdentityFragmentIds()
            | "Key fragments by id"
            >> beam.Map(lambda fragment: (fragment.identity_fragment_id, fragment))
            | "Group fragments by id" >> beam.GroupByKey()
            | "Deduplicate fragments" >> beam.Map(lambda kv: next(iter(kv[1])))
            | f"Write IdentityFragments to {self.pipeline_parameters.fragment_output_dataset}"
            >> WriteRootEntitiesToBQ(
                output_dataset=self.pipeline_parameters.fragment_output_dataset,
                output_table_ids=get_bq_schema_for_entities_module(
                    identity_fragment_entities
                ).keys(),
                entities_module=identity_fragment_entities,
            )
        )
