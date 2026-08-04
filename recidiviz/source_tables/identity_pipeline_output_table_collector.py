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
"""Helpers for building source table collections for identity ingest pipeline
outputs.
"""
from recidiviz.common.constants.tenants import Tenant
from recidiviz.ingest.direct import direct_ingest_regions
from recidiviz.ingest.direct.ingest_mappings.identity_ingest_view_manifest_compiler_delegate import (
    IdentityIngestViewManifestCompilerDelegate,
)
from recidiviz.ingest.direct.ingest_mappings.ingest_view_manifest_collector import (
    IngestViewManifestCollector,
)
from recidiviz.ingest.direct.regions.direct_ingest_region_utils import (
    get_direct_ingest_states_existing_in_env,
)
from recidiviz.ingest.direct.types.ingest_pipeline_type import IngestPipelineType
from recidiviz.persistence.entity.entities_bq_schema import (
    get_bq_schema_for_entities_module,
)
from recidiviz.persistence.entity.identity import (
    identity_cluster_entities,
    identity_fragment_entities,
)
from recidiviz.pipelines.ingest.identity.dataset_config import (
    identity_cluster_dataset_for_tenant,
    identity_fragment_dataset_for_tenant,
    identity_ingest_view_results_dataset_for_tenant,
    identity_rejections_dataset_for_tenant,
)
from recidiviz.pipelines.ingest.identity.rejected_identity_cluster import (
    IDENTITY_CLUSTER_ID_COL,
    REJECTED_IDENTITY_CLUSTER_TABLE_ID,
    rejected_identity_cluster_schema_fields,
)
from recidiviz.pipelines.pipeline_names import IDENTITY_INGEST_PIPELINE_NAME
from recidiviz.source_tables.ingest_pipeline_output_helpers import (
    build_ingest_view_results_source_table_collection_from_manifests,
)
from recidiviz.source_tables.source_table_config import (
    DataflowPipelineSourceTableLabel,
    SourceTableCollection,
    SourceTableCollectionUpdateConfig,
    StateSpecificSourceTableLabel,
)


def build_identity_ingest_view_results_source_table_collection(
    tenant: Tenant,
) -> SourceTableCollection:
    """Build the source table collection for the
    `{tenant}_identity_ingest_view_results` identity ingest pipeline output
    dataset for a given tenant.
    """
    try:
        state_code = tenant.to_state_code()
    except ValueError as e:
        raise ValueError(
            "No support for identity ingest pipeline with non-state tenants. If you "
            "are trying to run this pipeline with a non-state tenant like NYC, you "
            "will need to first add support for non-state ingest view result datasets."
        ) from e
    region = direct_ingest_regions.get_direct_ingest_region(
        region_code=state_code.value
    )
    return build_ingest_view_results_source_table_collection_from_manifests(
        dataset_id=identity_ingest_view_results_dataset_for_tenant(tenant.value),
        description=(
            f"Stores materialized ingest view results produced by the "
            f"identity ingest pipeline for tenant {tenant.value}."
        ),
        labels=[
            DataflowPipelineSourceTableLabel(IDENTITY_INGEST_PIPELINE_NAME),
            StateSpecificSourceTableLabel(state_code=state_code),
        ],
        manifest_collector=IngestViewManifestCollector(
            region=region,
            delegate=IdentityIngestViewManifestCompilerDelegate(region=region),
            ingest_pipeline_type=IngestPipelineType.IDENTITY,
        ),
    )


def build_identity_fragment_output_source_table_collection(
    tenant: Tenant,
) -> SourceTableCollection:
    """Build the source table collection for the `{tenant}_identity_fragment`
    identity ingest pipeline output dataset for a given tenant. This is a
    debug-only dataset carrying the pre-clustering IdentityFragment output,
    used to investigate clustering decisions.
    """
    collection = SourceTableCollection(
        dataset_id=identity_fragment_dataset_for_tenant(tenant.value),
        update_config=SourceTableCollectionUpdateConfig.regenerable(),
        labels=[
            DataflowPipelineSourceTableLabel(IDENTITY_INGEST_PIPELINE_NAME),
            StateSpecificSourceTableLabel(state_code=tenant.to_state_code()),
        ],
        description=(
            f"Stores pre-clustering IdentityFragment output produced by the "
            f"identity ingest pipeline for tenant {tenant.value}."
        ),
    )
    for table_id, schema_fields in get_bq_schema_for_entities_module(
        identity_fragment_entities
    ).items():
        collection.add_source_table(
            table_id=table_id,
            schema_fields=schema_fields,
            clustering_fields=["identity_fragment_id"],
        )
    return collection


def build_identity_cluster_output_source_table_collection(
    tenant: Tenant,
) -> SourceTableCollection:
    """Build the source table collection for the `{tenant}_identity_cluster`
    identity ingest pipeline output dataset for a given tenant.
    """
    collection = SourceTableCollection(
        dataset_id=identity_cluster_dataset_for_tenant(tenant.value),
        update_config=SourceTableCollectionUpdateConfig.regenerable(),
        labels=[
            DataflowPipelineSourceTableLabel(IDENTITY_INGEST_PIPELINE_NAME),
            StateSpecificSourceTableLabel(state_code=tenant.to_state_code()),
        ],
        description=(
            f"Stores clustered identity output produced by the identity ingest "
            f"pipeline for tenant {tenant.value}, consumed by the Identity Service."
        ),
    )
    for table_id, schema_fields in get_bq_schema_for_entities_module(
        identity_cluster_entities
    ).items():
        collection.add_source_table(
            table_id=table_id,
            schema_fields=schema_fields,
            clustering_fields=["identity_cluster_id"],
        )
    return collection


def build_identity_rejections_source_table_collection(
    tenant: Tenant,
) -> SourceTableCollection:
    """Build the source table collection for the "{tenant}_identity_rejections"
    dataset, the pipeline-written record of what it dropped from clustering and
    why. Each run rewrites the tables, so the collection is regenerable."""
    collection = SourceTableCollection(
        dataset_id=identity_rejections_dataset_for_tenant(tenant.value),
        update_config=SourceTableCollectionUpdateConfig.regenerable(),
        labels=[
            DataflowPipelineSourceTableLabel(IDENTITY_INGEST_PIPELINE_NAME),
            StateSpecificSourceTableLabel(state_code=tenant.to_state_code()),
        ],
        description=(
            f"Stores the identity ingest pipeline's record of clusters and "
            f"external ids it dropped from clustering for tenant {tenant.value}, "
            f"for human review."
        ),
    )
    collection.add_source_table(
        table_id=REJECTED_IDENTITY_CLUSTER_TABLE_ID,
        schema_fields=rejected_identity_cluster_schema_fields(),
        description=(
            "Records each cluster the identity ingest pipeline rejected "
            "because its fragments have conflicting attributes, one "
            "self-contained row per rejection per run."
        ),
        clustering_fields=[IDENTITY_CLUSTER_ID_COL],
    )
    return collection


def build_identity_pipeline_output_source_table_collections() -> list[
    SourceTableCollection
]:
    """Builds the collection of source tables that are output by the identity
    ingest pipeline."""
    return [
        collection
        for state_code in get_direct_ingest_states_existing_in_env()
        for collection in (
            build_identity_ingest_view_results_source_table_collection(
                Tenant.from_state_code(state_code)
            ),
            build_identity_fragment_output_source_table_collection(
                Tenant.from_state_code(state_code)
            ),
            build_identity_cluster_output_source_table_collection(
                Tenant.from_state_code(state_code)
            ),
            build_identity_rejections_source_table_collection(
                Tenant.from_state_code(state_code)
            ),
        )
    ]
