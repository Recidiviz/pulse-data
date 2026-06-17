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

Schemas are derived from `identity_cluster_entities` via the entity-framework
`get_bq_schema_for_entities_module` helper, so adding or renaming a field on
(e.g.) `IdentityClusterName` automatically propagates to the
`identity_cluster_name` BQ table.
"""
from recidiviz.ingest.direct.regions.direct_ingest_region_utils import (
    get_direct_ingest_states_existing_in_env,
)
from recidiviz.persistence.entity.entities_bq_schema import (
    get_bq_schema_for_entities_module,
)
from recidiviz.persistence.entity.identity import identity_cluster_entities
from recidiviz.pipelines.ingest.identity.dataset_config import (
    identity_cluster_dataset_for_tenant,
)
from recidiviz.pipelines.pipeline_names import IDENTITY_INGEST_PIPELINE_NAME
from recidiviz.source_tables.source_table_config import (
    DataflowPipelineSourceTableLabel,
    SourceTableCollection,
    SourceTableCollectionUpdateConfig,
)


def build_identity_cluster_output_source_table_collection(
    tenant: str,
) -> SourceTableCollection:
    """Build the source table collection for the `{tenant}_identity_cluster`
    identity ingest pipeline output dataset for a given tenant.
    """
    collection = SourceTableCollection(
        dataset_id=identity_cluster_dataset_for_tenant(tenant),
        update_config=SourceTableCollectionUpdateConfig.regenerable(),
        labels=[DataflowPipelineSourceTableLabel(IDENTITY_INGEST_PIPELINE_NAME)],
        description=(
            f"Stores tenant {tenant}'s latest identity ingest pipeline output."
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


def build_identity_pipeline_output_source_table_collections() -> list[
    SourceTableCollection
]:
    """Builds the collection of source tables that are output by the identity
    ingest pipeline."""
    # TODO(OBT-33498): Also emit a `{tenant}_identity` collection per tenant
    # (pre-clustering IdentityFragment output, for debugging).
    # TODO(OBT-33499): Also emit a `{tenant}_identity_ingest_view_results`
    # collection per tenant (raw materialized ingest view results, for debugging).
    return [
        build_identity_cluster_output_source_table_collection(state_code.value)
        for state_code in get_direct_ingest_states_existing_in_env()
    ]
