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
inputs, tables the pipeline reads but does not write.

Input collections deliberately do not carry DataflowPipelineOutputSourceTableLabel.
That label routes a dataset through sandbox dataset creation, which is only
appropriate for datasets the pipeline writes; sandbox pipeline runs read the
deployed versions of input tables unless explicitly pointed elsewhere.
"""
from recidiviz.common.constants.tenants import Tenant
from recidiviz.ingest.direct.regions.direct_ingest_region_utils import (
    get_direct_ingest_states_existing_in_env,
)
from recidiviz.pipelines.ingest.identity.dataset_config import (
    identity_overrides_dataset_for_tenant,
)
from recidiviz.pipelines.ingest.identity.identity_cluster_override import (
    IDENTITY_CLUSTER_OVERRIDE_TABLE_ID,
    identity_cluster_override_schema_fields,
)
from recidiviz.source_tables.source_table_config import (
    SourceTableCollection,
    SourceTableCollectionUpdateConfig,
    StateSpecificSourceTableLabel,
)


def build_identity_overrides_source_table_collection(
    tenant: Tenant,
) -> SourceTableCollection:
    """Build the source table collection for the "{tenant}_identity_overrides"
    identity ingest pipeline input dataset. Its human-recorded rows must never
    be dropped by a schema update or a pipeline run, so it is protected.
    """
    collection = SourceTableCollection(
        dataset_id=identity_overrides_dataset_for_tenant(tenant.value),
        update_config=SourceTableCollectionUpdateConfig.protected(),
        labels=[
            StateSpecificSourceTableLabel(state_code=tenant.to_state_code()),
        ],
        description=(
            f"Stores reviewer-recorded overrides of the identity ingest "
            f"pipeline's clustering decisions for tenant {tenant.value}, read by "
            f"the pipeline."
        ),
    )
    collection.add_source_table(
        table_id=IDENTITY_CLUSTER_OVERRIDE_TABLE_ID,
        schema_fields=identity_cluster_override_schema_fields(),
        description=(
            "Records each reviewer decision to keep (BLESS) or exclude (EXCLUDE) "
            "a specific cluster the identity ingest pipeline would otherwise get "
            "wrong, one row per cluster."
        ),
    )
    return collection


def build_identity_pipeline_input_source_table_collections() -> list[
    SourceTableCollection
]:
    """Builds the collection of source tables that are read (but not written) by
    the identity ingest pipeline.
    """
    return [
        build_identity_overrides_source_table_collection(
            Tenant.from_state_code(state_code)
        )
        for state_code in get_direct_ingest_states_existing_in_env()
    ]
