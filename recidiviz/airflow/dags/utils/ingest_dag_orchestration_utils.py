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
"""
Helper functions for orchestrating the Ingest Airflow Dag.
"""
from datetime import datetime
from typing import List, Tuple

from recidiviz.common.constants.states import StateCode
from recidiviz.ingest.direct import direct_ingest_regions
from recidiviz.ingest.direct.gating import is_ingest_in_dataflow_enabled
from recidiviz.ingest.direct.ingest_mappings.ingest_view_manifest_collector import (
    IngestViewManifestCollector,
)
from recidiviz.ingest.direct.ingest_mappings.ingest_view_results_parser_delegate import (
    IS_PRIMARY_INSTANCE_PROPERTY_NAME,
    IS_SECONDARY_INSTANCE_PROPERTY_NAME,
    IngestViewResultsParserDelegateImpl,
)
from recidiviz.ingest.direct.regions.direct_ingest_region_utils import (
    get_direct_ingest_states_existing_in_env,
)
from recidiviz.ingest.direct.types.direct_ingest_instance import DirectIngestInstance
from recidiviz.persistence.database.schema_type import SchemaType


def should_run_secondary_ingest_pipeline(state_code: StateCode) -> bool:
    """
    Returns whether the secondary ingest pipeline should be run for the given state.
    """
    if not is_ingest_in_dataflow_enabled(state_code, DirectIngestInstance.SECONDARY):
        return False

    region = direct_ingest_regions.get_direct_ingest_region(
        region_code=state_code.value.lower()
    )
    primary_manifest_collector = IngestViewManifestCollector(
        region=region,
        delegate=IngestViewResultsParserDelegateImpl(
            region=region,
            schema_type=SchemaType.STATE,
            ingest_instance=DirectIngestInstance.PRIMARY,
            results_update_datetime=datetime.now(),
        ),
    )

    secondary_manifest_collector = IngestViewManifestCollector(
        region=region,
        delegate=IngestViewResultsParserDelegateImpl(
            region=region,
            schema_type=SchemaType.STATE,
            ingest_instance=DirectIngestInstance.SECONDARY,
            results_update_datetime=datetime.now(),
        ),
    )

    differing_launch_envs = False
    for (
        ingest_view_name,
        primary_manifest,
    ) in primary_manifest_collector.ingest_view_to_manifest.items():
        secondary_manifest = secondary_manifest_collector.ingest_view_to_manifest[
            ingest_view_name
        ]
        differing_launch_envs |= (
            primary_manifest.should_launch != secondary_manifest.should_launch
        )

    return differing_launch_envs or any(
        manifest.output.env_properties_referenced().intersection(
            {IS_PRIMARY_INSTANCE_PROPERTY_NAME, IS_SECONDARY_INSTANCE_PROPERTY_NAME}
        )
        for manifest in primary_manifest_collector.ingest_view_to_manifest.values()
    )


def has_launchable_ingest_views(
    state_code: StateCode, ingest_instance: DirectIngestInstance
) -> bool:
    region = direct_ingest_regions.get_direct_ingest_region(
        region_code=state_code.value.lower()
    )
    ingest_manifest_collector = IngestViewManifestCollector(
        region=region,
        delegate=IngestViewResultsParserDelegateImpl(
            region=region,
            schema_type=SchemaType.STATE,
            ingest_instance=ingest_instance,
            results_update_datetime=datetime.now(),
        ),
    )
    return len(ingest_manifest_collector.launchable_ingest_views()) > 0


def _should_enable_state_and_instance(
    state_code: StateCode, ingest_instance: DirectIngestInstance
) -> bool:
    return (
        direct_ingest_regions.get_direct_ingest_region(
            state_code.value.lower()
        ).is_ingest_launched_in_env()
        and is_ingest_in_dataflow_enabled(state_code, ingest_instance)
        and has_launchable_ingest_views(state_code, ingest_instance)
        and (
            ingest_instance is DirectIngestInstance.PRIMARY
            or should_run_secondary_ingest_pipeline(state_code)
        )
    )


def get_ingest_pipeline_enabled_state_and_instance_pairs() -> List[
    Tuple[StateCode, DirectIngestInstance]
]:
    """
    Returns a list of all state and ingest instance pairs that the ingest pipeline should be run for.
    """
    states_and_instances: List[Tuple[StateCode, DirectIngestInstance]] = []
    for state in get_direct_ingest_states_existing_in_env():
        for instance in DirectIngestInstance:
            if _should_enable_state_and_instance(state, instance):
                states_and_instances.append((state, instance))

    return states_and_instances
