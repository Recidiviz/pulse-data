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
""" Entrypoint for the ingest pipeline should run in dag check. """
import argparse

from recidiviz.common.constants.states import StateCode
from recidiviz.entrypoints.entrypoint_interface import EntrypointInterface
from recidiviz.ingest.direct import direct_ingest_regions
from recidiviz.ingest.direct.gating import ingest_pipeline_can_run_in_dag
from recidiviz.ingest.direct.ingest_mappings.ingest_view_contents_context import (
    IngestViewContentsContextImpl,
)
from recidiviz.ingest.direct.ingest_mappings.ingest_view_manifest_collector import (
    IngestViewManifestCollector,
)
from recidiviz.ingest.direct.ingest_mappings.ingest_view_manifest_compiler_delegate import (
    IS_PRIMARY_INSTANCE_PROPERTY_NAME,
    IS_SECONDARY_INSTANCE_PROPERTY_NAME,
    IngestViewManifestCompilerDelegateImpl,
)
from recidiviz.ingest.direct.types.direct_ingest_instance import DirectIngestInstance
from recidiviz.persistence.database.schema_type import SchemaType


def _should_run_secondary_ingest_pipeline(state_code: StateCode) -> bool:
    """
    Returns whether the secondary ingest pipeline should be run for the given state.
    """
    if not ingest_pipeline_can_run_in_dag(state_code, DirectIngestInstance.SECONDARY):
        return False

    region = direct_ingest_regions.get_direct_ingest_region(
        region_code=state_code.value.lower()
    )
    manifest_collector = IngestViewManifestCollector(
        region=region,
        delegate=IngestViewManifestCompilerDelegateImpl(
            region=region, schema_type=SchemaType.STATE
        ),
    )

    differing_launch_envs = False
    for manifest in manifest_collector.ingest_view_to_manifest.values():
        differing_launch_envs |= manifest.should_launch(
            IngestViewContentsContextImpl(ingest_instance=DirectIngestInstance.PRIMARY)
        ) != manifest.should_launch(
            IngestViewContentsContextImpl(
                ingest_instance=DirectIngestInstance.SECONDARY
            )
        )

    return differing_launch_envs or any(
        manifest.output.env_properties_referenced().intersection(
            {IS_PRIMARY_INSTANCE_PROPERTY_NAME, IS_SECONDARY_INSTANCE_PROPERTY_NAME}
        )
        for manifest in manifest_collector.ingest_view_to_manifest.values()
    )


def _has_launchable_ingest_views(
    state_code: StateCode, ingest_instance: DirectIngestInstance
) -> bool:
    region = direct_ingest_regions.get_direct_ingest_region(
        region_code=state_code.value.lower()
    )
    ingest_manifest_collector = IngestViewManifestCollector(
        region=region,
        delegate=IngestViewManifestCompilerDelegateImpl(
            region=region, schema_type=SchemaType.STATE
        ),
    )
    return (
        len(
            ingest_manifest_collector.launchable_ingest_views(
                ingest_instance=ingest_instance
            )
        )
        > 0
    )


def ingest_pipeline_should_run_in_dag(
    state_code: StateCode, ingest_instance: DirectIngestInstance
) -> bool:
    return (
        direct_ingest_regions.get_direct_ingest_region(
            state_code.value.lower()
        ).is_ingest_launched_in_env()
        and ingest_pipeline_can_run_in_dag(state_code, ingest_instance)
        and _has_launchable_ingest_views(state_code, ingest_instance)
        and (
            ingest_instance is DirectIngestInstance.PRIMARY
            or _should_run_secondary_ingest_pipeline(state_code)
        )
    )


class IngestPipelineShouldRunInDagEntrypoint(EntrypointInterface):
    """Entrypoint for the ingest pipeline should run in dag check."""

    @staticmethod
    def get_parser() -> argparse.ArgumentParser:
        """Parses arguments for the ingest pipeline should run in dag check."""
        parser = argparse.ArgumentParser()

        parser.add_argument(
            "--state_code",
            help="The state code for which the ingest pipeline enabled status needs to be checked",
            type=StateCode,
            choices=list(StateCode),
            required=True,
        )

        parser.add_argument(
            "--ingest_instance",
            help="The ingest instance for which the ingest pipeline enabled status needs to be checked",
            type=DirectIngestInstance,
            choices=list(DirectIngestInstance),
            required=True,
        )

        return parser

    @staticmethod
    def run_entrypoint(args: argparse.Namespace) -> bool:
        """Runs the raw data flashing check."""
        state_code = args.state_code
        ingest_instance = args.ingest_instance

        return ingest_pipeline_should_run_in_dag(state_code, ingest_instance)
