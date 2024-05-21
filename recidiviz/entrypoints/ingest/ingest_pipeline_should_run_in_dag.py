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
import logging

from recidiviz.common.constants.operations.direct_ingest_instance_status import (
    DirectIngestStatus,
)
from recidiviz.common.constants.states import StateCode
from recidiviz.entrypoints.entrypoint_interface import EntrypointInterface
from recidiviz.entrypoints.entrypoint_utils import save_to_xcom
from recidiviz.ingest.direct import direct_ingest_regions
from recidiviz.ingest.direct.ingest_mappings.ingest_view_contents_context import (
    IngestViewContentsContextImpl,
)
from recidiviz.ingest.direct.ingest_mappings.ingest_view_manifest_collector import (
    IngestViewManifestCollector,
)
from recidiviz.ingest.direct.ingest_mappings.ingest_view_manifest_compiler_delegate import (
    IS_PRIMARY_INSTANCE_PROPERTY_NAME,
    IS_SECONDARY_INSTANCE_PROPERTY_NAME,
    StateSchemaIngestViewManifestCompilerDelegate,
)
from recidiviz.ingest.direct.metadata.direct_ingest_instance_status_manager import (
    DirectIngestInstanceStatusManager,
)
from recidiviz.ingest.direct.types.direct_ingest_instance import DirectIngestInstance
from recidiviz.utils import environment


# TODO(#27378): Delete this function once the legacy ingest DAG has been deleted
def _secondary_has_raw_data_changes(state_code: StateCode) -> bool:
    """
    Returns if there are raw data changes that require a reimport for the given state. If so,
    we should run the secondary ingest pipeline for the given state.
    """
    instance_status_manager = DirectIngestInstanceStatusManager(
        region_code=state_code.value.lower(),
        ingest_instance=DirectIngestInstance.SECONDARY,
    )

    return (
        instance_status_manager.get_current_status()
        != DirectIngestStatus.NO_RAW_DATA_REIMPORT_IN_PROGRESS
    )


# TODO(#27378): Delete this function once the legacy ingest DAG has been deleted
def _secondary_has_manifest_views_with_launch_env_changes(
    state_code: StateCode,
) -> bool:
    """
    Returns if there are manifest views that have launch environment changes for the given state. If so,
    we should run the secondary ingest pipeline for the given state.
    """
    region = direct_ingest_regions.get_direct_ingest_region(
        region_code=state_code.value.lower()
    )
    manifest_collector = IngestViewManifestCollector(
        region=region,
        delegate=StateSchemaIngestViewManifestCompilerDelegate(region=region),
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


# TODO(#27378): Delete this function once the legacy ingest DAG has been deleted
def _should_run_secondary_ingest_pipeline(state_code: StateCode) -> bool:
    """
    Returns whether the secondary ingest pipeline should be run for the given state.
    """
    if _secondary_has_raw_data_changes(state_code):
        return True

    return _secondary_has_manifest_views_with_launch_env_changes(state_code)


def _has_launchable_ingest_views(
    state_code: StateCode, ingest_instance: DirectIngestInstance
) -> bool:
    region = direct_ingest_regions.get_direct_ingest_region(
        region_code=state_code.value.lower()
    )
    ingest_manifest_collector = IngestViewManifestCollector(
        region=region,
        delegate=StateSchemaIngestViewManifestCompilerDelegate(region=region),
    )
    return (
        len(
            ingest_manifest_collector.launchable_ingest_views(
                ingest_instance=ingest_instance
            )
        )
        > 0
    )


# TODO(#27378): Delete this function once the legacy ingest DAG has been deleted
def legacy_ingest_pipeline_should_run_in_dag(
    state_code: StateCode, ingest_instance: DirectIngestInstance
) -> bool:
    """Returns True if we should run the ingest pipeline for this (state, instance),
    False otherwise.
    """
    if not direct_ingest_regions.get_direct_ingest_region(
        state_code.value.lower()
    ).is_ingest_launched_in_env():
        logging.info(
            "Ingest for [%s, %s] is not launched in environment [%s] - returning False",
            state_code.value,
            ingest_instance.value,
            environment.get_gcp_environment(),
        )
        return False

    if not _has_launchable_ingest_views(state_code, ingest_instance):
        logging.info(
            "No launchable views found for [%s, %s] - returning False",
            state_code.value,
            ingest_instance.value,
        )
        return False

    if (
        ingest_instance is DirectIngestInstance.SECONDARY
        and not _should_run_secondary_ingest_pipeline(state_code)
    ):
        logging.info(
            "No special gating/raw data reimport state indicates that we should run "
            "the SECONDARY [%s] pipeline - returning False",
            state_code.value,
        )
        return False

    logging.info(
        "Ingest pipeline for [%s, %s] is eligible to run - returning True",
        state_code.value,
        ingest_instance.value,
    )
    return True


# TODO(#27378): Delete this entrypoint once the legacy ingest DAG has been deleted
class LegacyIngestPipelineShouldRunInDagEntrypoint(EntrypointInterface):
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
    def run_entrypoint(args: argparse.Namespace) -> None:
        """Runs the raw data flashing check."""
        state_code = args.state_code
        ingest_instance = args.ingest_instance

        save_to_xcom(
            legacy_ingest_pipeline_should_run_in_dag(state_code, ingest_instance)
        )


def ingest_pipeline_should_run_in_dag(
    state_code: StateCode, ingest_instance: DirectIngestInstance
) -> bool:
    """Returns True if we should run the ingest pipeline for this (state, instance),
    False otherwise.
    """
    if not direct_ingest_regions.get_direct_ingest_region(
        state_code.value.lower()
    ).is_ingest_launched_in_env():
        logging.info(
            "Ingest for [%s, %s] is not launched in environment [%s] - returning False",
            state_code.value,
            ingest_instance.value,
            environment.get_gcp_environment(),
        )
        return False

    if not _has_launchable_ingest_views(state_code, ingest_instance):
        logging.info(
            "No launchable views found for [%s, %s] - returning False",
            state_code.value,
            ingest_instance.value,
        )
        return False

    logging.info(
        "Ingest pipeline for [%s, %s] is eligible to run - returning True",
        state_code.value,
        ingest_instance.value,
    )
    return True


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
    def run_entrypoint(args: argparse.Namespace) -> None:
        """Runs the raw data flashing check."""
        state_code = args.state_code
        ingest_instance = args.ingest_instance

        save_to_xcom(ingest_pipeline_should_run_in_dag(state_code, ingest_instance))
