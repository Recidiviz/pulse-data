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
"""Shared reporting helpers for the US_TN Tranche 1 (ingest view) TOMIS 1.0 ->
TOMIS 2.0 (MiCase) migration burndown. See TN-1939.

Computes, from the live US_TN ingest view registry, which real ingest views
exist today and where each one currently launches -- consumed by
us_tn_tomis1_burndown_markdown_generator.py.
"""
from functools import cache

from recidiviz.common.constants.states import StateCode
from recidiviz.ingest.direct.direct_ingest_regions import (
    DirectIngestRegion,
    get_direct_ingest_region,
)
from recidiviz.ingest.direct.feature_flags_registry import resolve_ingest_feature_flags
from recidiviz.ingest.direct.ingest_mappings.activity_ingest_view_manifest_compiler_delegate import (
    ActivityIngestViewManifestCompilerDelegate,
)
from recidiviz.ingest.direct.ingest_mappings.ingest_view_contents_context import (
    IngestViewContentsContext,
)
from recidiviz.ingest.direct.ingest_mappings.ingest_view_manifest_collector import (
    IngestViewManifestCollector,
)
from recidiviz.ingest.direct.types.ingest_pipeline_type import IngestPipelineType
from recidiviz.ingest.direct.views.direct_ingest_view_query_builder_collector import (
    DirectIngestViewQueryBuilderCollector,
)
from recidiviz.tools.docs.us_tn_tomis1_burndown.us_tn_tomis_migration_reporting import (
    raise_if_names_untracked_or_stale,
)
from recidiviz.utils.environment import GCP_PROJECT_PRODUCTION, GCP_PROJECT_STAGING

# US_TN's identity_views/ subdirectory exists but is empty -- every real US_TN
# ingest view today is an activity-pipeline view.
_STATE_CODE = StateCode.US_TN
_PIPELINE_TYPE = IngestPipelineType.ACTIVITY


@cache
def _us_tn_region() -> DirectIngestRegion:
    return get_direct_ingest_region(region_code=_STATE_CODE.value.lower())


@cache
def us_tn_ingest_view_query_builder_collector() -> DirectIngestViewQueryBuilderCollector:
    """Returns the collector over US_TN's real ingest view query builder .py
    files -- the ground truth of which ingest views exist today, independent
    of any manually-maintained tracking dict.
    """
    return DirectIngestViewQueryBuilderCollector.from_state_code(
        state_code=_STATE_CODE, ingest_pipeline_type=_PIPELINE_TYPE
    )


def us_tn_real_ingest_view_names() -> frozenset[str]:
    """Returns the names of every real, currently-defined US_TN ingest view."""
    return frozenset(
        builder.ingest_view_name
        for builder in us_tn_ingest_view_query_builder_collector().get_query_builders()
    )


@cache
def us_tn_ingest_view_manifest_collector() -> IngestViewManifestCollector:
    """Returns the collector over US_TN's compiled ingest mapping manifests.
    A real ingest view (see us_tn_real_ingest_view_names) only has an entry
    here once its ingest_mappings/us_tn_<name>.yaml has been written.
    """
    region = _us_tn_region()
    return IngestViewManifestCollector(
        region=region,
        delegate=ActivityIngestViewManifestCompilerDelegate(region=region),
        ingest_pipeline_type=_PIPELINE_TYPE,
    )


def _context_local() -> IngestViewContentsContext:
    # Mirrors IngestViewContentsContext.build_for_tests, which is test_only and
    # so cannot be called from this script.
    return IngestViewContentsContext(
        is_local=True,
        is_staging=True,
        is_production=False,
        is_sandbox=False,
        state_code=_STATE_CODE,
        feature_flags=resolve_ingest_feature_flags(GCP_PROJECT_STAGING),
    )


def _context_for_project(project_id: str) -> IngestViewContentsContext:
    return IngestViewContentsContext.build_for_project(
        project_id=project_id, is_sandbox=False, state_code=_STATE_CODE
    )


def ingest_view_launch_status(view_name: str) -> dict[str, bool] | None:
    """Returns, for the given US_TN ingest view name, whether it would launch
    under: local/unit tests, staging today, and production today. Returns
    None if the view has no ingest_mappings/us_tn_<name>.yaml yet (a real
    view can exist as a query builder before its mapping is written).
    """
    manifest_collector = us_tn_ingest_view_manifest_collector()
    if view_name not in manifest_collector.ingest_view_to_manifest:
        return None

    manifest = manifest_collector.ingest_view_to_manifest[view_name]
    return {
        "runs_local": manifest.should_launch(_context_local()),
        "runs_staging": manifest.should_launch(
            _context_for_project(GCP_PROJECT_STAGING)
        ),
        "runs_production": manifest.should_launch(
            _context_for_project(GCP_PROJECT_PRODUCTION)
        ),
    }


def validate_ingest_view_migration_pairs(pairs: dict[str, str | None]) -> None:
    """Raises ValueError unless `pairs` classifies every real US_TN ingest
    view (see us_tn_ingest_view_migration_pairs.py): every real
    ingest_view_name must appear as a key or as a non-None value, and every
    key or non-None value must correspond to a real, currently-existing
    ingest view. Does not prescribe which side of the migration a given real
    view belongs on -- that judgment call is made by whoever adds the entry.
    """
    tracked_names = set(pairs) | {v for v in pairs.values() if v is not None}
    raise_if_names_untracked_or_stale(
        real_names=us_tn_real_ingest_view_names(),
        tracked_names=tracked_names,
        untracked_error_prefix=(
            "Found US_TN ingest views not tracked in "
            "US_TN_INGEST_VIEW_MIGRATION_PAIRS (us_tn_ingest_view_migration_pairs.py). "
            "Add each as a new key (if it has no TOMIS 2.0 replacement yet) or as "
            "the value of the key it replaces"
        ),
        stale_error_prefix=(
            "Found entries in US_TN_INGEST_VIEW_MIGRATION_PAIRS with no "
            "corresponding real US_TN ingest view. Remove these"
        ),
    )
