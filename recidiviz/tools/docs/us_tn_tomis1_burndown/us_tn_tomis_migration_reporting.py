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
"""Shared reporting helpers for the US_TN TOMIS 1.0 -> TOMIS 2.0 (MiCase)
migration.

Computes, from the live deployed view registry, which deployed BigQuery views
would still reference legacy TOMIS 1.0 raw data if the tomis_2_0_enabled
feature flag were turned on. Consumed both by the CI-enforced burndown tests
in us_tn_tomis_migration_burndown_test.py and by the
us_tn_tomis1_burndown_markdown_generator.py reporting script.
"""
from collections import defaultdict
from functools import cache
from unittest import mock

from recidiviz.big_query.big_query_address import BigQueryAddress
from recidiviz.big_query.big_query_view import BigQueryView, BigQueryViewBuilder
from recidiviz.big_query.big_query_view_utils import build_views_to_update
from recidiviz.calculator.query.state.views.us_tn_tomis_1_0_analog_views.view_config import (
    US_TN_TOMIS_1_0_ANALOG_VIEW_BUILDERS,
)
from recidiviz.ingest.direct.regions.us_tn.us_tn_tomis_migration_file_tags import (
    legacy_tomis_deprecated_addresses,
)
from recidiviz.utils.metadata import local_project_id_override
from recidiviz.view_registry.deployed_view_graphs import (
    builders_for_all_view_graphs_across_projects,
)


@cache
def _deployed_views_with_tomis_2_0_enabled(project_id: str) -> tuple[BigQueryView, ...]:
    """Returns all deployed BigQuery views for the given project as they would
    build if tomis_2_0_enabled were flipped on. Shared by every
    reference-based reporting function below (legacy_tomis_references_for_project,
    canonical_view_references_for_project): a view's own address, and which
    other views reference it, does not depend on the flag -- only which
    upstream source a flag-branching view (e.g. UsTnTomisAnalogViewBuilder)
    itself reads from does. Building this once per project and sharing it
    avoids paying for multiple full-repo view builds.

    NOTE: For the patch below to take effect, any view builder logic that gates
    its generated SQL on the flag must resolve the flag when the view is built
    (i.e. inside build(), not at module import time) and must read it via the
    feature_flags_registry module (e.g.
    `feature_flags_registry.is_tomis_2_0_enabled(...)`) rather than a name
    imported directly into the calling module.
    """
    with mock.patch(
        "recidiviz.ingest.direct.feature_flags_registry.is_tomis_2_0_enabled",
        return_value=True,
    ), local_project_id_override(project_id):
        return tuple(
            build_views_to_update(
                candidate_view_builders=builders_for_all_view_graphs_across_projects(),
                sandbox_context=None,
            )
        )


@cache
def legacy_tomis_references_for_project(
    project_id: str,
) -> dict[BigQueryAddress, frozenset[BigQueryAddress]]:
    """Returns, for each legacy TOMIS 1.0 address, the deployed views in the
    given project whose queries would reference it if the tomis_2_0_enabled
    feature flag were turned on.
    """
    deprecated_addresses = legacy_tomis_deprecated_addresses()
    references: dict[BigQueryAddress, set[BigQueryAddress]] = defaultdict(set)
    for view in _deployed_views_with_tomis_2_0_enabled(project_id):
        if view.address in deprecated_addresses:
            continue
        for parent_address in view.parent_tables:
            if parent_address in deprecated_addresses:
                references[parent_address].add(view.address)
    return {
        deprecated_address: frozenset(referencing_addresses)
        for deprecated_address, referencing_addresses in references.items()
    }


@cache
def us_tn_tomis_analog_view_builders_by_file_tag(
    project_id: str,
) -> dict[str, BigQueryViewBuilder]:
    """Returns the US_TN TOMIS 1.0 analog view builders (see
    UsTnTomisAnalogViewBuilder) deployed in the given project, keyed by the
    legacy TOMIS 1.0 file tag each one is a canonical analog of.
    """
    return {
        builder.view_id: builder
        for builder in US_TN_TOMIS_1_0_ANALOG_VIEW_BUILDERS
        if builder.should_deploy_in_project(project_id)
    }


@cache
def canonical_view_references_for_project(
    project_id: str,
) -> dict[str, frozenset[BigQueryAddress]]:
    """Returns, for each legacy TOMIS 1.0 file tag that has a canonical
    UsTnTomisAnalogViewBuilder analog, the deployed views in the given project
    that reference that analog view's address directly -- i.e. the downstream
    consumers that have already been repointed at the canonical layer for that
    tag. File tags with no analog built yet are omitted.

    Computed from the same tomis_2_0_enabled-mocked view graph as
    legacy_tomis_references_for_project (see
    _deployed_views_with_tomis_2_0_enabled), rather than a separate
    un-mocked build: the consumer -> analog edge does not depend on the
    tomis_2_0_enabled flag, only the analog -> raw data edge does, so the
    mocked graph already reflects it correctly and sharing the build avoids
    a second full-repo view build.
    """
    analog_address_to_file_tag = {
        builder.table_for_query: builder.view_id
        for builder in US_TN_TOMIS_1_0_ANALOG_VIEW_BUILDERS
        if builder.should_deploy_in_project(project_id)
    }

    references: dict[str, set[BigQueryAddress]] = defaultdict(set)
    for view in _deployed_views_with_tomis_2_0_enabled(project_id):
        for parent_address in view.parent_tables:
            if file_tag := analog_address_to_file_tag.get(parent_address):
                references[file_tag].add(view.address)
    return {
        file_tag: frozenset(referencing_addresses)
        for file_tag, referencing_addresses in references.items()
    }


def raise_if_names_untracked_or_stale(
    *,
    real_names: frozenset[str],
    tracked_names: set[str],
    untracked_error_prefix: str,
    stale_error_prefix: str,
) -> None:
    """Raises ValueError if `tracked_names` does not exactly match
    `real_names`: either some real name has no tracked entry ("untracked"), or
    some tracked entry no longer corresponds to a real name ("stale").

    Shared by every "does this manually-maintained dict classify every real
    X" completeness check in this package (e.g.
    validate_ingest_view_migration_pairs, validate_raw_data_migration_statuses)
    -- these differ only in what "real" and "tracked" mean for that dict and
    how the error should read, which the caller supplies as message prefixes;
    the diff-and-raise mechanics are identical.
    """
    errors = []
    if untracked := sorted(real_names - tracked_names):
        errors.append(f"{untracked_error_prefix}: {untracked}")
    if stale := sorted(tracked_names - real_names):
        errors.append(f"{stale_error_prefix}: {stale}")
    if errors:
        raise ValueError("\n\n".join(errors))


if __name__ == "__main__":
    print(canonical_view_references_for_project("recidiviz-staging"))
