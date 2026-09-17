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
"""Generates the content of the US_TN TOMIS 1.0 deprecation burndown doc
(docs/ingest/us_tn/us_tn_tomis1_burndown.md), tracking migration progress
from TOMIS 1.0 to TOMIS 2.0 (MiCase). See TN-1939.

This module holds no test assertions itself -- it only computes and renders
the burndown tables. The checked-in doc is regenerated from live repo/
registry state by the generate_docs_for_region pre-commit hook (see
recidiviz.tools.docs.region_documentation_generator) whenever US_TN's raw
data or either manually-maintained tracking dict in this package changes.
"""
from recidiviz.ingest.direct.regions.us_tn.us_tn_tomis_migration_file_tags import (
    LEGACY_TOMIS_FILE_TAGS,
    legacy_tomis_deprecated_addresses_by_file_tag,
)
from recidiviz.tools.docs.us_tn_tomis1_burndown.us_tn_ingest_view_migration_pairs import (
    US_TN_INGEST_VIEW_MIGRATION_PAIRS,
)
from recidiviz.tools.docs.us_tn_tomis1_burndown.us_tn_ingest_view_migration_reporting import (
    ingest_view_launch_status,
    validate_ingest_view_migration_pairs,
)
from recidiviz.tools.docs.us_tn_tomis1_burndown.us_tn_raw_data_migration_reporting import (
    validate_raw_data_migration_statuses,
)
from recidiviz.tools.docs.us_tn_tomis1_burndown.us_tn_raw_data_migration_statuses import (
    US_TN_RAW_DATA_MIGRATION_STATUSES,
)
from recidiviz.tools.docs.us_tn_tomis1_burndown.us_tn_tomis_migration_reporting import (
    canonical_view_references_for_project,
    legacy_tomis_references_for_project,
    us_tn_tomis_analog_view_builders_by_file_tag,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING


def _mark(value: bool) -> str:
    return "✅" if value else "❌"


def _raw_data_references_row(file_tag: str) -> tuple[str, bool, int, int]:
    """Returns (file_tag, analog_built, refs_on_1_0, refs_on_analog) for the
    given legacy TOMIS 1.0 file tag, computed from staging only. These are
    reference relationships between deployed views -- which views exist and
    what they read from -- not data conditions, so they are stable across the
    tomis_2_0_enabled flag and (for this reporting purpose) across projects;
    staging is used as the single representative source rather than building
    the full deployed view graph multiple times.
    """
    deprecated_addresses = legacy_tomis_deprecated_addresses_by_file_tag()[file_tag]
    analog_built = file_tag in us_tn_tomis_analog_view_builders_by_file_tag(
        GCP_PROJECT_STAGING
    )

    legacy_refs_by_address = legacy_tomis_references_for_project(GCP_PROJECT_STAGING)
    refs_on_1_0 = frozenset().union(
        *(
            legacy_refs_by_address.get(address, frozenset())
            for address in deprecated_addresses
        )
    )
    refs_on_analog = canonical_view_references_for_project(GCP_PROJECT_STAGING).get(
        file_tag, frozenset()
    )

    return (file_tag, analog_built, len(refs_on_1_0), len(refs_on_analog))


def _raw_data_references_rows() -> list[tuple[str, bool, int, int]]:
    """Returns one row per legacy TOMIS 1.0 file tag that has an analog built
    or at least one live reference to the legacy data. Tags with neither --
    no analog and nothing referencing them -- are omitted: there is nothing
    to track for them.
    """
    all_rows = (
        _raw_data_references_row(file_tag)
        for file_tag in sorted(LEGACY_TOMIS_FILE_TAGS)
    )
    return [row for row in all_rows if row[1] or row[2]]  # analog_built or refs_on_1_0


def generate_raw_data_references_table() -> str:
    """Returns the Markdown table of legacy TOMIS 1.0 raw data file tags and
    their downstream reference burndown status (Tranche 2). Computed live
    from the deployed view registry: no manually-maintained data is used
    here. Omits file tags with no analog built and no live references -- see
    _raw_data_references_rows.
    """
    lines = [
        "## Raw data references (Tranche 2)",
        "",
        "For each legacy TOMIS 1.0 file tag with an analog built or a live "
        "reference to it: whether a canonical `UsTnTomisAnalogViewBuilder` "
        "analog view has been built for it (once merged, its TOMIS 2.0 query "
        "is assumed validated against real data), how many deployed views "
        "(in staging, used as the representative project -- these are "
        "reference relationships, not data conditions, so they don't vary by "
        "project or by the `tomis_2_0_enabled` flag) would still reference "
        "the legacy TOMIS 1.0 data directly, and how many have already been "
        "repointed at the canonical analog view. File tags with neither an "
        "analog nor a live reference are omitted.",
        "",
        "| File tag | Analog built? | Refs on 1.0 | Refs on Analog |",
        "|---|---|---|---|",
    ]
    for (
        file_tag,
        analog_built,
        refs_on_1_0,
        refs_on_analog,
    ) in _raw_data_references_rows():
        lines.append(
            f"| `{file_tag}` | {_mark(analog_built)} "
            f"| {refs_on_1_0} | {refs_on_analog} |"
        )
    lines.append("")
    return "\n".join(lines)


def _ingest_view_pairing_row(tomis_1_0_name: str, tomis_2_0_name: str | None) -> str:
    """Returns a single Markdown row for the given TOMIS 1.0 -> TOMIS 2.0
    ingest view pairing, showing the TOMIS 2.0 view's launch status. Shows a
    placeholder if no TOMIS 2.0 view has been written yet, or if it exists but
    has no mapping yet.
    """
    if tomis_2_0_name is None:
        return f"| `{tomis_1_0_name}` | _(not yet written)_ | - | - | - |"

    status = ingest_view_launch_status(tomis_2_0_name)
    if status is None:
        return f"| `{tomis_1_0_name}` | `{tomis_2_0_name}` | _(no mapping yet)_ | _(no mapping yet)_ | _(no mapping yet)_ |"
    return (
        f"| `{tomis_1_0_name}` | `{tomis_2_0_name}` | {_mark(status['runs_local'])} "
        f"| {_mark(status['runs_staging'])} | {_mark(status['runs_production'])} |"
    )


def generate_ingest_view_gating_table() -> str:
    """Returns the Markdown table of US_TN ingest view migration status
    (Tranche 1): each legacy TOMIS 1.0 ingest view paired with its TOMIS 2.0
    replacement, if one has been written (see
    US_TN_INGEST_VIEW_MIGRATION_PAIRS in us_tn_ingest_view_migration_pairs.py
    -- manually maintained), and where the TOMIS 2.0 view currently launches.

    Raises ValueError if US_TN_INGEST_VIEW_MIGRATION_PAIRS does not classify
    every real US_TN ingest view -- see validate_ingest_view_migration_pairs.
    """
    validate_ingest_view_migration_pairs(US_TN_INGEST_VIEW_MIGRATION_PAIRS)

    lines = [
        "## Ingest views + mappings (Tranche 1)",
        "",
        "Each legacy TOMIS 1.0 ingest view paired with its TOMIS 2.0 "
        "replacement, if one has been written (manually tracked in "
        "us_tn_ingest_view_migration_pairs.py). Launch status for the TOMIS "
        "2.0 view is computed live from its mapping YAML's `launch_env`.",
        "",
        "| 1.0 view | 2.0 view | 2.0 runs local | 2.0 runs staging | 2.0 runs prod |",
        "|---|---|---|---|---|",
    ]
    for tomis_1_0_name in sorted(US_TN_INGEST_VIEW_MIGRATION_PAIRS):
        tomis_2_0_name = US_TN_INGEST_VIEW_MIGRATION_PAIRS[tomis_1_0_name]
        lines.append(_ingest_view_pairing_row(tomis_1_0_name, tomis_2_0_name))
    lines.append("")
    return "\n".join(lines)


def generate_raw_data_migrations_table() -> str:
    """Returns the Markdown table of US_TN raw data migration completion
    status: one row per legacy file tag with a raw_data/migrations/ module,
    and the manually-assessed status of whether its underlying data issue
    still applies to MiCase data (tracked in
    us_tn_raw_data_migration_statuses.py -- manually maintained).

    Raises ValueError if US_TN_RAW_DATA_MIGRATION_STATUSES does not classify
    every real US_TN raw data migration -- see
    validate_raw_data_migration_statuses.
    """
    validate_raw_data_migration_statuses(US_TN_RAW_DATA_MIGRATION_STATUSES)

    lines = [
        "## Raw data migrations",
        "",
        "Each US_TN raw data migration file tag and whether the "
        "data-quality issue it corrects has been confirmed to still apply to "
        "MiCase data (manually tracked in us_tn_raw_data_migration_statuses.py "
        "-- this cannot be inferred from code alone).",
        "",
        "| File tag | Completion status |",
        "|---|---|",
    ]
    for file_tag in sorted(US_TN_RAW_DATA_MIGRATION_STATUSES):
        lines.append(
            f"| `{file_tag}` | {US_TN_RAW_DATA_MIGRATION_STATUSES[file_tag]} |"
        )
    lines.append("")
    return "\n".join(lines)


def generate_burndown_markdown() -> str:
    """Returns the full content of docs/ingest/us_tn/us_tn_tomis1_burndown.md:
    the TOMIS 1.0 deprecation burndown tracking migration progress for
    TN-1939. Regenerated from live repo/registry state (plus the manually-
    maintained tracking dicts) by the generate_docs_for_region pre-commit
    hook -- do not hand-edit the checked-in Markdown file; edit the generator
    functions above or the manually-maintained dicts instead.

    Raises ValueError if either manually-maintained dict
    (US_TN_INGEST_VIEW_MIGRATION_PAIRS or US_TN_RAW_DATA_MIGRATION_STATUSES)
    no longer classifies every real US_TN ingest view / raw data migration.
    """
    return "\n".join(
        (
            "# US_TN TOMIS 1.0 deprecation burndown",
            "",
            "Auto-generated by the `generate_docs_for_region` pre-commit "
            "hook -- do not hand-edit. Tracks migration progress from TOMIS "
            "1.0 to TOMIS 2.0 (MiCase). See "
            "[TN-1939](https://linear.app/recidiviz/issue/TN-1939) and the "
            "US_TN Data Migration Plan (Notion).",
            "",
            generate_ingest_view_gating_table(),
            generate_raw_data_references_table(),
            generate_raw_data_migrations_table(),
        )
    )
