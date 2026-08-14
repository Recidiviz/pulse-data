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
"""Entrypoint that reconciles the ``protection`` Resource Manager tag onto every BigQuery
dataset in the source-table protect set, so the catastrophic-delete deny policy (see
recidiviz/tools/deploy/deletion_protection/) protects persisted data while leaving the churned
view layer and sandboxes untouched.

Design:
  - The protect set is the in-code source-table registry MINUS the ``.regenerable()`` tier
    (rebuildable from another source, so safe to delete) -- i.e. the ``.protected()`` (raw
    data, irreplaceable) and ``.externally_managed()`` tiers -- plus a small allowlist of
    platform data managed outside the registry.
  - Add-only: this job NEVER removes the tag and NEVER deletes anything. Over-protecting a
    dataset is harmless; under-protecting is the catastrophe we are guarding against. Drift
    (tagged but no longer in the protect set) is logged for a human, never auto-corrected.
  - Idempotent, with a ``--dry-run`` that prints exactly which datasets would be tagged.

Intended to run as a per-project deploy step under the deploy service account, right after
the source-table update, so the tags track the registry automatically.
"""
import argparse
import logging
from concurrent import futures

from recidiviz.big_query.big_query_client import (
    BQ_CLIENT_MAX_POOL_SIZE,
    BigQueryClientImpl,
)
from recidiviz.entrypoints.entrypoint_interface import EntrypointInterface
from recidiviz.source_tables.collect_all_source_table_configs import (
    build_source_table_repository_for_collected_schemata,
)
from recidiviz.source_tables.source_table_config import (
    SourceTableCollectionUpdateConfig,
)
from recidiviz.utils import metadata

# The org-scoped Resource Manager tag created by the deletion_protection guardrail. BigQuery
# ``resource_tags`` keys are namespaced as "<org_id>/<tag_key_short_name>".
RECIDIVIZ_ORG_ID = "448885369991"
PROTECTION_TAG_KEY = f"{RECIDIVIZ_ORG_ID}/protection"
PROTECTION_TAG_VALUE = "managed-data"

# Datasets holding platform-managed data created OUTSIDE the source-table registry (e.g. the
# CloudSQL->BQ refresh mirror, ops logging). They still hold real data and must be protected.
# TODO(AUR-1): finalize this list (the audit found ~20/project) and, preferably, bring
# these collections into the source-table registry so this allowlist can be deleted.
INFRA_PROTECT_ALLOWLIST: set[str] = {
    "cloud_sql_to_bq_refresh",
    "aurora_waf_logs",
    "cloud_dlp_findings",
    "on_call_logs",
    "gcp_logs",
}


class ApplyDatasetProtectionTagsEntrypoint(EntrypointInterface):
    """Reconciles the ``protection`` tag onto the protected-tier source-table datasets."""

    @staticmethod
    def get_parser() -> argparse.ArgumentParser:
        """Parses arguments for the protection-tag reconcile process."""
        parser = argparse.ArgumentParser()
        parser.add_argument("--dry-run", action="store_true")
        return parser

    @staticmethod
    def run_entrypoint(*, args: argparse.Namespace) -> None:
        apply_protection_tags(dry_run=args.dry_run)


def datasets_to_protect(project_id: str) -> set[str]:
    """Returns the set of dataset ids that must carry the protection tag.

    The protected tier is every source-table dataset EXCEPT the ``.regenerable()`` ones --
    those are rebuildable from another source, so they are safe to delete. That leaves the
    ``.protected()`` tier (raw data, irreplaceable) and the ``.externally_managed()`` tier,
    plus the infra allowlist. A dataset is protected if any of its collections is
    non-regenerable.
    """
    repository = build_source_table_repository_for_collected_schemata(
        project_id=project_id
    )
    regenerable = SourceTableCollectionUpdateConfig.regenerable()
    protected_tier = {
        collection.dataset_id
        for collection in repository.source_table_collections
        if collection.update_config != regenerable
    }
    return protected_tier | INFRA_PROTECT_ALLOWLIST


def apply_protection_tags(*, dry_run: bool) -> None:
    """Ensures every dataset in the protect set carries the protection tag. Add-only."""
    bq_client = BigQueryClientImpl()
    project_id = metadata.project_id()
    to_protect = datasets_to_protect(project_id)

    # Only tag datasets that actually exist; a registry entry may not be created yet.
    live_dataset_ids = {item.dataset_id for item in bq_client.list_datasets()}
    target_dataset_ids = sorted(to_protect & live_dataset_ids)
    not_yet_created = sorted(to_protect - live_dataset_ids)

    newly_tagged: list[str] = []
    already_tagged: list[str] = []
    with futures.ThreadPoolExecutor(
        max_workers=int(BQ_CLIENT_MAX_POOL_SIZE / 2)
    ) as executor:
        future_to_id = {
            executor.submit(
                _ensure_protection_tag,
                bq_client=bq_client,
                dataset_id=dataset_id,
                dry_run=dry_run,
            ): dataset_id
            for dataset_id in target_dataset_ids
        }
        for future in futures.as_completed(future_to_id):
            dataset_id = future_to_id[future]
            (newly_tagged if future.result() else already_tagged).append(dataset_id)

    prefix = "[DRY RUN] " if dry_run else ""
    logging.info(
        "%sProtection tags reconciled: %d newly tagged, %d already tagged, "
        "%d in registry but not yet created.",
        prefix,
        len(newly_tagged),
        len(already_tagged),
        len(not_yet_created),
    )

    _log_protection_drift(bq_client=bq_client, expected=to_protect)


def _ensure_protection_tag(
    *, bq_client: BigQueryClientImpl, dataset_id: str, dry_run: bool
) -> bool:
    """Adds the protection tag to a dataset if missing (merging, not clobbering other tags).

    Returns True if the tag was added (or would be, in a dry run), False if already present.
    """
    dataset = bq_client.get_dataset(dataset_id)
    resource_tags = dict(dataset.resource_tags or {})
    if resource_tags.get(PROTECTION_TAG_KEY) == PROTECTION_TAG_VALUE:
        return False

    if not dry_run:
        resource_tags[PROTECTION_TAG_KEY] = PROTECTION_TAG_VALUE
        dataset.resource_tags = resource_tags
        bq_client.client.update_dataset(dataset, ["resource_tags"])

    logging.info(
        "%sTagged dataset [%s] with [%s=%s]",
        "[DRY RUN] " if dry_run else "",
        dataset_id,
        PROTECTION_TAG_KEY,
        PROTECTION_TAG_VALUE,
    )
    return True


def _log_protection_drift(*, bq_client: BigQueryClientImpl, expected: set[str]) -> None:
    """Logs datasets carrying the protection tag but no longer in the protect set.

    Never auto-removes the tag -- removing protection is the dangerous direction, so drift is
    surfaced for a human to resolve.
    """
    drift = []
    for item in bq_client.list_datasets():
        if item.dataset_id in expected:
            continue
        dataset = bq_client.get_dataset(item.dataset_id)
        if (dataset.resource_tags or {}).get(
            PROTECTION_TAG_KEY
        ) == PROTECTION_TAG_VALUE:
            drift.append(item.dataset_id)

    if drift:
        logging.warning(
            "PROTECTION DRIFT: these datasets carry the protection tag but are NOT in the "
            "protect set. NOT auto-removing -- review and untag manually if intended: %s",
            sorted(drift),
        )
