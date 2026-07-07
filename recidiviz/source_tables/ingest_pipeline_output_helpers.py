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
"""Shared helpers for building source table collections for ingest pipeline
outputs, used by both the activity and identity ingest pipeline output
collectors.
"""
from google.cloud import bigquery

from recidiviz.ingest.direct.ingest_mappings.ingest_view_manifest_collector import (
    IngestViewManifestCollector,
)
from recidiviz.pipelines.ingest.constants import INGEST_VIEW_RESULTS_SCHEMA_COLUMNS
from recidiviz.source_tables.source_table_config import (
    SourceTableCollection,
    SourceTableCollectionUpdateConfig,
    SourceTableLabel,
)


def build_ingest_view_results_source_table_collection_from_manifests(
    *,
    dataset_id: str,
    description: str,
    labels: list[SourceTableLabel],
    manifest_collector: IngestViewManifestCollector,
) -> SourceTableCollection:
    """Builds a source table collection with one table per launchable ingest
    view, where each table's schema is derived from that view's manifest input
    columns plus the shared `INGEST_VIEW_RESULTS_SCHEMA_COLUMNS`. Shared by the
    activity and identity ingest pipeline output collectors.

    We intentionally create a table for every ingest view with a defined
    manifest YAML, regardless of whether that view is gated to produce results
    in this environment. This keeps the list of expected tables stable across
    local and deployed environments.
    """
    collection = SourceTableCollection(
        dataset_id=dataset_id,
        update_config=SourceTableCollectionUpdateConfig.regenerable(),
        labels=labels,
        description=description,
    )
    for (
        ingest_view_name,
        manifest,
    ) in manifest_collector.ingest_view_to_manifest.items():
        schema = [
            bigquery.SchemaField(
                name=column_name, field_type=type_name, mode="NULLABLE"
            )
            for column_name, type_name in manifest.input_column_to_type.items()
        ] + INGEST_VIEW_RESULTS_SCHEMA_COLUMNS

        collection.add_source_table(table_id=ingest_view_name, schema_fields=schema)
    return collection
