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
"""Helpers for deriving the expected shape of identity ingest pipeline
output from the tenant's ingest view manifests."""
from recidiviz.ingest.direct.ingest_mappings.ingest_view_manifest_collector import (
    IngestViewManifestCollector,
)


def get_valid_external_id_types(
    *,
    ingest_manifest_collector: IngestViewManifestCollector,
    ingest_view_names: list[str],
) -> frozenset[str]:
    """Returns the set of external ID types that the given identity ingest
    views can hydrate, i.e. every id_type that may legitimately appear on an
    IdentityCluster built from those views' output."""
    return frozenset(
        id_type
        for view_name in ingest_view_names
        for id_type in ingest_manifest_collector.ingest_view_to_manifest[
            view_name
        ].root_entity_external_id_types
    )
