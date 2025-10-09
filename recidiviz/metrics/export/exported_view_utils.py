# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2025 Recidiviz, Inc.
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
"""Utility functions for metric exports."""
from functools import cache

from recidiviz.big_query.big_query_address import BigQueryAddress
from recidiviz.metrics.export.export_config import VIEW_COLLECTION_EXPORT_INDEX


@cache
def get_all_metric_export_view_addresses() -> set[BigQueryAddress]:
    """Returns the addresses of all views that are exported via any metric export."""
    export_addresses = set()
    for export_config in VIEW_COLLECTION_EXPORT_INDEX.values():
        export_addresses |= {vb.address for vb in export_config.view_builders_to_export}
    return export_addresses
