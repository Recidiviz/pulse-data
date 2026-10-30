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
"""Dataset and source table addresses for the engineering metrics views."""
from recidiviz.big_query.big_query_address import BigQueryAddress
from recidiviz.source_tables.yaml_managed.datasets import GITHUB_EVENTS_RAW_DATASET

# Views over GitHub activity that feed the engineering metrics Looker dashboards.
ENG_METRICS_DATASET: str = "eng_metrics"

# The daily archive of GitHub org webhook deliveries, one row per delivery.
GITHUB_EVENTS_ARCHIVE_ADDRESS = BigQueryAddress(
    dataset_id=GITHUB_EVENTS_RAW_DATASET, table_id="github_events_archive"
)
