# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2024 Recidiviz, Inc.
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
"""BigQuery-specific resource labels"""

import attrs

from recidiviz.cloud_resources.resource_label import ResourceLabel
from recidiviz.common.constants import platform_logging_strings


@attrs.define(kw_only=True)
class BigQueryDatasetIdJobLabel(ResourceLabel):
    key: str = attrs.field(default=platform_logging_strings.DATASET_ID)


@attrs.define(kw_only=True)
class BigQueryAddressJobLabel(ResourceLabel):
    key: str = attrs.field(default=platform_logging_strings.BIG_QUERY_ADDRESS)


@attrs.define(kw_only=True)
class BigQueryTableIdJobLabel(ResourceLabel):
    key: str = attrs.field(default=platform_logging_strings.TABLE_ID)
