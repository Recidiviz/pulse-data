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
"""Type aliases shared across ingest pipelines (activity, identity, transforms)."""

from recidiviz.persistence.entity.generate_primary_key import PrimaryKey

# Beam does not have a standard datetime coder that it uses to decode/encode between steps
# for datetime objects, therefore we will use UTC timestamps for any keys that require
# datetime objects.
UpperBoundDate = float

ExternalIdKey = tuple[str, str]

IngestViewName = str

ExternalId = str

EntityClassName = str
EntityKey = tuple[PrimaryKey, EntityClassName]
Error = str
