# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2019 Recidiviz, Inc.
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
"""Accessor for a global instance of the sqladmin client."""

import google
from googleapiclient import discovery

_client = None


def sqladmin_client() -> discovery.Resource:
    global _client
    if not _client:
        credentials, _ = google.auth.default()
        _client = discovery.build("sqladmin", "v1beta4", credentials=credentials)
    return _client
