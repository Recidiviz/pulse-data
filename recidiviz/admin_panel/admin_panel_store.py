# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2021 Recidiviz, Inc.
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
"""An interface for data stores used to hold some sort of state for the Admin Panel."""
from abc import abstractmethod

from redis import Redis

from recidiviz.case_triage.util import get_redis_connection_options
from recidiviz.utils.secrets import get_secret


def get_admin_panel_redis() -> Redis:
    host = get_secret("admin_panel_redis_host")
    port = get_secret("admin_panel_redis_port")

    if host is None or port is None:
        raise ValueError(
            f"Unable to create admin panel cache with host: {host} port: {port}"
        )

    return Redis(host=host, port=int(port), **get_redis_connection_options())


class AdminPanelStore:
    @property
    def redis(self) -> Redis:
        return get_admin_panel_redis()

    @abstractmethod
    def hydrate_cache(self) -> None:
        """Recalculates the state of the internal store. This is called every 15 minutes inside a Cloud Run Job"""
