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
"""Implements an abstraction around Segment's client to make it easier for us
to use in development and tests."""
from typing import Any, Dict, Optional

from analytics import Client

from recidiviz.utils.environment import in_gcp


class SegmentClient:
    """Implements base Segment client that can be extended for domain-specific
    use-cases."""

    def __init__(self, write_key: str):
        is_local = not in_gcp()

        # When `send` is set to False, we do not send any logs to Segment.
        # We also set `debug` to True locally for more logging during development.
        self.client = Client(
            write_key,
            send=(not is_local),
            debug=is_local,
        )

    def track(
        self, user_id: Optional[str], event_name: str, metadata: Dict[str, Any]
    ) -> None:
        if user_id:
            self.client.track(user_id, event_name, metadata)
