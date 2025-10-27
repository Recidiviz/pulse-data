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
"""PagerDuty alert data wrapper with cached field path lookups."""
from typing import Any

import attr


@attr.define
class PagerDutyAlert:
    """Wrapper for alert data with cached field path lookups.

    Pre-computes all possible field paths on initialization for fast lookup.
    """

    raw_data: dict[str, Any]
    _field_paths: dict[str, Any] = attr.field(init=False, factory=dict)

    def __attrs_post_init__(self) -> None:
        """Build cache of all field paths on initialization."""
        self._build_field_path_cache(self.raw_data, prefix="")

    def _build_field_path_cache(
        self, data: Any, prefix: str, parent_path: str = ""
    ) -> None:
        """Recursively build mapping of field paths to values.

        Args:
            data: Current data to process (dict, list, or primitive)
            prefix: Current field path prefix
            parent_path: Parent path for building full path
        """
        if isinstance(data, dict):
            for key, value in data.items():
                # Build the full path
                if parent_path:
                    full_path = f"{parent_path}.{key}"
                else:
                    full_path = key

                # Store this path
                self._field_paths[full_path] = value

                # Recurse into nested structures
                if isinstance(value, (dict, list)):
                    self._build_field_path_cache(value, prefix, full_path)

        elif isinstance(data, list):
            # For lists, we could index them, but for now we just store the list itself
            # at the current path (already stored by parent)
            for idx, item in enumerate(data):
                if isinstance(item, (dict, list)):
                    # Store indexed access like "items[0].field"
                    indexed_path = f"{parent_path}[{idx}]"
                    self._field_paths[indexed_path] = item
                    self._build_field_path_cache(item, prefix, indexed_path)

    def get(self, field_path: str) -> Any | None:
        """Get value for a field path, or None if not found.

        Args:
            field_path: Dot-separated field path (e.g., "incident.policy_name")

        Returns:
            Value at the field path, or None if not found
        """
        return self._field_paths.get(field_path)

    def has_field(self, field_path: str) -> bool:
        """Check if a field path exists.

        Args:
            field_path: Dot-separated field path

        Returns:
            True if the field path exists
        """
        return field_path in self._field_paths

    def get_available_fields(self) -> list[str]:
        """Get all available field paths for debugging/error messages.

        Returns:
            Sorted list of all available field paths
        """
        return sorted(self._field_paths.keys())

    # Convenience methods for common incident properties

    def get_incident_id(self) -> str:
        """Get the incident ID from the alert.

        Returns:
            Incident ID or empty string if not found
        """
        return self.get("incident.incident_id") or ""

    def get_policy_name(self) -> str:
        """Get the policy name from the alert.

        Returns:
            Policy name or empty string if not found
        """
        return self.get("incident.policy_name") or ""

    def get_incident_url(self) -> str | None:
        """Get the Cloud Console URL for the incident.

        Returns:
            URL or None if not found
        """
        return self.get("incident.url")

    def get_dedup_key(self) -> str:
        """Generate a deduplication key for PagerDuty.

        Returns:
            Dedup key in format "policy_name:incident_id" or just "policy_name"
        """
        incident_id = self.get_incident_id()
        policy_name = self.get_policy_name()
        return f"{policy_name}:{incident_id}" if incident_id else policy_name
