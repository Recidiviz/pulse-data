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
"""Utils for working with GCP resources."""
import re


def format_resource_label(label: str) -> str:
    """Ensures that labels meet GCP requirements.

    The requirements for values are that they can contain only lowercase letters, numeric
    characters, underscores, and dashes, and have a maximum length of 63 characters:
            https://cloud.google.com/resource-manager/docs/labels-overview#requirements
    This formatting *should* be valid across all GCP services, such as compute engine
    labels and BigQuery job labels.

    This method converts the label string to lowercase, replaces any disallowed characters
    with a dash, and truncates the length to 63.
    """

    label_value = re.sub(r"[^\w_-]", "-", label.lower())
    if len(label_value) > 63:
        label_value = label_value[:63]
    return label_value


def subnetwork_path(*, project_id: str, region: str, subnetwork: str) -> str:
    """Returns a fully-qualified subnetwork string."""
    return f"projects/{project_id}/regions/{region}/subnetworks/{subnetwork}"


def network_path(*, project_id: str, network: str) -> str:
    """Returns a fully-qualified network string."""
    return f"projects/{project_id}/global/networks/{network}"
