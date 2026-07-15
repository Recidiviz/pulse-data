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
"""Helpers for getting information about Google Compute Engine service accounts."""
import os

import yaml

from recidiviz.utils import config, metadata

_GCP_PROJECT_NUMBERS_YAML_PATH = os.path.join(
    os.path.dirname(config.__file__), "gcp_project_numbers.yaml"
)

_project_numbers_by_project_id: dict[str, str] | None = None


def _get_project_numbers_by_project_id() -> dict[str, str]:
    """Returns the project id -> project number mapping loaded from
    gcp_project_numbers.yaml, reading (and caching) it on first access.
    """
    global _project_numbers_by_project_id
    if _project_numbers_by_project_id is None:
        with open(_GCP_PROJECT_NUMBERS_YAML_PATH, encoding="utf-8") as f:
            _project_numbers_by_project_id = yaml.safe_load(f)
    return _project_numbers_by_project_id


def get_default_compute_engine_service_account_email(
    project_id: str | None = None,
) -> str:
    """Returns the email of the default Compute Engine service account for the
    given project (or the current project if none is provided).
    """
    project_id = project_id or metadata.project_id()

    project_numbers = _get_project_numbers_by_project_id()
    if project_id not in project_numbers:
        raise ValueError(
            f"No project number configured for project [{project_id}] in "
            f"[{_GCP_PROJECT_NUMBERS_YAML_PATH}]"
        )
    return f"{project_numbers[project_id]}-compute@developer.gserviceaccount.com"
