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
"""The canonical identity of every Airflow DAG deployed in a project."""

from enum import Enum


class AirflowDag(Enum):
    """The canonical identity of every Airflow DAG deployed in a project."""

    CALCULATION = "calculation"
    LLM_DOCUMENT_EXTRACTION = "llm_document_extraction"
    IDENTITY_INGEST = "identity_ingest"
    RAW_DATA_IMPORT = "raw_data_import"
    MONITORING = "hourly_monitoring"
    METADATA_MAINTENANCE = "metadata_maintenance"
    SFTP = "sftp"

    def dag_id(self, project_id: str) -> str:
        """Returns the full, project-scoped id of this DAG."""
        return f"{project_id}_{self.value}_dag"

    @classmethod
    def from_dag_id(cls, dag_id: str, *, project_id: str) -> "AirflowDag":
        """Returns the AirflowDag whose project-scoped id is dag_id, raising if none
        matches."""
        for candidate in cls:
            if candidate.dag_id(project_id) == dag_id:
                return candidate
        raise ValueError(
            f"No AirflowDag matches dag_id [{dag_id}] for project [{project_id}]."
        )
