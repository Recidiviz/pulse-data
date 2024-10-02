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
"""Sequencing tasks for the raw data import dag."""
from typing import List

from airflow.decorators import task


@task.short_circuit(ignore_downstream_trigger_rules=False)
def has_files_to_import(serialized_bq_metadata: List[str] | None) -> bool:
    """If we find no files to import, let's explicitly skip the import steps of the DAG
    and skip right to clean up and storage.
    """
    return serialized_bq_metadata is not None and len(serialized_bq_metadata) != 0
