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
"""Helpers for getting information about all Airflow DAGs that are deployed in a
project.
"""
from typing import List, Set


def get_metadata_maintenance_dag_id(project_id: str) -> str:
    """Returns the id of the calculation DAG defined in metadata_maintenance_dag.py."""
    return f"{project_id}_metadata_maintenance_dag"


def get_calculation_dag_id(project_id: str) -> str:
    """Returns the id of the calculation DAG defined in calculation_dag.py."""
    return f"{project_id}_calculation_dag"


def get_monitoring_dag_id(project_id: str) -> str:
    """Returns the id of the monitoring DAG defined in monitoring_dag.py."""
    return f"{project_id}_hourly_monitoring_dag"


def get_raw_data_import_dag_id(project_id: str) -> str:
    """Returns the id of the raw data import DAG defined in raw_data_import_dag.py."""
    return f"{project_id}_raw_data_import_dag"


def get_sftp_dag_id(project: str) -> str:
    """Returns the id of the monitoring DAG defined in sftp_dag.py."""
    return f"{project}_sftp_dag"


def get_all_dag_ids(project_id: str) -> List[str]:
    """A list of all DAGs that are deployed in the given project. Each should have a
    corresponding DAG definition in a recidiviz/airflow/dags/*_dag.py file.
    """
    return [
        get_monitoring_dag_id(project_id),
        get_calculation_dag_id(project_id),
        get_sftp_dag_id(project_id),
        get_raw_data_import_dag_id(project_id),
        get_metadata_maintenance_dag_id(project_id),
    ]


def get_known_configuration_parameters(project_id: str, dag_id: str) -> Set[str]:
    """Returns the set of parameters that are valid for the DAG with the given name.

    NOTE: If a new parameter is being added, add it here and review the values returned
    for the given DAG in get_discrete_configuration_parameters() list.
    """
    if dag_id == get_calculation_dag_id(project_id):
        return set()
    if dag_id == get_monitoring_dag_id(project_id):
        return set()
    if dag_id == get_sftp_dag_id(project_id):
        return set()
    if dag_id == get_raw_data_import_dag_id(project_id):
        return {
            "ingest_instance",
            "state_code_filter",
        }
    if dag_id == get_metadata_maintenance_dag_id(project_id):
        return set()
    raise ValueError(f"Unexpected dag_id [{dag_id}]")


def get_discrete_configuration_parameters(project_id: str, dag_id: str) -> List[str]:
    """Returns a list of parameters that cause DAG Run history to partition into
    discrete sets of runs for the purpose of tracking incident resolution.

    For example, we track incidents for primary instance runs and secondary instance
    runs separately.
    """
    if dag_id == get_calculation_dag_id(project_id):
        return []
    if dag_id == get_monitoring_dag_id(project_id):
        return []
    if dag_id == get_sftp_dag_id(project_id):
        return []
    if dag_id == get_raw_data_import_dag_id(project_id):
        return ["ingest_instance"]
    if dag_id == get_metadata_maintenance_dag_id(project_id):
        return []
    raise ValueError(f"Unexpected dag_id [{dag_id}]")
