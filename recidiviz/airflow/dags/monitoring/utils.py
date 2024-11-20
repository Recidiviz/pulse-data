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
"""Utils for the monitoring DAG"""
from typing import Any

from recidiviz.airflow.dags.monitoring.dag_registry import (
    get_discrete_configuration_parameters,
)
from recidiviz.airflow.dags.utils.environment import get_project_id


def filter_params_to_discrete(
    dag_config_json: dict[str, Any], dag_id: str
) -> dict[str, Any]:
    """Filters the parameter keys in |dag_config_json| down to it's DAG's known
    discrete parameters.
    """
    discrete_dag_conf_json = {
        key: value
        for key, value in dag_config_json.items()
        if key
        in get_discrete_configuration_parameters(
            project_id=get_project_id(), dag_id=dag_id
        )
    }
    return discrete_dag_conf_json
