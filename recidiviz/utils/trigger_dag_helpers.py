# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2023 Recidiviz, Inc.
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
"""Helper functions for triggering DAGs."""
import json
import logging
from typing import Optional

from google.cloud.orchestration.airflow.service_v1 import (
    EnvironmentsClient,
    ExecuteAirflowCommandRequest,
)

from recidiviz.common.constants.states import StateCode
from recidiviz.ingest.direct.types.direct_ingest_instance import DirectIngestInstance
from recidiviz.utils.metadata import project_id


def trigger_dag_run(dag_id: str, conf: dict) -> None:
    client = EnvironmentsClient()
    response = client.execute_airflow_command(
        ExecuteAirflowCommandRequest(
            environment=client.environment_path(
                project=project_id(),
                location="us-central1",
                environment="orchestration-v2",
            ),
            command="dags",
            subcommand="trigger",
            parameters=[dag_id, "--conf", json.dumps(conf)],
        )
    )

    if response.error:
        raise ValueError(f"Failed to trigger DAG {dag_id}: {response.error}")


def trigger_calculation_dag() -> None:
    """Sends a message to the PubSub topic to trigger the post-deploy CloudSQL to BQ
    refresh, which then will trigger the calculation DAG on completion."""

    logging.info("Triggering the calc DAG.")
    trigger_dag_run(f"{project_id()}_calculation_dag", conf={})


def trigger_raw_data_import_dag(
    *,
    raw_data_instance: DirectIngestInstance,
    state_code_filter: Optional[StateCode],
) -> None:
    """Sends a message to the PubSub topic to trigger the raw data import DAG"""

    logging.info(
        "Triggering the raw data import DAG with instance: [%s], and state code filter: [%s]",
        raw_data_instance.value,
        state_code_filter.value if state_code_filter else None,
    )

    if raw_data_instance == DirectIngestInstance.SECONDARY and not state_code_filter:
        raise ValueError(
            "Cannot trigger a state-agnostic SECONDARY dag run; please provide a state_code_filter if you want to trigger a raw data import DAG run in SECONDARY"
        )

    trigger_dag_run(
        f"{project_id()}_raw_data_import_dag",
        conf={
            "state_code_filter": (
                state_code_filter.value if state_code_filter else None
            ),
            "ingest_instance": raw_data_instance.value,
        },
    )
