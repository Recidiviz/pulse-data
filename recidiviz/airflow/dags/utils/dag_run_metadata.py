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
"""Utilities for snapshotting DAG run metadata (platform version, app engine
image) at the start of a DAG run."""
import logging
import os

from airflow.decorators import task
from airflow.models import DAG, DagRun
from airflow.models.variable import Variable
from airflow.operators.python import get_current_context
from airflow.utils.session import create_session

from recidiviz.airflow.dags.utils.environment import (
    RECIDIVIZ_APP_ENGINE_IMAGE,
    is_experiment_environment,
)

PLATFORM_VERSION_VARIABLE = "DATA_PLATFORM_VERSION"
APP_ENGINE_IMAGE_VARIABLE = "RECIDIVIZ_APP_ENGINE_IMAGE"

PLATFORM_VERSION_XCOM_KEY = "platform_version"
APP_ENGINE_IMAGE_XCOM_KEY = "app_engine_image"

_RECORD_DAG_RUN_METADATA_TASK_ID = "record_dag_run_metadata"


def resolve_metadata_task_id(dag: DAG) -> str:
    """Returns the fully-qualified task ID for record_dag_run_metadata,
    accounting for TaskGroup nesting (e.g. 'initialize_dag.record_dag_run_metadata')."""
    suffix = f".{_RECORD_DAG_RUN_METADATA_TASK_ID}"
    for task_id in dag.task_ids:
        if task_id == _RECORD_DAG_RUN_METADATA_TASK_ID or task_id.endswith(suffix):
            return task_id
    raise ValueError(
        f"Could not find {_RECORD_DAG_RUN_METADATA_TASK_ID} task in DAG {dag.dag_id}"
    )


def _get_current_platform_version() -> str:
    """Returns the current DATA_PLATFORM_VERSION Airflow Variable. This value
    may change mid-DAG run if a deploy lands; prefer reading the XCom value
    pushed by record_dag_run_metadata for version-pinned contexts.
    """
    return Variable.get(PLATFORM_VERSION_VARIABLE, default_var="unknown")


def _get_current_app_engine_image() -> str | None:
    """Returns the current app engine Docker image reference. In experiment
    environments, reads from the OS environment variable; otherwise reads the
    RECIDIVIZ_APP_ENGINE_IMAGE Airflow Variable. This value may change mid-DAG
    run if a deploy lands; prefer reading the XCom value pushed by
    record_dag_run_metadata for version-pinned contexts.
    """
    if is_experiment_environment():
        if not (image_name := os.environ.get(RECIDIVIZ_APP_ENGINE_IMAGE)):
            raise ValueError(
                f"environment variable {RECIDIVIZ_APP_ENGINE_IMAGE} not set."
            )
        return image_name

    return Variable.get(APP_ENGINE_IMAGE_VARIABLE, default_var=None)


@task(task_id=_RECORD_DAG_RUN_METADATA_TASK_ID)
def record_dag_run_metadata() -> None:
    """Snapshots the platform version and app engine image at the start of a
    DAG run. Sets DagRun.note for UI visibility and pushes both values to XCom
    so downstream tasks (especially RecidivizKubernetesPodOperator) use a
    consistent version throughout the run.
    """
    context = get_current_context()
    dag_run = context["dag_run"]
    ti = context["ti"]

    version = _get_current_platform_version()
    image = _get_current_app_engine_image()

    logging.info("Running on platform version: %s", version)
    logging.info("App engine image: %s", image)

    with create_session() as session:
        persisted_dag_run = (
            session.query(DagRun)
            .filter(
                DagRun.dag_id == dag_run.dag_id,
                DagRun.run_id == dag_run.run_id,
            )
            .one()
        )
        persisted_dag_run.note = f"Platform version: {version}"

    ti.xcom_push(key=PLATFORM_VERSION_XCOM_KEY, value=version)
    ti.xcom_push(key=APP_ENGINE_IMAGE_XCOM_KEY, value=image)
