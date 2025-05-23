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
"""Handles queueing of calculation dags."""
from typing import Any, Optional

from airflow.decorators import task, task_group
from airflow.models import DagRun

from recidiviz.airflow.dags.monitoring.dag_registry import (
    get_known_configuration_parameters,
)
from recidiviz.airflow.dags.operators.wait_until_can_continue_or_cancel_sensor_async import (
    WaitUntilCanContinueOrCancelSensorAsync,
)
from recidiviz.airflow.dags.utils.config_utils import (
    INGEST_INSTANCE,
    SANDBOX_PREFIX,
    STATE_CODE_FILTER,
    get_ingest_instance,
    get_sandbox_prefix,
    get_state_code_filter,
    handle_params_check,
    handle_queueing_result,
)
from recidiviz.airflow.dags.utils.environment import get_project_id
from recidiviz.airflow.dags.utils.wait_until_can_continue_or_cancel_delegates import (
    SingleIngestInstanceWaitUntilCanContinueOrCancelDelegate,
)
from recidiviz.ingest.direct.types.direct_ingest_instance import DirectIngestInstance

# Need a disable pointless statement because Python views the chaining operator ('>>') as a "pointless" statement
# pylint: disable=W0104 pointless-statement

# Need a disable expression-not-assigned because the chaining ('>>') doesn't need expressions to be assigned
# pylint: disable=W0106 expression-not-assigned


INITIALIZE_DAG_GROUP_ID = "initialize_dag"


def templated_argument_from_conf(
    conf_key: str,
    entrypoint_argument: Optional[str] = None,
    jinja_filter: Optional[str] = None,
) -> str:
    """
    Returns a Jinja-templated entrypoint argument
    example:
    {% if 'ingest_instance' in dag_run.conf %}--ingest_instance={{dag_run.conf["ingest_instance"]}}{% endif %}
    """
    if entrypoint_argument is None:
        entrypoint_argument = conf_key

    jinja_filter_str = ""
    if jinja_filter:
        jinja_filter_str = f"| {jinja_filter}"

    return (
        f"{{% if '{conf_key}' in dag_run.conf and dag_run.conf['{conf_key}'] %}}"
        f'--{entrypoint_argument}={{{{ dag_run.conf["{entrypoint_argument}"] {jinja_filter_str} }}}}'
        "{% endif %}"
    )


INGEST_INSTANCE_JINJA_ARG = templated_argument_from_conf(
    INGEST_INSTANCE, jinja_filter="upper"
)
SANDBOX_PREFIX_JINJA_ARG = templated_argument_from_conf(SANDBOX_PREFIX)
STATE_CODE_FILTER_JINJA_ARG = templated_argument_from_conf(
    STATE_CODE_FILTER, jinja_filter="upper"
)


@task
def verify_parameters(dag_run: Optional[DagRun] = None) -> bool:
    """Verifies that the required parameters are set in the dag_run configuration and
    returns additional parameters that can be referenced throughout the DAG run.
    """
    if not dag_run:
        raise ValueError(
            "Dag run not passed to task. Should be automatically set due to function "
            "being a task."
        )

    unknown_parameters = {
        parameter
        for parameter in dag_run.conf.keys()
        if parameter
        not in get_known_configuration_parameters(
            project_id=get_project_id(), dag_id=dag_run.dag_id
        )
    }

    if unknown_parameters:
        raise ValueError(
            f"Unknown configuration parameters supplied: {unknown_parameters}"
        )

    ingest_instance = get_ingest_instance(dag_run)
    if not ingest_instance:
        raise ValueError("[ingest_instance] must be set in dag_run configuration")

    if ingest_instance not in {instance.value for instance in DirectIngestInstance}:
        raise ValueError(
            f"[ingest_instance] not valid DirectIngestInstance: {ingest_instance}."
        )

    sandbox_prefix: Optional[str] = get_sandbox_prefix(dag_run)
    state_code_filter = get_state_code_filter(dag_run)
    if ingest_instance == "SECONDARY":
        if not state_code_filter:
            raise ValueError(
                "[state_code_filter] must be set in dag_run configuration for SECONDARY "
                "ingest_instance"
            )
        if not sandbox_prefix:
            raise ValueError(
                "[sandbox_prefix] must be set in dag_run configuration for SECONDARY "
                "ingest_instance"
            )

    # TODO(#25274): Remove this check once we properly implement the state_code_filter for PRIMARY
    if ingest_instance == "PRIMARY" and state_code_filter and not sandbox_prefix:
        raise ValueError(
            "[sandbox_prefix] must be set in dag_run configuration for PRIMARY "
            "ingest_instance when [state_code_filter] is set"
        )

    return True


@task_group(group_id=INITIALIZE_DAG_GROUP_ID)
def initialize_calculation_dag_group() -> Any:
    wait_to_continue_or_cancel = WaitUntilCanContinueOrCancelSensorAsync(
        delegate=SingleIngestInstanceWaitUntilCanContinueOrCancelDelegate(),
        task_id="wait_to_continue_or_cancel",
    )
    (
        handle_params_check(verify_parameters())
        >> wait_to_continue_or_cancel
        >> handle_queueing_result(wait_to_continue_or_cancel.output)
    )
