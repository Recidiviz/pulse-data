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
"""DAG configuration to run raw data imports"""

from airflow.decorators import dag
from airflow.utils.task_group import TaskGroup

from recidiviz.airflow.dags.monitoring.dag_registry import get_raw_data_import_dag_id
from recidiviz.airflow.dags.raw_data.initialize_raw_data_dag_group import (
    initialize_raw_data_dag_group,
)
from recidiviz.airflow.dags.raw_data.raw_data_branching import (
    create_raw_data_branch_map,
    get_raw_data_branch_filter,
    get_raw_data_import_branch_key,
)
from recidiviz.airflow.dags.utils.branching_by_key import create_branching_by_key
from recidiviz.airflow.dags.utils.default_args import DEFAULT_ARGS
from recidiviz.airflow.dags.utils.environment import get_project_id
from recidiviz.common.constants.states import StateCode
from recidiviz.ingest.direct.types.direct_ingest_instance import DirectIngestInstance

# Need a disable pointless statement because Python views the chaining operator ('>>') as a "pointless" statement
# pylint: disable=W0104 pointless-statement

# Need a "disable expression-not-assigned" because the chaining ('>>') doesn't need expressions to be assigned
# pylint: disable=W0106 expression-not-assigned


def create_single_state_code_ingest_instance_raw_data_import_branch(
    state_code: StateCode,
    raw_data_instance: DirectIngestInstance,
) -> TaskGroup:
    """Given a |state_code| and |raw_data_instance|, will create a task group that
    executes the necessary steps to import all relevant files in the ingest bucket into
    BigQuery.
    """
    with TaskGroup(
        get_raw_data_import_branch_key(state_code.value, raw_data_instance.value)
    ) as raw_data_branch:
        # --- step 1: file discovery & registration ------------------------------------
        # inputs: (state_code, raw_data_instance)
        # execution layer: k8s
        # outputs: [ ImportReadyOriginalFile ], [ RequiresPreImportNormalizationFile ], [ RequiresCleanupFile ]

        # ... code will go here ...

        # ------------------------------------------------------------------------------

        # --- step 2: pre-import normalization -----------------------------------------
        # inputs: [ RequiresNormalizationFile ]
        # execution layer: k8s
        # outputs: [ ImportReadyNormalizedFile ]

        # ... code will go here ...

        # ------------------------------------------------------------------------------

        # --- step 3: big-query upload -------------------------------------------------
        # inputs: [ *[ ImportReadyOriginalFile ], *[ ImportReadyNormalizedFile ] ]
        # execution layer: k8s
        # outputs: [ ImportSessionInfo ]

        # ... code will go here ...

        # ------------------------------------------------------------------------------

        # --- step 4: cleanup and storage ----------------------------------------------
        # inputs: [ ImportSessionInfo ], [ RequiresCleanupFile ]
        # execution layer: k8s
        # outputs:

        # ... code will go here ...

        # ------------------------------------------------------------------------------
        pass

    return raw_data_branch


@dag(
    dag_id=get_raw_data_import_dag_id(get_project_id()),
    default_args=DEFAULT_ARGS,
    schedule=None,
    catchup=False,
    render_template_as_native_obj=True,
)
def create_raw_data_import_dag() -> None:
    """DAG configuration to run raw data imports"""

    # --- step 0: pipeline initialization ---------------------------------------------
    # inputs: dag parameters
    # execution layer: celery
    # outputs: selects pipeline branches for each selected (state_code, ingest_instance)
    # pair.

    # branches are created for all enabled states for both primary and secondary.
    # by default, we run w/ only primary branches being selected. when state code
    # and ingest instance filters are both applied, only the specified branch will be
    # selected

    with TaskGroup(
        "state_code_ingest_instance_branching"
    ) as state_code_ingest_instance_branching:
        create_branching_by_key(
            create_raw_data_branch_map(
                create_single_state_code_ingest_instance_raw_data_import_branch
            ),
            get_raw_data_branch_filter,
        )

    initialize_raw_data_dag_group() >> state_code_ingest_instance_branching

    # ---------------------------------------------------------------------------------


raw_data_import_dag = create_raw_data_import_dag()
