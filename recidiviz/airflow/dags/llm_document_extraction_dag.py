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
"""DAG that runs the LLM document extraction pipeline, branched per state.

By default all state branches are selected. To run a single state, pass
`state_code_filter` in the DAG run conf.
"""

from airflow.decorators import dag
from airflow.models import DagRun
from airflow.operators.empty import EmptyOperator
from airflow.utils.task_group import TaskGroup
from airflow.utils.trigger_rule import TriggerRule

from recidiviz.airflow.dags.llm_document_extraction.document_store_tasks import (
    build_document_upload_pod_arguments,
    check_has_updates,
    record_document_upload_results,
    run_document_discovery,
)
from recidiviz.airflow.dags.llm_document_extraction.initialize_llm_document_extraction_dag_group import (
    initialize_llm_document_extraction_dag_group,
)
from recidiviz.airflow.dags.monitoring.dag_registry import (
    get_llm_document_extraction_dag_id,
)
from recidiviz.airflow.dags.operators.recidiviz_kubernetes_pod_operator import (
    build_mapped_kubernetes_pod_task,
)
from recidiviz.airflow.dags.utils.branching_by_key import (
    DAGNode,
    create_branching_by_key,
)
from recidiviz.airflow.dags.utils.config_utils import get_state_code_filter
from recidiviz.airflow.dags.utils.default_args import DEFAULT_ARGS
from recidiviz.airflow.dags.utils.environment import get_project_id
from recidiviz.common.constants.states import StateCode
from recidiviz.documents.store.document_collection_config import (
    get_states_with_document_collections,
)

# pylint: disable=W0104 pointless-statement

UPLOAD_TASK_INSTANCE_COUNT = 10
LLM_DOCUMENT_EXTRACTION_BRANCHING = "llm_document_extraction_branching"
RUN_ID_FORMAT_ARG = "{{ run_id }}"


def get_llm_document_extraction_branch_key(state_code: StateCode) -> str:
    return f"{state_code.value.lower()}_llm_document_extraction_branch"


def get_llm_document_extraction_branch_filter(dag_run: DagRun) -> list[str] | None:
    """Returns the branch key for the state_code_filter on |dag_run|, or None
    if no filter is set (in which case all branches run).
    """
    selected_state_code_str = get_state_code_filter(dag_run)
    if not selected_state_code_str:
        return None
    return [get_llm_document_extraction_branch_key(StateCode(selected_state_code_str))]


# TODO(OBT-32106) Branch per document collection so extractors aren't blocked
# on uploads from unrelated collections, and to make the per-collection logic
# reusable for the downstream entity resolution upload.
def create_single_state_llm_document_extraction_branch(
    state_code: StateCode,
) -> list[DAGNode]:
    """Creates the per-state task group for the LLM document extraction pipeline."""
    with TaskGroup(get_llm_document_extraction_branch_key(state_code)) as branch:
        # --- Step 1: Document Discovery ---
        collection_results = run_document_discovery(
            state_code=state_code,
            run_id=RUN_ID_FORMAT_ARG,
        )

        # Short-circuit so build_args, upload, and record are all skipped
        # when discovery finds no collections with updates.
        proceed = check_has_updates(collection_results=collection_results)

        # --- Step 2: GCS Upload (KPO) ---
        upload_pod_arguments = build_document_upload_pod_arguments(
            state_code=state_code,
            collection_results=collection_results,
            upload_task_instance_count=UPLOAD_TASK_INSTANCE_COUNT,
        )
        upload_tasks = build_mapped_kubernetes_pod_task(
            task_id="document_upload",
            expand_arguments=upload_pod_arguments,
        )

        # ALL_DONE sequencing task downstream of upload_tasks so an upload failure does not block
        # record_results from running, but any other task failure does block record_results.
        # This allows us to record partial results when some uploads fail.
        after_upload_noop = EmptyOperator(
            task_id="after_upload_noop",
            trigger_rule=TriggerRule.ALL_DONE,
        )

        # --- Step 3: Record Results ---
        record_results = record_document_upload_results(
            collection_results=collection_results,
            state_code=state_code,
            run_id=RUN_ID_FORMAT_ARG,
        )

        # proceed >> after_upload_noop is an explicit edge so the short-circuit's
        # ignore_downstream_trigger_rules=False also skips the ALL_DONE barrier
        # (it would otherwise run because skipped uploads still count as "done").
        proceed >> [upload_pod_arguments, after_upload_noop]
        upload_tasks >> after_upload_noop >> record_results
        upload_pod_arguments >> record_results

    return [branch]


def create_llm_document_extraction_branch_map() -> dict[str, list[DAGNode] | DAGNode]:
    return {
        get_llm_document_extraction_branch_key(
            state_code
        ): create_single_state_llm_document_extraction_branch(state_code)
        for state_code in get_states_with_document_collections()
    }


@dag(
    dag_id=get_llm_document_extraction_dag_id(get_project_id()),
    default_args=DEFAULT_ARGS,
    schedule=None,
    catchup=False,
    render_template_as_native_obj=True,
)
def create_llm_document_extraction_dag() -> None:
    initialize_dag = initialize_llm_document_extraction_dag_group()

    with TaskGroup(
        LLM_DOCUMENT_EXTRACTION_BRANCHING
    ) as llm_document_extraction_branching:
        create_branching_by_key(
            create_llm_document_extraction_branch_map(),
            get_llm_document_extraction_branch_filter,
        )

    initialize_dag >> llm_document_extraction_branching


# TODO(#63822) Add dag-level tests
llm_document_extraction_dag = create_llm_document_extraction_dag()
