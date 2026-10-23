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
"""DAG that runs the LLM document extraction pipeline.

By default every (state, document collection) upload branch launched in this project
runs, followed by every launched extractor collection's extraction. To target a single
state, pass `state_code_filter` in the DAG run conf; to additionally target a single
document collection's upload, pass `document_collection_name_filter` (which requires
`state_code_filter` to also be set, since document collection names are only unique within
a state). The filters scope the upload branching only; extraction always runs for every
launched extractor collection in the targeted state(s).
"""

from typing import Callable

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
from recidiviz.airflow.dags.llm_document_extraction.extraction_tasks import (
    check_has_job,
    identify_job_documents_task,
    sync_extractor_metadata_task,
)
from recidiviz.airflow.dags.llm_document_extraction.initialize_llm_document_extraction_dag_group import (
    initialize_llm_document_extraction_dag_group,
)
from recidiviz.airflow.dags.monitoring.dag_registry import (
    get_llm_document_extraction_dag_id,
)
from recidiviz.airflow.dags.operators.recidiviz_kubernetes_pod_operator import (
    build_kubernetes_pod_task,
    build_mapped_kubernetes_pod_task,
)
from recidiviz.airflow.dags.utils.branching_by_key import (
    DAGNode,
    create_branching_by_key,
    select_all_branches,
)
from recidiviz.airflow.dags.utils.config_utils import (
    get_document_collection_name_filter,
    get_state_code_filter,
)
from recidiviz.airflow.dags.utils.constants import (
    AFTER_UPLOAD_NOOP_TASK_ID,
    DOCUMENT_UPLOAD_TASK_ID,
    RUN_EXTRACTION_TASK_ID,
    UPDATE_FIRST_ORDER_VIEWS_TASK_ID,
)
from recidiviz.airflow.dags.utils.default_args import DEFAULT_ARGS
from recidiviz.airflow.dags.utils.environment import get_project_id
from recidiviz.airflow.dags.utils.source_table_tasks import (
    execute_update_big_query_table_schemata,
    execute_validate_source_table_datasets,
)
from recidiviz.common.constants.states import StateCode
from recidiviz.documents.extraction.llm_extraction_view_graph_stage import (
    LLMExtractionViewGraphStage,
)
from recidiviz.documents.extraction.llm_extractor_config_collectors import (
    load_launched_first_order_llm_extractor_configs,
)
from recidiviz.documents.store.document_collection_config import (
    get_states_with_launched_document_collections,
    load_launched_document_collection_configs,
)
from recidiviz.persistence.database.schema_type import SchemaType

# pylint: disable=W0104 pointless-statement

UPLOAD_TASK_INSTANCE_COUNT = 10
# Task group ids are deliberately short: at render time, Airflow concatenates
# the full task group prefix onto the already-prefixed task_id of a mapped
# operator (see MappedOperator.unmap / BaseOperator.__init__), so the
# validated string is roughly double the nominal depth. The 250-char
# validate_key cap is easy to hit with long, descriptive group names.
LLM_DOCUMENT_EXTRACTION_BRANCHING = "extraction"
DOCUMENT_COLLECTIONS_BRANCHING = "document_collections"
EXTRACTOR_COLLECTIONS_BRANCHING = "extractor_collections"
RUN_ID_FORMAT_ARG = "{{ run_id }}"


def get_llm_document_extraction_branch_key(state_code: StateCode) -> str:
    return state_code.value.lower()


def get_document_collection_branch_key(document_collection_name: str) -> str:
    return document_collection_name


def get_llm_document_extraction_branch_filter(dag_run: DagRun) -> list[str] | None:
    """Returns the state-level branch keys to run, or None to run all states."""
    selected_state_code_str = get_state_code_filter(dag_run)
    if not selected_state_code_str:
        return None
    return [get_llm_document_extraction_branch_key(StateCode(selected_state_code_str))]


def make_document_collection_branch_filter(
    state_code: StateCode, launched_document_collection_names: set[str]
) -> Callable[[DagRun], list[str] | None]:
    """Returns a branch filter scoped to |state_code|'s launched document collections.
    If document_collection_name_filter is set, selects only its branch; raises if the
    filter names a document collection not launched for this state in this project."""

    def document_collection_branch_filter(dag_run: DagRun) -> list[str] | None:
        selected_document_collection_name = get_document_collection_name_filter(dag_run)
        if not selected_document_collection_name:
            return None
        if selected_document_collection_name not in launched_document_collection_names:
            raise ValueError(
                f"document_collection_name_filter [{selected_document_collection_name}] does not "
                f"match any document collection launched for state [{state_code.value}] in this "
                f"project. Launched document collections: {sorted(launched_document_collection_names)}"
            )
        return [get_document_collection_branch_key(selected_document_collection_name)]

    return document_collection_branch_filter


def create_single_document_collection_branch(
    state_code: StateCode,
    document_collection_name: str,
) -> list[DAGNode]:
    """Creates the per-document-collection task group."""
    with TaskGroup(
        get_document_collection_branch_key(document_collection_name)
    ) as branch:
        # --- Step 1: Document Discovery ---
        collection_result = run_document_discovery(
            state_code=state_code,
            collection_name=document_collection_name,
            run_id=RUN_ID_FORMAT_ARG,
        )

        # Short-circuit so build_args, upload, and record are all skipped
        # when discovery found no new metadata updates.
        proceed = check_has_updates(collection_result=collection_result)

        # --- Step 2: GCS Upload (KPO) ---
        upload_pod_arguments = build_document_upload_pod_arguments(
            state_code=state_code,
            collection_result=collection_result,
            upload_task_instance_count=UPLOAD_TASK_INSTANCE_COUNT,
        )
        upload_tasks = build_mapped_kubernetes_pod_task(
            task_id=DOCUMENT_UPLOAD_TASK_ID,
            expand_arguments=upload_pod_arguments,
        )

        # ALL_DONE sequencing task downstream of upload_tasks so an upload failure does not block
        # record_results from running, but any other task failure does block record_results.
        # This allows us to record partial results when some uploads fail.
        after_upload_noop = EmptyOperator(
            task_id=AFTER_UPLOAD_NOOP_TASK_ID,
            trigger_rule=TriggerRule.ALL_DONE,
        )

        # --- Step 3: Record Results ---
        record_results = record_document_upload_results(
            collection_result=collection_result,
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


def create_single_extractor_collection_branch(
    state_code: StateCode,
    extractor_collection_name: str,
) -> list[DAGNode]:
    """Creates the per-extractor-collection task group: sync the extractor metadata,
    identify the documents for a new extraction job, run that job through the LLM, and
    deploy the first-order views over its results.

    TODO(OBT-45428): Add the entity-resolution pass after first-order views.
    """
    with TaskGroup(extractor_collection_name) as branch:
        # --- Step 1: Sync extractor metadata ---
        extractor_version_id = sync_extractor_metadata_task(
            state_code=state_code,
            extractor_collection_name=extractor_collection_name,
        )

        # --- Step 2: Identify the documents for a new job ---
        job_id = identify_job_documents_task(
            state_code=state_code,
            extractor_collection_name=extractor_collection_name,
            extractor_version_id=extractor_version_id,
        )

        # Short-circuit so extraction and the view deploy are skipped when no
        # documents needed processing (no job was generated).
        proceed = check_has_job(job_id=job_id)

        # --- Step 3: Run the extraction job through the LLM (KPO) ---
        run_extraction = build_kubernetes_pod_task(
            task_id=RUN_EXTRACTION_TASK_ID,
            arguments=[
                "--entrypoint=LLMDocumentExtractionSyncJobEntrypoint",
                f"--state_code={state_code.value}",
                f"--extractor_collection_name={extractor_collection_name}",
                f"--job_id={{{{ task_instance.xcom_pull(task_ids='{job_id.operator.task_id}') }}}}",
            ],
            cloud_sql_connections=[SchemaType.OPERATIONS],
        )

        # --- Step 4: Deploy the first-order views over the results (KPO) ---
        update_first_order_views = build_kubernetes_pod_task(
            task_id=UPDATE_FIRST_ORDER_VIEWS_TASK_ID,
            arguments=[
                "--entrypoint=UpdateLLMExtractionManagedViewGraphEntrypoint",
                f"--state_code={state_code.value}",
                f"--extractor_collection_name={extractor_collection_name}",
                f"--stage={LLMExtractionViewGraphStage.FIRST_ORDER.value}",
            ],
        )

        proceed >> run_extraction >> update_first_order_views

    return [branch]


def create_single_state_llm_document_extraction_branch(
    state_code: StateCode,
) -> list[DAGNode]:
    """Creates the per-state task group, which nests a per-document-collection document
    upload branching followed by a per-extractor-collection extraction branching."""
    with TaskGroup(get_llm_document_extraction_branch_key(state_code)) as branch:
        with TaskGroup(
            DOCUMENT_COLLECTIONS_BRANCHING
        ) as document_collections_branching:
            document_collection_names = sorted(
                load_launched_document_collection_configs(
                    state_code, project_id=get_project_id()
                )
            )
            document_collection_branch_map: dict[str, list[DAGNode] | DAGNode] = {
                get_document_collection_branch_key(
                    document_collection_name
                ): create_single_document_collection_branch(
                    state_code, document_collection_name
                )
                for document_collection_name in document_collection_names
            }
            create_branching_by_key(
                document_collection_branch_map,
                make_document_collection_branch_filter(
                    state_code, set(document_collection_names)
                ),
            )

        extractor_collection_names = sorted(
            load_launched_first_order_llm_extractor_configs(
                state_code, project_id=get_project_id()
            )
        )
        # A state can have document collections but no extractor configs yet; skip the
        # extraction branching entirely in that case, since an empty branch map would
        # produce an orphaned branch_start/branch_end pair that run for no reason.
        if extractor_collection_names:
            with TaskGroup(
                EXTRACTOR_COLLECTIONS_BRANCHING
            ) as extractor_collections_branching:
                extractor_collection_branch_map: dict[str, list[DAGNode] | DAGNode] = {
                    extractor_collection_name: create_single_extractor_collection_branch(
                        state_code, extractor_collection_name
                    )
                    for extractor_collection_name in extractor_collection_names
                }
                create_branching_by_key(
                    extractor_collection_branch_map,
                    select_all_branches,
                )

            # Extraction reads the document store the upload branching writes, so it
            # runs only after every collection's upload has finished.
            # TODO(OBT-54936): Wire each extractor branch downstream of only its
            # input_document_collection's upload branch, so document_collection_name_filter
            # correctly scopes extraction to the extractors that read the filtered
            # collection.
            document_collections_branching >> extractor_collections_branching

    return [branch]


def create_llm_document_extraction_branch_map() -> dict[str, list[DAGNode] | DAGNode]:
    return {
        get_llm_document_extraction_branch_key(
            state_code
        ): create_single_state_llm_document_extraction_branch(state_code)
        for state_code in get_states_with_launched_document_collections(
            project_id=get_project_id()
        )
    }


@dag(
    dag_id=get_llm_document_extraction_dag_id(get_project_id()),
    default_args=DEFAULT_ARGS,
    schedule=None,
    catchup=False,
    render_template_as_native_obj=True,
)
def create_llm_document_extraction_dag() -> None:
    """Creates the LLM document extraction DAG."""
    initialize_dag = initialize_llm_document_extraction_dag_group()

    update_big_query_table_schemata = execute_update_big_query_table_schemata()

    validate_source_tables = execute_validate_source_table_datasets()

    with TaskGroup(
        LLM_DOCUMENT_EXTRACTION_BRANCHING
    ) as llm_document_extraction_branching:
        create_branching_by_key(
            create_llm_document_extraction_branch_map(),
            get_llm_document_extraction_branch_filter,
        )

    (
        initialize_dag
        >> update_big_query_table_schemata
        >> llm_document_extraction_branching
    )

    # Side branch off the schemata update so a validation failure surfaces the run as
    # failed without blocking the pipelines. It runs after the schemata update so the
    # group's tables exist.
    update_big_query_table_schemata >> validate_source_tables


llm_document_extraction_dag = create_llm_document_extraction_dag()
