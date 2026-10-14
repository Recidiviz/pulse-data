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
"""Airflow tasks for the per-extractor stages of the LLM document extraction DAG."""

from airflow.decorators import task

from recidiviz.airflow.dags.utils.cloud_sql import cloud_sql_conn_id_for_schema_type
from recidiviz.airflow.dags.utils.cloud_sql_session import (
    airflow_cloud_sql_session_for_connection,
)
from recidiviz.airflow.dags.utils.constants import (
    IDENTIFY_JOB_DOCUMENTS_TASK_ID,
    SYNC_EXTRACTOR_METADATA_TASK_ID,
)
from recidiviz.big_query.big_query_client import BigQueryClientImpl
from recidiviz.common.constants.states import StateCode
from recidiviz.documents.extraction.llm_extraction_job_manager import (
    LLMExtractionJobManager,
)
from recidiviz.documents.extraction.llm_extraction_job_tasks import (
    identify_job_documents,
    sync_extractor_metadata,
)
from recidiviz.documents.extraction.llm_extractor_config_collectors import (
    get_extractor_config,
)
from recidiviz.persistence.database.schema_type import SchemaType


@task(task_id=SYNC_EXTRACTOR_METADATA_TASK_ID)
def sync_extractor_metadata_task(
    state_code: StateCode,
    extractor_collection_name: str,
) -> str:
    """Syncs the extractor's config to Postgres and returns its extractor_version_id."""
    with airflow_cloud_sql_session_for_connection(
        cloud_sql_conn_id_for_schema_type(SchemaType.OPERATIONS)
    ) as session:
        return sync_extractor_metadata(
            config=get_extractor_config(state_code, extractor_collection_name),
            session=session,
        )


@task(task_id=IDENTIFY_JOB_DOCUMENTS_TASK_ID)
def identify_job_documents_task(
    state_code: StateCode,
    extractor_collection_name: str,
    extractor_version_id: str,
) -> str | None:
    """Generates an LLMExtractionJob for the given extractor based on the currently
    eligible documents and returns its job_id.
    """
    with airflow_cloud_sql_session_for_connection(
        cloud_sql_conn_id_for_schema_type(SchemaType.OPERATIONS)
    ) as session:
        return identify_job_documents(
            config=get_extractor_config(state_code, extractor_collection_name),
            extractor_version_id=extractor_version_id,
            job_manager=LLMExtractionJobManager(),
            bq_client=BigQueryClientImpl(),
            session=session,
        )
