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
"""Constants for keys used in platform logging operations (e.g. BQ query labeling)"""

# -- PLATFORM ENVIRONMENTS --
# this is the literal place where the call bq query is run
# - key


PLATFORM_ENVIRONMENT: str = "platform_environment"
# - values
AIRFLOW: str = "airflow"
DATAFLOW: str = "dataflow"
KUBERNETES: str = "kubernetes"
CLOUD_RUN: str = "cloud_run"
LOCAL_MACHINE: str = "local_machine"

# -- PLATFORM ORCHESTRATION MECHANISM --
# this is the process that orchestrated (scheduled / kicked off) the bq query to run
# - key
PLATFORM_ORCHESTRATION_MECHANISM: str = "platform_orchestration_mechanism"
# - values
RAW_DATA_IMPORT_DAG: str = "raw_data_import_dag"
CALCULATION_DAG: str = "calculation_dag"
LOCAL_SCRIPT: str = "local_script"

# -- RAW DATA IMPORT STEP --
# these are the conceptually distinct steps that happen during raw data import
# - key
RAW_DATA_IMPORT_STEP: str = "raw_data_import_step"
# - values
RAW_DATA_TEMP_LOAD: str = "raw_data_temp_load"
RAW_DATA_PRE_IMPORT_TRANSFORMATIONS: str = "raw_data_pre_import_transformations"
RAW_DATA_MIGRATIONS: str = "raw_data_migrations"
RAW_DATA_PRE_IMPORT_VALIDATIONS = "raw_data_pre_import_validations"
RAW_DATA_PRUNING = "raw_data_pruning"
RAW_DATA_TABLE_APPEND = "raw_data_table_append"


# -- AIRFLOW DAG PHASE --
# this is a conceptually distinct step within an airflow dag that we want to be able to track
# key
AIRFLOW_DAG_PHASE: str = "airflow_dag_phase"
# calc dag values
BQ_REFRESH: str = "bq_refresh"
VIEW_UPDATE: str = "view_update"
VALIDATIONS: str = "validations"
METRIC_EXPORTS: str = "metric_exports"


# -- OTHER COMMON KEYS --
STATE_CODE = "state_code"
INGEST_INSTANCE = "ingest_instance"

SANDBOX_PREFIX = "sandbox_prefix"

DATAFLOW_PIPELINE_TYPE: str = "dataflow_pipeline_type"
DATAFLOW_PIPELINE_NAME: str = "dataflow_pipeline_name"
DATAFLOW_PIPELINE_JOB: str = "dataflow_pipeline_job"

LOCAL_SCRIPT_NAME: str = "local_script_name"
LOCAL_SCRIPT_USER: str = "local_script_user"

RAW_DATA_FILE_TAG: str = "raw_data_file_tag"

DATASET_ID: str = "dataset_id"
TABLE_ID: str = "table_id"
BIG_QUERY_ADDRESS: str = "big_query_address"


# -- OTHER COMMON VALUES --
STATE_AGNOSTIC: str = "state_agnostic"
UNKNOWN = "unknown"
