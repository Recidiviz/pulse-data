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
"""Constants that may be referenced by multiple DAGs."""

# Task id for the task in a Dataflow pipeline group that builds the parameters for the
# flex template.
CREATE_FLEX_TEMPLATE_TASK_ID = "create_flex_template"

# Task id for the task in a Dataflow pipeline group that runs a dataflow pipeline.
DATAFLOW_OPERATOR_TASK_ID = "run_pipeline"

# Task id for the check that runs before ingest pipelines to make sure we aren't
# running with stale / missing raw data.
CHECK_FOR_VALID_WATERMARKS_TASK_ID = "check_for_valid_watermarks"


# Task id for the task in raw data for files that fail to register with the operations
# db -- we WANT to report these errors
RAISE_OPERATIONS_REGISTRATION_ERRORS = "raise_operations_registration_errors"


# Task id for "failure" task in the raw data import DAG whose job it is to just raise
# task failures if we encounter them during execution. For certain raw data tasks, we
# batch files together and airflow won't push to xcom for failed tasks for k8s, so we
# need to split the "failure" out from the actual execution; for these failure tasks,
# it's nice to have the failure task close to the failure location so it's easier to
# reason about what might have gone wrong.
# Instead of reporting the Airflow task failures, we'll have our file-tag level alerting
# infra that reads from the operations database report these errors.

RAISE_HEADER_VERIFICATION_ERRORS = "raise_header_verification_errors"
RAISE_FILE_CHUNKING_ERRORS = "raise_file_chunking_errors"
RAISE_CHUNK_NORMALIZATION_ERRORS = "raise_chunk_normalization_errors"
RAISE_LOAD_PREP_ERRORS = "raise_load_prep_errors"
RAISE_APPEND_ERRORS = "raise_append_errors"

RAW_DATA_TASKS_ALLOWED_TO_FAIL: set[str] = {
    RAISE_HEADER_VERIFICATION_ERRORS,
    RAISE_FILE_CHUNKING_ERRORS,
    RAISE_CHUNK_NORMALIZATION_ERRORS,
    RAISE_LOAD_PREP_ERRORS,
    RAISE_APPEND_ERRORS,
}
