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
# db
RAISE_OPERATIONS_REGISTRATION_ERRORS = "raise_operations_registration_errors"
