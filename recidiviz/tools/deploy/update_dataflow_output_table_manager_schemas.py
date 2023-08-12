# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2020 Recidiviz, Inc.
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
"""Utilizes library functions from recidiviz.pipelines.dataflow_output_table_manager.py
to update dataflow, supplemental, and state-specific schemas if necessary"""
import functools
from typing import Optional

from recidiviz.pipelines.dataflow_output_table_manager import (
    update_dataflow_metric_tables_schemas,
    update_normalized_state_schema,
    update_state_specific_ingest_state_schemas,
    update_state_specific_ingest_view_results_schemas,
    update_state_specific_normalized_state_schemas,
    update_supplemental_dataset_schemas,
)
from recidiviz.tools.utils.script_helpers import interactive_prompt_retry_on_exception


def update_dataflow_output_schemas(
    sandbox_dataset_prefix: Optional[str] = None,
) -> None:
    """Updates dataflow output schemas if necessary"""
    prompt_retry_on_ex = functools.partial(
        interactive_prompt_retry_on_exception,
        accepted_response_override="yes",
        exit_on_cancel=False,
    )
    prompt_retry_on_ex(
        fn=lambda: update_dataflow_metric_tables_schemas(
            sandbox_dataset_prefix=sandbox_dataset_prefix
        ),
        input_text="update dataflow metric tables schemas raised an exception - retry?",
    )
    prompt_retry_on_ex(
        fn=lambda: update_supplemental_dataset_schemas(
            sandbox_dataset_prefix=sandbox_dataset_prefix
        ),
        input_text="update supplemental dataset schemas raised an exception - retry?",
    )
    prompt_retry_on_ex(
        fn=lambda: update_state_specific_normalized_state_schemas(
            sandbox_dataset_prefix=sandbox_dataset_prefix
        ),
        input_text="update state specific normalized state schemas raised an exception - retry?",
    )
    prompt_retry_on_ex(
        fn=lambda: update_normalized_state_schema(
            sandbox_dataset_prefix=sandbox_dataset_prefix
        ),
        input_text="update normalized state schema raised an exception - retry?",
    )
    prompt_retry_on_ex(
        fn=lambda: update_state_specific_ingest_state_schemas(
            sandbox_dataset_prefix=sandbox_dataset_prefix
        ),
        input_text="update state specific ingest state schemas raised an exception - retry?",
    )
    prompt_retry_on_ex(
        fn=lambda: update_state_specific_ingest_view_results_schemas(
            sandbox_dataset_prefix=sandbox_dataset_prefix
        ),
        input_text="update state specific ingest view results schemas raised an exception - retry?",
    )
