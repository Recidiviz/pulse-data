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
"""Utilizes library functions from recidiviz.calculator.dataflow_output_table_manager.py
to update dataflow, supplemental, and state-specific schemas if necessary"""
import argparse
import functools
import logging

from recidiviz.calculator.dataflow_output_table_manager import (
    update_dataflow_metric_tables_schemas,
    update_normalized_state_schema,
    update_state_specific_normalized_state_schemas,
    update_supplemental_dataset_schemas,
)
from recidiviz.tools.utils.script_helpers import interactive_prompt_retry_on_exception
from recidiviz.utils.environment import GCP_PROJECT_PRODUCTION, GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override


def update_schemas() -> None:
    prompt_retry_on_ex = functools.partial(
        interactive_prompt_retry_on_exception,
        accepted_response_override="yes",
        exit_on_cancel=False,
    )
    prompt_retry_on_ex(
        fn=update_dataflow_metric_tables_schemas,
        input_text="update dataflow metric tables schemas raised an exception - retry?",
    )
    prompt_retry_on_ex(
        fn=update_supplemental_dataset_schemas,
        input_text="update supplemental dataset schemas raised an exception - retry?",
    )
    prompt_retry_on_ex(
        fn=update_state_specific_normalized_state_schemas,
        input_text="update state specific normalized state schemas raised an exception - retry?",
    )
    prompt_retry_on_ex(
        fn=update_normalized_state_schema,
        input_text="update normalized state schema raised an exception - retry?",
    )


if __name__ == "__main__":
    logging.getLogger().setLevel(logging.INFO)
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--project-id",
        choices=[GCP_PROJECT_STAGING, GCP_PROJECT_PRODUCTION],
        required=True,
    )
    project_id = parser.parse_args().project_id
    with local_project_id_override(project_id):
        update_schemas()
