# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2022 Recidiviz, Inc.
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
"""Mapping between task name and completion event type"""
from recidiviz.big_query.big_query_view import SimpleBigQueryViewBuilder
from recidiviz.calculator.query.state.dataset_config import REFERENCE_VIEWS_DATASET
from recidiviz.task_eligibility.single_task_eligibility_spans_view_collector import (
    SingleTaskEligibilityBigQueryViewCollector,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

TASK_TO_COMPLETION_EVENT_VIEW_NAME = "task_to_completion_event"

TASK_TO_COMPLETION_EVENT_DESCRIPTION = (
    """Mapping between task name and completion event type"""
)

TASK_VIEW_BUILDERS = SingleTaskEligibilityBigQueryViewCollector()

TASK_TO_COMPLETION_EVENT_QUERY_TEMPLATE = "UNION DISTINCT".join(
    [
        f"""
SELECT
    "{task_builder.state_code.name}" AS state_code,
    "{task_builder.task_name}" AS task_name,
    "{task_builder.completion_event_builder.completion_event_type.name}" AS completion_event_type
"""
        for task_builder in TASK_VIEW_BUILDERS.collect_view_builders()
    ]
)

TASK_TO_COMPLETION_EVENT_VIEW_BUILDER = SimpleBigQueryViewBuilder(
    dataset_id=REFERENCE_VIEWS_DATASET,
    view_id=TASK_TO_COMPLETION_EVENT_VIEW_NAME,
    view_query_template=TASK_TO_COMPLETION_EVENT_QUERY_TEMPLATE,
    description=TASK_TO_COMPLETION_EVENT_DESCRIPTION,
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        TASK_TO_COMPLETION_EVENT_VIEW_BUILDER.build_and_print()
