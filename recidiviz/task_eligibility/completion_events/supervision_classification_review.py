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
"""Defines a view that shows all classification review dates for clients in Michigan.
"""
from recidiviz.calculator.query.state.dataset_config import ANALYST_VIEWS_DATASET
from recidiviz.task_eligibility.task_completion_event_big_query_view_builder import (
    TaskCompletionEventBigQueryViewBuilder,
    TaskCompletionEventType,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

_DESCRIPTION = """Defines a view that shows all classification review dates for clients in Michigan. Classification
reviews happen every 6 months after the initial classification review and should result in a supervision level 
downgrade unless there are extenuating circumstances. 
"""
# TODO(#19779) remove reference to analyst data once state specific support is enabled
_QUERY_TEMPLATE = """
SELECT *
FROM `{project_id}.{analyst_data_dataset}.supervision_classification_review_dates_materialized`
"""

VIEW_BUILDER: TaskCompletionEventBigQueryViewBuilder = (
    TaskCompletionEventBigQueryViewBuilder(
        completion_event_type=TaskCompletionEventType.SUPERVISION_CLASSIFICATION_REVIEW,
        description=_DESCRIPTION,
        completion_event_query_template=_QUERY_TEMPLATE,
        analyst_data_dataset=ANALYST_VIEWS_DATASET,
    )
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        VIEW_BUILDER.build_and_print()
