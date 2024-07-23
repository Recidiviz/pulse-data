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
"""Reference view that contains metadata about each completion event type"""
from typing import List

from recidiviz.big_query.big_query_view import SimpleBigQueryViewBuilder
from recidiviz.calculator.query.state.dataset_config import REFERENCE_VIEWS_DATASET
from recidiviz.task_eligibility.task_completion_event_big_query_view_builder import (
    TaskCompletionEventType,
)

COMPLETION_EVENT_TYPE_METADATA_VIEW_NAME = "completion_event_type_metadata"

COMPLETION_EVENT_TYPE_METADATA_VIEW_DESCRIPTION = (
    "Reference view that contains metadata about each completion event type"
)


def get_completion_event_metadata_view_builder() -> SimpleBigQueryViewBuilder:
    """Returns a view builder that maps every completion event type to its metadata attributes"""
    all_completion_event_query_fragments: List[str] = []
    for completion_event_type in TaskCompletionEventType:
        query_fragment = f"""
SELECT
    "{completion_event_type.value}" AS completion_event_type,
    "{completion_event_type.system_type.value}" AS system_type,
"""
        all_completion_event_query_fragments.append(query_fragment)

    query_template = "\nUNION ALL\n".join(all_completion_event_query_fragments)
    return SimpleBigQueryViewBuilder(
        dataset_id=REFERENCE_VIEWS_DATASET,
        view_id=COMPLETION_EVENT_TYPE_METADATA_VIEW_NAME,
        view_query_template=query_template,
        description=COMPLETION_EVENT_TYPE_METADATA_VIEW_DESCRIPTION,
        should_materialize=True,
    )
