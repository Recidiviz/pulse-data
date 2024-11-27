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
"""Reference view that contains metadata about each completion event type

Output fields for this view are:

    - completion_event_type:
        Represents an event that a Recidiviz Workflows tool is expected to facilitate in order
        to help measure impact of our tools.

    - system_type
        Type of criminal justice system impacted by the completion event (INCARCERATION,
        SUPERVISION, SENTENCING)

    - decarceral_impact_type
        Represents the overarching type of decarceral impact (defined as a meaningful movement
        of a justice-impacted individual toward greater liberty) that a given event is meant to
        facilitate, based on the theory of change of Recidiviz tools. Even if a given event does
        not represent the final movement experienced by a JII, the theory of change underlying our
        tool’s influence on this event should ladder up to this impact type. For example, a review
        hearing for release from restrictive housing has an impact type of “release from
        restrictive housing”, because even if the hearing itself does not represent a decarceral
        shift in the JII’s experience in the system, we believe that Recidiviz tools should
        facilitate hearing events as a means for increasing eventual releases from restrictive
        housing.

    - is_jii_decarceral_transition
        A boolean flag that is True if an event represents a meaningful transition in a JII’s lived
        experience toward greater liberty. This flag is False for any other event that helps
        facilitates the impact type, but doesn’t on its own represent a decarceral shift in a JII’s
        lived experience. For example, a release from restrictive housing event will have a True
        flag (because this represents a JII moving out of solitary confinement into a less carceral
        facility), whereas a review hearing to assess eligibility for release will have a False
        flag (because a hearing on its own doesn’t represent any movement for a JII, but does
        represent an action taken by actors in the criminal justice system that could eventually
        facilitate a JII transition). Similarly, an early discharge is a JII decarceral transition,
        whereas an early discharge request form submission is an intermediate staff action and
        does not count as a JII transition.

    - has_mandatory_due_date
        A boolean flag that is True if there is a mandatory due date associated with the event
        indicating when the event is supposed to be completed by. Mandatory due dates include
        sentence projected full term release date (max), restrictive housing hearing dates,
        assessment/classification dates.
"""
from typing import List

from recidiviz.big_query.big_query_view import SimpleBigQueryViewBuilder
from recidiviz.calculator.query.state.dataset_config import REFERENCE_VIEWS_DATASET
from recidiviz.task_eligibility.task_completion_event_big_query_view_builder import (
    TaskCompletionEventType,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

COMPLETION_EVENT_TYPE_METADATA_VIEW_NAME = "completion_event_type_metadata"


def get_completion_event_metadata_view_builder() -> SimpleBigQueryViewBuilder:
    """Returns a view builder that maps every completion event type to its metadata attributes"""
    all_completion_event_query_fragments: List[str] = []
    for completion_event_type in TaskCompletionEventType:
        query_fragment = f"""
SELECT
    "{completion_event_type.value}" AS completion_event_type,
    "{completion_event_type.system_type.value}" AS system_type,
    "{completion_event_type.decarceral_impact_type.value}" AS decarceral_impact_type,
    {str(completion_event_type.is_jii_decarceral_transition).upper()} AS is_jii_decarceral_transition,
    {str(completion_event_type.has_mandatory_due_date).upper()} AS has_mandatory_due_date,
"""
        all_completion_event_query_fragments.append(query_fragment)

    query_template = "\nUNION ALL\n".join(all_completion_event_query_fragments)
    return SimpleBigQueryViewBuilder(
        dataset_id=REFERENCE_VIEWS_DATASET,
        view_id=COMPLETION_EVENT_TYPE_METADATA_VIEW_NAME,
        view_query_template=query_template,
        description=__doc__,
        should_materialize=True,
    )


if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        get_completion_event_metadata_view_builder().build_and_print()
