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
"""Helper for determining if a view is an infra "library" view"""

from recidiviz.big_query.big_query_address import BigQueryAddress
from recidiviz.calculator.query.state.dataset_config import SENTENCE_SESSIONS_DATASET
from recidiviz.calculator.query.state.views.reference.completion_event_type_metadata import (
    get_completion_event_metadata_view_builder,
)
from recidiviz.calculator.query.state.views.reference.incarceration_location_ids_to_names import (
    INCARCERATION_LOCATION_IDS_TO_NAMES_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.reference.location_metadata.location_metadata import (
    LOCATION_METADATA_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.reference.state_staff_with_names import (
    STATE_STAFF_WITH_NAMES_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.reference.supervision_location_ids_to_names import (
    SUPERVISION_LOCATION_IDS_TO_NAMES_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.reference.task_to_completion_event import (
    TASK_TO_COMPLETION_EVENT_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.sessions.absconsion_bench_warrant_sessions import (
    ABSCONSION_BENCH_WARRANT_SESSIONS_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.sessions.assessment_score_sessions import (
    ASSESSMENT_SCORE_SESSIONS_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.sessions.compartment_level_0_super_sessions import (
    COMPARTMENT_LEVEL_0_SUPER_SESSIONS_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.sessions.compartment_level_1_super_sessions import (
    COMPARTMENT_LEVEL_1_SUPER_SESSIONS_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.sessions.compartment_session_end_reasons import (
    COMPARTMENT_SESSION_END_REASONS_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.sessions.compartment_session_start_reasons import (
    COMPARTMENT_SESSION_START_REASONS_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.sessions.compartment_sessions import (
    COMPARTMENT_SESSIONS_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.sessions.compartment_sessions_closest_sentence_imposed_group import (
    COMPARTMENT_SESSIONS_CLOSEST_SENTENCE_IMPOSED_GROUP_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.sessions.compartment_sub_sessions import (
    COMPARTMENT_SUB_SESSIONS_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.sessions.custodial_authority_sessions import (
    CUSTODIAL_AUTHORITY_SESSIONS_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.sessions.custody_level_dedup_priority import (
    CUSTODY_LEVEL_DEDUP_PRIORITY_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.sessions.custody_level_sessions import (
    CUSTODY_LEVEL_SESSIONS_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.sessions.dataflow_sessions import (
    DATAFLOW_SESSIONS_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.sessions.housing_unit_raw_text_sessions import (
    HOUSING_UNIT_SESSIONS_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.sessions.housing_unit_type_collapsed_solitary_sessions import (
    HOUSING_UNIT_TYPE_COLLAPSED_SOLITARY_SESSIONS_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.sessions.housing_unit_type_non_protective_custody_solitary_sessions import (
    HOUSING_UNIT_TYPE_NON_PROTECTIVE_CUSTODY_SOLITARY_SESSIONS_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.sessions.housing_unit_type_sessions import (
    HOUSING_UNIT_TYPE_SESSIONS_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.sessions.incarceration_projected_completion_date_spans import (
    INCARCERATION_PROJECTED_COMPLETION_DATE_SPANS_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.sessions.incarceration_super_sessions import (
    INCARCERATION_SUPER_SESSIONS_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.sessions.location_sessions import (
    LOCATION_SESSIONS_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.sessions.location_type_sessions import (
    LOCATION_TYPE_SESSIONS_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.sessions.person_caseload_location_sessions import (
    PERSON_CASELOAD_LOCATION_SESSIONS_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.sessions.person_demographics import (
    PERSON_DEMOGRAPHICS_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.sessions.prioritized_supervision_sessions import (
    PRIORITIZED_SUPERVISION_SESSIONS_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.sessions.sentence_deadline_spans import (
    SENTENCE_DEADLINE_SPANS_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.sessions.sentence_imposed_group_summary import (
    SENTENCE_IMPOSED_GROUP_SUMMARY_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.sessions.sentence_spans import (
    SENTENCE_SPANS_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.sessions.session_location_names import (
    SESSION_LOCATION_NAMES_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.sessions.supervision_employment_status_sessions import (
    SUPERVISION_EMPLOYMENT_STATUS_SESSIONS_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.sessions.supervision_level_sessions import (
    SUPERVISION_LEVEL_SESSIONS_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.sessions.supervision_officer_sessions import (
    SUPERVISION_OFFICER_SESSIONS_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.sessions.supervision_projected_completion_date_spans import (
    SUPERVISION_PROJECTED_COMPLETION_DATE_SPANS_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.sessions.supervision_staff_attribute_sessions import (
    SUPERVISION_STAFF_ATTRIBUTE_SESSIONS_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.sessions.supervision_unit_supervisor_sessions import (
    SUPERVISION_UNIT_SUPERVISOR_SESSIONS_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.sessions.system_sessions import (
    SYSTEM_SESSIONS_VIEW_BUILDER,
)
from recidiviz.ingest.views.dataset_config import NORMALIZED_STATE_DATASET


def is_view_part_of_infra_library_2025(view_address: BigQueryAddress) -> bool:
    """Returns true if this view is a "library" view - a view maintained by DSI /
    Doppler that provides a meaningful state- and product-agnostic abstraction that
    should be used in downstream product views.

    NOTE: THE ALGORITHMS FOR COMPUTING SCORES THAT USE THIS FUNCTION ARE LOCKED FOR 2025
    - DO NOT MAKE ANY MODIFICATIONS TO THE LOGIC (other than adding completely net new
    views), OTHERWISE WE WON'T BE ABLE TO LEGITIMATELY COMPARE HOW COMPLEXITY CHANGES
    OVER THE COURSE OF THE YEAR.
    """
    if view_address.is_state_specific_address():
        # State-specific tables are not considered "library" views under any
        # circumstances.
        return False

    if view_address.dataset_id in {
        NORMALIZED_STATE_DATASET,
        SENTENCE_SESSIONS_DATASET,
    }:
        return True

    sessions_library_views: set[BigQueryAddress] = {
        ABSCONSION_BENCH_WARRANT_SESSIONS_VIEW_BUILDER.address,
        ASSESSMENT_SCORE_SESSIONS_VIEW_BUILDER.address,
        COMPARTMENT_LEVEL_0_SUPER_SESSIONS_VIEW_BUILDER.address,
        COMPARTMENT_LEVEL_1_SUPER_SESSIONS_VIEW_BUILDER.address,
        COMPARTMENT_SESSIONS_VIEW_BUILDER.address,
        COMPARTMENT_SESSIONS_CLOSEST_SENTENCE_IMPOSED_GROUP_VIEW_BUILDER.address,
        COMPARTMENT_SESSION_END_REASONS_VIEW_BUILDER.address,
        COMPARTMENT_SESSION_START_REASONS_VIEW_BUILDER.address,
        COMPARTMENT_SUB_SESSIONS_VIEW_BUILDER.address,
        CUSTODIAL_AUTHORITY_SESSIONS_VIEW_BUILDER.address,
        CUSTODY_LEVEL_DEDUP_PRIORITY_VIEW_BUILDER.address,
        CUSTODY_LEVEL_SESSIONS_VIEW_BUILDER.address,
        DATAFLOW_SESSIONS_VIEW_BUILDER.address,
        HOUSING_UNIT_SESSIONS_VIEW_BUILDER.address,
        HOUSING_UNIT_TYPE_COLLAPSED_SOLITARY_SESSIONS_VIEW_BUILDER.address,
        HOUSING_UNIT_TYPE_NON_PROTECTIVE_CUSTODY_SOLITARY_SESSIONS_VIEW_BUILDER.address,
        HOUSING_UNIT_TYPE_SESSIONS_VIEW_BUILDER.address,
        INCARCERATION_PROJECTED_COMPLETION_DATE_SPANS_VIEW_BUILDER.address,
        INCARCERATION_SUPER_SESSIONS_VIEW_BUILDER.address,
        LOCATION_SESSIONS_VIEW_BUILDER.address,
        LOCATION_TYPE_SESSIONS_VIEW_BUILDER.address,
        PERSON_CASELOAD_LOCATION_SESSIONS_VIEW_BUILDER.address,
        PERSON_DEMOGRAPHICS_VIEW_BUILDER.address,
        PRIORITIZED_SUPERVISION_SESSIONS_VIEW_BUILDER.address,
        SENTENCE_DEADLINE_SPANS_VIEW_BUILDER.address,
        SENTENCE_IMPOSED_GROUP_SUMMARY_VIEW_BUILDER.address,
        SENTENCE_SPANS_VIEW_BUILDER.address,
        SESSION_LOCATION_NAMES_VIEW_BUILDER.address,
        SUPERVISION_EMPLOYMENT_STATUS_SESSIONS_VIEW_BUILDER.address,
        SUPERVISION_LEVEL_SESSIONS_VIEW_BUILDER.address,
        SUPERVISION_OFFICER_SESSIONS_VIEW_BUILDER.address,
        SUPERVISION_PROJECTED_COMPLETION_DATE_SPANS_VIEW_BUILDER.address,
        SUPERVISION_STAFF_ATTRIBUTE_SESSIONS_VIEW_BUILDER.address,
        SUPERVISION_UNIT_SUPERVISOR_SESSIONS_VIEW_BUILDER.address,
        SYSTEM_SESSIONS_VIEW_BUILDER.address,
    }

    reference_views_library_views: set[BigQueryAddress] = {
        STATE_STAFF_WITH_NAMES_VIEW_BUILDER.address,
        LOCATION_METADATA_VIEW_BUILDER.address,
        get_completion_event_metadata_view_builder().address,
        SUPERVISION_LOCATION_IDS_TO_NAMES_VIEW_BUILDER.address,
        INCARCERATION_LOCATION_IDS_TO_NAMES_VIEW_BUILDER.address,
        TASK_TO_COMPLETION_EVENT_VIEW_BUILDER.address,
    }

    if view_address in {*sessions_library_views, *reference_views_library_views}:
        return True

    return False
