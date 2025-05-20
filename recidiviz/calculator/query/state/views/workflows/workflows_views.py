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
"""Reference views used by other views."""
from typing import List

from recidiviz.big_query.big_query_view import BigQueryViewBuilder
from recidiviz.calculator.query.state.views.workflows.all_funnel_events import (
    ALL_FUNNEL_EVENTS_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.workflows.client_record_archive import (
    CLIENT_RECORD_ARCHIVE_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.workflows.clients_latest_referral_status import (
    CLIENTS_LATEST_REFERRAL_STATUS_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.workflows.clients_latest_referral_status_extended import (
    CLIENTS_LATEST_REFERRAL_STATUS_EXTENDED_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.workflows.clients_milestones_congratulated_another_way import (
    CLIENTS_MILESTONES_CONGRATULATED_ANOTHER_WAY_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.workflows.clients_milestones_congratulations_sent import (
    CLIENTS_MILESTONES_CONGRATULATIONS_SENT_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.workflows.clients_milestones_side_panel_opened import (
    CLIENTS_MILESTONES_SIDE_PANEL_OPENED_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.workflows.clients_opportunity_marked_submitted import (
    CLIENTS_OPPORTUNITY_MARKED_SUBMITTED_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.workflows.clients_opportunity_previewed import (
    CLIENTS_OPPORTUNITY_PREVIEWED_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.workflows.clients_opportunity_snoozed import (
    CLIENTS_OPPORTUNITY_SNOOZED_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.workflows.clients_opportunity_unsubmitted import (
    CLIENTS_OPPORTUNITY_UNSUBMITTED_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.workflows.clients_profile_viewed import (
    CLIENTS_PROFILE_VIEWED_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.workflows.clients_referral_form_copied import (
    CLIENTS_REFERRAL_FORM_COPIED_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.workflows.clients_referral_form_downloaded import (
    CLIENTS_REFERRAL_FORM_DOWNLOADED_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.workflows.clients_referral_form_edited import (
    CLIENTS_REFERRAL_FORM_EDITED_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.workflows.clients_referral_form_first_edited import (
    CLIENTS_REFERRAL_FORM_FIRST_EDITED_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.workflows.clients_referral_form_printed import (
    CLIENTS_REFERRAL_FORM_PRINTED_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.workflows.clients_referral_form_submitted import (
    CLIENTS_REFERRAL_FORM_SUBMITTED_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.workflows.clients_referral_form_viewed import (
    CLIENTS_REFERRAL_FORM_VIEWED_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.workflows.clients_referral_status_updated import (
    CLIENTS_REFERRAL_STATUS_UPDATED_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.workflows.clients_snooze_spans import (
    CLIENTS_SNOOZE_SPANS_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.workflows.clients_surfaced import (
    CLIENTS_SURFACED_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.workflows.current_impact_funnel_status import (
    CURRENT_IMPACT_FUNNEL_STATUS_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.workflows.firestore.firestore_views import (
    FIRESTORE_VIEW_BUILDERS,
)
from recidiviz.calculator.query.state.views.workflows.milestones_funnel import (
    MILESTONES_FUNNEL_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.workflows.person_id_to_external_id import (
    PERSON_ID_TO_EXTERNAL_ID_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.workflows.person_record import (
    PERSON_RECORD_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.workflows.reidentified_dashboard_users import (
    REIDENTIFIED_DASHBOARD_USERS_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.workflows.resident_record_archive import (
    RESIDENT_RECORD_ARCHIVE_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.workflows.us_ar.resident_metadata import (
    US_AR_RESIDENT_METADATA_VIEW_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.workflows.us_az.resident_metadata import (
    US_AZ_RESIDENT_METADATA_VIEW_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.workflows.us_ix.resident_metadata import (
    US_IX_RESIDENT_METADATA_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.workflows.us_ma.resident_metadata import (
    US_MA_RESIDENT_METADATA_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.workflows.us_me.resident_metadata import (
    US_ME_RESIDENT_METADATA_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.workflows.us_mo.resident_metadata import (
    US_MO_RESIDENT_METADATA_VIEW_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.workflows.us_nd.resident_metadata import (
    US_ND_RESIDENT_METADATA_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.workflows.workflows_caseload_search_field_by_state import (
    WORKFLOWS_CASELOAD_SEARCH_FIELD_BY_STATE_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.workflows.workflows_usage import (
    WORKFLOWS_USAGE_VIEW_BUILDER,
)

WORKFLOWS_VIEW_BUILDERS: List[BigQueryViewBuilder] = [
    *FIRESTORE_VIEW_BUILDERS,
    PERSON_ID_TO_EXTERNAL_ID_VIEW_BUILDER,
    CLIENT_RECORD_ARCHIVE_VIEW_BUILDER,
    RESIDENT_RECORD_ARCHIVE_VIEW_BUILDER,
    CLIENTS_SURFACED_VIEW_BUILDER,
    CLIENTS_OPPORTUNITY_PREVIEWED_VIEW_BUILDER,
    CLIENTS_OPPORTUNITY_MARKED_SUBMITTED_VIEW_BUILDER,
    CLIENTS_OPPORTUNITY_UNSUBMITTED_VIEW_BUILDER,
    CLIENTS_OPPORTUNITY_SNOOZED_VIEW_BUILDER,
    CLIENTS_REFERRAL_FORM_VIEWED_VIEW_BUILDER,
    CLIENTS_LATEST_REFERRAL_STATUS_VIEW_BUILDER,
    CLIENTS_LATEST_REFERRAL_STATUS_EXTENDED_VIEW_BUILDER,
    REIDENTIFIED_DASHBOARD_USERS_VIEW_BUILDER,
    CLIENTS_PROFILE_VIEWED_VIEW_BUILDER,
    CLIENTS_REFERRAL_FORM_COPIED_VIEW_BUILDER,
    CLIENTS_REFERRAL_FORM_DOWNLOADED_VIEW_BUILDER,
    CLIENTS_REFERRAL_FORM_FIRST_EDITED_VIEW_BUILDER,
    CLIENTS_REFERRAL_FORM_PRINTED_VIEW_BUILDER,
    CLIENTS_REFERRAL_FORM_SUBMITTED_VIEW_BUILDER,
    CLIENTS_REFERRAL_STATUS_UPDATED_VIEW_BUILDER,
    CLIENTS_REFERRAL_FORM_EDITED_VIEW_BUILDER,
    CLIENTS_MILESTONES_CONGRATULATIONS_SENT_VIEW_BUILDER,
    CLIENTS_SNOOZE_SPANS_VIEW_BUILDER,
    PERSON_RECORD_VIEW_BUILDER,
    CURRENT_IMPACT_FUNNEL_STATUS_VIEW_BUILDER,
    US_AR_RESIDENT_METADATA_VIEW_VIEW_BUILDER,
    US_AZ_RESIDENT_METADATA_VIEW_VIEW_BUILDER,
    WORKFLOWS_USAGE_VIEW_BUILDER,
    US_IX_RESIDENT_METADATA_VIEW_BUILDER,
    US_MO_RESIDENT_METADATA_VIEW_VIEW_BUILDER,
    US_ND_RESIDENT_METADATA_VIEW_BUILDER,
    CLIENTS_MILESTONES_SIDE_PANEL_OPENED_VIEW_BUILDER,
    CLIENTS_MILESTONES_CONGRATULATED_ANOTHER_WAY_VIEW_BUILDER,
    ALL_FUNNEL_EVENTS_VIEW_BUILDER,
    MILESTONES_FUNNEL_VIEW_BUILDER,
    US_ME_RESIDENT_METADATA_VIEW_BUILDER,
    WORKFLOWS_CASELOAD_SEARCH_FIELD_BY_STATE_VIEW_BUILDER,
    US_MA_RESIDENT_METADATA_VIEW_BUILDER,
]
