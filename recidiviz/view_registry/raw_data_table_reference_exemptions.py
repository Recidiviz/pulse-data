# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2025 Recidiviz, Inc.
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
"""Exemptions for views that are allowed to reference raw data tables directly."""

from typing import Dict

from recidiviz.big_query.big_query_address import BigQueryAddress
from recidiviz.calculator.query.state.views.analyst_data.us_ix.us_ix_detainer_spans import (
    US_IX_DETAINER_SPANS_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.reference.location_metadata.us_ca_location_metadata import (
    US_CA_LOCATION_METADATA_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.reference.location_metadata.us_ny_location_metadata import (
    US_NY_LOCATION_METADATA_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.reference.location_metadata.us_pa_location_metadata import (
    US_PA_LOCATION_METADATA_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.sessions.us_ix.us_ix_parole_board_hearing_sessions import (
    US_IX_PAROLE_BOARD_HEARING_SESSIONS_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.workflows.firestore.client_record import (
    CLIENT_RECORD_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.workflows.firestore.resident_record import (
    RESIDENT_RECORD_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.workflows.firestore.us_pa_complete_transfer_to_special_circumstances_supervision_request_record import (
    US_PA_COMPLETE_TRANSFER_TO_SPECIAL_CIRCUMSTANCES_SUPERVISION_REQUEST_RECORD_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.workflows.firestore.us_pa_transfer_to_administrative_supervision_form_record import (
    US_PA_TRANSFER_TO_ADMINISTRATIVE_SUPERVISION_FORM_RECORD_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.workflows.us_nd.resident_metadata import (
    US_ND_RESIDENT_METADATA_VIEW_BUILDER,
)
from recidiviz.common.constants.states import StateCode
from recidiviz.task_eligibility.criteria.state_specific.us_nd.incarceration_past_parole_review_date_plus_one_month import (
    VIEW_BUILDER as US_ND_INCARCERATION_PAST_PAROLE_REVIEW_DATE_PLUS_ONE_MONTH_VIEW_BUILDER,
)
from recidiviz.task_eligibility.criteria.state_specific.us_nd.incarceration_within_5_or_more_months_of_parole_review_date import (
    VIEW_BUILDER as US_ND_INCARCERATION_WITHIN_5_OR_MORE_MONTHS_OF_PAROLE_REVIEW_DATE_VIEW_BUILDER,
)
from recidiviz.task_eligibility.criteria.state_specific.us_nd.incarceration_within_6_months_of_parole_review_date import (
    VIEW_BUILDER as US_ND_INCARCERATION_WITHIN_6_MONTHS_OF_PAROLE_REVIEW_DATE_VIEW_BUILDER,
)
from recidiviz.task_eligibility.criteria.state_specific.us_nd.incarceration_within_12_months_of_parole_review_date import (
    VIEW_BUILDER as US_ND_INCARCERATION_WITHIN_12_MONTHS_OF_PAROLE_REVIEW_DATE_VIEW_BUILDER,
)
from recidiviz.task_eligibility.criteria.state_specific.us_nd.not_in_active_revocation_status import (
    VIEW_BUILDER as US_ND_NOT_IN_ACTIVE_REVOCATION_STATUS_VIEW_BUILDER,
)
from recidiviz.task_eligibility.criteria.state_specific.us_pa.meets_special_circumstances_criteria_for_time_served import (
    VIEW_BUILDER as US_PA_MEETS_SPECIAL_CIRCUMSTANCES_CRITERIA_FOR_TIME_SERVED_VIEW_BUILDER,
)
from recidiviz.task_eligibility.criteria.state_specific.us_pa.not_on_sex_offense_protocol import (
    VIEW_BUILDER as US_PA_NOT_ON_SEX_OFFENSE_PROTOCOL_VIEW_BUILDER,
)
from recidiviz.task_eligibility.criteria.state_specific.us_pa.on_supervision_at_least_one_year import (
    VIEW_BUILDER as US_PA_ON_SUPERVISION_AT_LEAST_ONE_YEAR_VIEW_BUILDER,
)
from recidiviz.validation.views.external_data.regions.us_me.population_releases import (
    US_ME_POPULATION_RELEASES_VIEW_BUILDER,
)
from recidiviz.validation.views.state.parole_agent_badge_number_changes import (
    PA_BADGE_NUMBER_CHANGES_VIEW_BUILDER,
)

# raw table datasets potentially contain rows that are marked as deleted so should
# not be referenced directly. if a raw data reference is necessary, views should use
# the _all raw data view
RAW_DATA_TABLE_REFERENCE_EXEMPTIONS: Dict[StateCode, Dict[BigQueryAddress, str]] = {
    StateCode.US_CA: {
        US_CA_LOCATION_METADATA_VIEW_BUILDER.address: "TODO(#51176) Update to use raw data _all view",
        CLIENT_RECORD_VIEW_BUILDER.address: "TODO(#51176) Update to use raw data _all view",
        PA_BADGE_NUMBER_CHANGES_VIEW_BUILDER.address: "TODO(#51176) Update to use raw data _all view",
    },
    StateCode.US_IX: {
        US_IX_DETAINER_SPANS_VIEW_BUILDER.address: "TODO(#51179) Update to use raw data _all view",
        US_IX_PAROLE_BOARD_HEARING_SESSIONS_VIEW_BUILDER.address: "TODO(#51179) Update to use raw data _all view",
    },
    StateCode.US_ME: {
        US_ME_POPULATION_RELEASES_VIEW_BUILDER.address: "TODO(#51181) Update to use raw data _all view",
        RESIDENT_RECORD_VIEW_BUILDER.address: "TODO(#51181) Update to use raw data _all view",
    },
    StateCode.US_ND: {
        US_ND_INCARCERATION_WITHIN_5_OR_MORE_MONTHS_OF_PAROLE_REVIEW_DATE_VIEW_BUILDER.address: "TODO(#51183) Update to use raw data _all view",
        US_ND_RESIDENT_METADATA_VIEW_BUILDER.address: "TODO(#51183) Update to use raw data _all view",
        US_ND_NOT_IN_ACTIVE_REVOCATION_STATUS_VIEW_BUILDER.address: "TODO(#51183) Update to use raw data _all view",
        US_ND_INCARCERATION_WITHIN_6_MONTHS_OF_PAROLE_REVIEW_DATE_VIEW_BUILDER.address: "TODO(#51183) Update to use raw data _all view",
        US_ND_INCARCERATION_PAST_PAROLE_REVIEW_DATE_PLUS_ONE_MONTH_VIEW_BUILDER.address: "TODO(#51183) Update to use raw data _all view",
        US_ND_INCARCERATION_WITHIN_12_MONTHS_OF_PAROLE_REVIEW_DATE_VIEW_BUILDER.address: "TODO(#51183) Update to use raw data _all view",
    },
    StateCode.US_NY: {
        US_NY_LOCATION_METADATA_VIEW_BUILDER.address: "TODO(#51185) Update to use raw data _all view"
    },
    StateCode.US_PA: {
        US_PA_ON_SUPERVISION_AT_LEAST_ONE_YEAR_VIEW_BUILDER.address: "TODO(#51186) Update to use raw data _all view",
        US_PA_COMPLETE_TRANSFER_TO_SPECIAL_CIRCUMSTANCES_SUPERVISION_REQUEST_RECORD_VIEW_BUILDER.address: "TODO(#51186) Update to use raw data _all view",
        US_PA_LOCATION_METADATA_VIEW_BUILDER.address: "TODO(#51186) Update to use raw data _all view",
        US_PA_MEETS_SPECIAL_CIRCUMSTANCES_CRITERIA_FOR_TIME_SERVED_VIEW_BUILDER.address: "TODO(#51186) Update to use raw data _all view",
        US_PA_TRANSFER_TO_ADMINISTRATIVE_SUPERVISION_FORM_RECORD_VIEW_BUILDER.address: "TODO(#51186) Update to use raw data _all view",
        US_PA_NOT_ON_SEX_OFFENSE_PROTOCOL_VIEW_BUILDER.address: "TODO(#51186) Update to use raw data _all view",
        CLIENT_RECORD_VIEW_BUILDER.address: "TODO(#51186) Update to use raw data _all view",
    },
}
