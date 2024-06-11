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
from recidiviz.calculator.query.state.views.reference.cleaned_offense_description_to_labels import (
    CLEANED_OFFENSE_DESCRIPTION_TO_LABELS_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.reference.current_staff import (
    CURRENT_STAFF_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.reference.incarceration_location_ids_to_names import (
    INCARCERATION_LOCATION_IDS_TO_NAMES_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.reference.ingested_product_users import (
    INGESTED_PRODUCT_USERS_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.reference.location_metadata.location_metadata import (
    LOCATION_METADATA_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.reference.location_metadata.us_ar_location_metadata import (
    US_AR_LOCATION_METADATA_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.reference.location_metadata.us_az_location_metadata import (
    US_AZ_LOCATION_METADATA_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.reference.location_metadata.us_ca_location_metadata import (
    US_CA_LOCATION_METADATA_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.reference.location_metadata.us_ix_location_metadata import (
    US_IX_LOCATION_METADATA_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.reference.location_metadata.us_mi_location_metadata import (
    US_MI_LOCATION_METADATA_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.reference.location_metadata.us_nd_location_metadata import (
    US_ND_LOCATION_METADATA_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.reference.location_metadata.us_pa_location_metadata import (
    US_PA_LOCATION_METADATA_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.reference.location_metadata.us_tn_location_metadata import (
    US_TN_LOCATION_METADATA_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.reference.persons_to_recent_county_of_residence import (
    PERSONS_TO_RECENT_COUNTY_OF_RESIDENCE_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.reference.product_failed_logins_monthly import (
    PRODUCT_FAILED_LOGINS_MONTHLY_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.reference.product_roster import (
    PRODUCT_ROSTER_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.reference.state_person_to_state_staff import (
    STATE_PERSON_TO_STATE_STAFF_VIEW_BUILDER,
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
from recidiviz.calculator.query.state.views.reference.us_ix_case_update_info import (
    US_IX_CASE_UPDATE_INFO_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.reference.us_mo_sentence_statuses import (
    US_MO_SENTENCE_STATUSES_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.reference.workflows_opportunity_configs import (
    WORKFLOWS_OPPORTUNITY_CONFIGS_VIEW_BUILDER,
)

REFERENCE_VIEW_BUILDERS: List[BigQueryViewBuilder] = [
    PERSONS_TO_RECENT_COUNTY_OF_RESIDENCE_VIEW_BUILDER,
    SUPERVISION_LOCATION_IDS_TO_NAMES_VIEW_BUILDER,
    INCARCERATION_LOCATION_IDS_TO_NAMES_VIEW_BUILDER,
    US_MO_SENTENCE_STATUSES_VIEW_BUILDER,
    US_IX_CASE_UPDATE_INFO_VIEW_BUILDER,
    CLEANED_OFFENSE_DESCRIPTION_TO_LABELS_VIEW_BUILDER,
    TASK_TO_COMPLETION_EVENT_VIEW_BUILDER,
    PRODUCT_FAILED_LOGINS_MONTHLY_VIEW_BUILDER,
    PRODUCT_ROSTER_VIEW_BUILDER,
    INGESTED_PRODUCT_USERS_VIEW_BUILDER,
    LOCATION_METADATA_VIEW_BUILDER,
    US_ND_LOCATION_METADATA_VIEW_BUILDER,
    US_PA_LOCATION_METADATA_VIEW_BUILDER,
    US_TN_LOCATION_METADATA_VIEW_BUILDER,
    US_IX_LOCATION_METADATA_VIEW_BUILDER,
    US_MI_LOCATION_METADATA_VIEW_BUILDER,
    US_CA_LOCATION_METADATA_VIEW_BUILDER,
    WORKFLOWS_OPPORTUNITY_CONFIGS_VIEW_BUILDER,
    STATE_PERSON_TO_STATE_STAFF_VIEW_BUILDER,
    STATE_STAFF_WITH_NAMES_VIEW_BUILDER,
    CURRENT_STAFF_VIEW_BUILDER,
    US_AR_LOCATION_METADATA_VIEW_BUILDER,
    US_AZ_LOCATION_METADATA_VIEW_BUILDER,
]
