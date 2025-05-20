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
"""Reference views used by other views."""
from typing import List

from recidiviz.big_query.big_query_view import BigQueryViewBuilder
from recidiviz.calculator.query.state.views.reference.cleaned_offense_description_to_labels import (
    CLEANED_OFFENSE_DESCRIPTION_TO_LABELS_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.reference.completion_event_type_metadata import (
    get_completion_event_metadata_view_builder,
)
from recidiviz.calculator.query.state.views.reference.current_physical_residence_address import (
    CURRENT_PHYSICAL_RESIDENCE_ADDRESS_VIEW_BUILDER,
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
from recidiviz.calculator.query.state.views.reference.location_metadata.us_ia_location_metadata import (
    US_IA_LOCATION_METADATA_VIEW_BUILDER,
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
from recidiviz.calculator.query.state.views.reference.location_metadata.us_or_location_metadata import (
    US_OR_LOCATION_METADATA_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.reference.location_metadata.us_pa_location_metadata import (
    US_PA_LOCATION_METADATA_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.reference.location_metadata.us_tn_location_metadata import (
    US_TN_LOCATION_METADATA_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.reference.location_metadata.us_tx_location_metadata import (
    US_TX_LOCATION_METADATA_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.reference.location_metadata.us_ut_location_metadata import (
    US_UT_LOCATION_METADATA_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.reference.product_display_person_external_ids import (
    PRODUCT_DISPLAY_PERSON_EXTERNAL_IDS_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.reference.product_failed_logins_monthly import (
    PRODUCT_FAILED_LOGINS_MONTHLY_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.reference.product_roster import (
    PRODUCT_ROSTER_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.reference.product_roster_archive import (
    PRODUCT_ROSTER_ARCHIVE_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.reference.product_staff import (
    PRODUCT_STAFF_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.reference.state_info import (
    get_state_info_view_builder,
)
from recidiviz.calculator.query.state.views.reference.state_resident_population import (
    STATE_RESIDENT_POPULATION_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.reference.state_resident_population_combined_race_ethnicity import (
    STATE_RESIDENT_POPULATION_COMBINED_RACE_ETHNICITY_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.reference.state_resident_population_combined_race_ethnicity_priority import (
    STATE_RESIDENT_POPULATION_COMBINED_RACE_ETHNICITY_PRIORITY_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.reference.state_staff_and_most_recent_supervisor_with_names import (
    STATE_STAFF_AND_MOST_RECENT_SUPERVISOR_WITH_NAMES_VIEW_BUILDER,
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
from recidiviz.calculator.query.state.views.reference.workflows_opportunity_configs import (
    WORKFLOWS_OPPORTUNITY_CONFIGS_VIEW_BUILDER,
)

LOCATION_METADATA_VIEW_BUILDERS = [
    LOCATION_METADATA_VIEW_BUILDER,
    US_AR_LOCATION_METADATA_VIEW_BUILDER,
    US_AZ_LOCATION_METADATA_VIEW_BUILDER,
    US_CA_LOCATION_METADATA_VIEW_BUILDER,
    US_IA_LOCATION_METADATA_VIEW_BUILDER,
    US_IX_LOCATION_METADATA_VIEW_BUILDER,
    US_MI_LOCATION_METADATA_VIEW_BUILDER,
    US_ND_LOCATION_METADATA_VIEW_BUILDER,
    US_OR_LOCATION_METADATA_VIEW_BUILDER,
    US_PA_LOCATION_METADATA_VIEW_BUILDER,
    US_TN_LOCATION_METADATA_VIEW_BUILDER,
    US_TX_LOCATION_METADATA_VIEW_BUILDER,
    US_UT_LOCATION_METADATA_VIEW_BUILDER,
]

REFERENCE_VIEW_BUILDERS: List[BigQueryViewBuilder] = [
    CLEANED_OFFENSE_DESCRIPTION_TO_LABELS_VIEW_BUILDER,
    CURRENT_PHYSICAL_RESIDENCE_ADDRESS_VIEW_BUILDER,
    CURRENT_STAFF_VIEW_BUILDER,
    INCARCERATION_LOCATION_IDS_TO_NAMES_VIEW_BUILDER,
    INGESTED_PRODUCT_USERS_VIEW_BUILDER,
    PRODUCT_DISPLAY_PERSON_EXTERNAL_IDS_VIEW_BUILDER,
    PRODUCT_FAILED_LOGINS_MONTHLY_VIEW_BUILDER,
    PRODUCT_ROSTER_VIEW_BUILDER,
    PRODUCT_ROSTER_ARCHIVE_VIEW_BUILDER,
    PRODUCT_STAFF_VIEW_BUILDER,
    STATE_RESIDENT_POPULATION_COMBINED_RACE_ETHNICITY_PRIORITY_VIEW_BUILDER,
    STATE_RESIDENT_POPULATION_COMBINED_RACE_ETHNICITY_VIEW_BUILDER,
    STATE_RESIDENT_POPULATION_VIEW_BUILDER,
    STATE_STAFF_AND_MOST_RECENT_SUPERVISOR_WITH_NAMES_VIEW_BUILDER,
    STATE_STAFF_WITH_NAMES_VIEW_BUILDER,
    SUPERVISION_LOCATION_IDS_TO_NAMES_VIEW_BUILDER,
    TASK_TO_COMPLETION_EVENT_VIEW_BUILDER,
    WORKFLOWS_OPPORTUNITY_CONFIGS_VIEW_BUILDER,
    get_completion_event_metadata_view_builder(),
    get_state_info_view_builder(),
    *LOCATION_METADATA_VIEW_BUILDERS,
]
