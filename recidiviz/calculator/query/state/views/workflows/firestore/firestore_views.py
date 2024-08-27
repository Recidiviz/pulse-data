#  Recidiviz - a data platform for criminal justice reform
#  Copyright (C) 2022 Recidiviz, Inc.
#
#  This program is free software: you can redistribute it and/or modify
#  it under the terms of the GNU General Public License as published by
#  the Free Software Foundation, either version 3 of the License, or
#  (at your option) any later version.
#
#  This program is distributed in the hope that it will be useful,
#  but WITHOUT ANY WARRANTY; without even the implied warranty of
#  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
#  GNU General Public License for more details.
#
#  You should have received a copy of the GNU General Public License
#  along with this program.  If not, see <https://www.gnu.org/licenses/>.
#  =============================================================================
"""Views to which will be exported to a GCP bucket to be ETL'd into Firestore."""
from typing import List

from recidiviz.big_query.big_query_view import BigQueryViewBuilder
from recidiviz.calculator.query.state.views.workflows.firestore.client_record import (
    CLIENT_RECORD_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.workflows.firestore.incarceration_staff_record import (
    INCARCERATION_STAFF_RECORD_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.workflows.firestore.location_record import (
    LOCATION_RECORD_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.workflows.firestore.resident_record import (
    RESIDENT_RECORD_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.workflows.firestore.supervision_staff_record import (
    SUPERVISION_STAFF_RECORD_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.workflows.firestore.us_az_release_to_tpr_request_record import (
    US_AZ_RELEASE_TO_TPR_REQUEST_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.workflows.firestore.us_ca_supervision_level_downgrade_form_record import (
    US_CA_SUPERVISION_LEVEL_DOWNGRADE_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.workflows.firestore.us_ix_complete_discharge_early_from_supervision_request_record import (
    US_IX_COMPLETE_DISCHARGE_EARLY_FROM_SUPERVISION_REQUEST_RECORD_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.workflows.firestore.us_ix_complete_full_term_early_from_supervision_request_record import (
    US_IX_COMPLETE_FULL_TERM_DISCHARGE_FROM_SUPERVISION_REQUEST_RECORD_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.workflows.firestore.us_ix_complete_transfer_to_limited_supervision_form_record import (
    US_IX_COMPLETE_TRANSFER_TO_LIMITED_SUPERVISION_FORM_RECORD_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.workflows.firestore.us_ix_supervision_level_downgrade_record import (
    US_IX_SUPERVISION_LEVEL_DOWNGRADE_RECORD_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.workflows.firestore.us_ix_supervision_tasks_record import (
    US_IX_SUPERVISION_TASKS_RECORD_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.workflows.firestore.us_ix_transfer_to_crc_resident_worker_request_record import (
    US_IX_TRANSFER_TO_CRC_RESIDENT_WORKER_REQUEST_RECORD_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.workflows.firestore.us_ix_transfer_to_crc_work_release_request_record import (
    US_IX_TRANSFER_TO_CRC_WORK_RELEASE_REQUEST_RECORD_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.workflows.firestore.us_ix_transfer_to_limited_supervision_jii_record import (
    US_IX_TRANSFER_TO_LIMITED_SUPERVISION_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.workflows.firestore.us_ix_transfer_to_xcrc_request_record import (
    US_IX_TRANSFER_TO_XCRC_REQUEST_RECORD_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.workflows.firestore.us_me_complete_early_termination_record import (
    US_ME_COMPLETE_EARLY_TERMINATION_RECORD_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.workflows.firestore.us_me_complete_transfer_to_sccp_form_record import (
    US_ME_TRANSFER_TO_SCCP_RECORD_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.workflows.firestore.us_me_custody_level_downgrade_to_medium_trustee_request_record import (
    US_ME_CUSTODY_LEVEL_DOWNGRADE_TO_MEDIUM_TRUSTEE_RECORD_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.workflows.firestore.us_me_custody_reclassification_review_form_record import (
    US_ME_RECLASSIFICATION_REVIEW_FORM_RECORD_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.workflows.firestore.us_me_furlough_release_form_record import (
    US_ME_COMPLETE_FURLOUGH_RELEASE_RECORD_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.workflows.firestore.us_me_transfer_to_sccp_jii_record import (
    US_ME_TRANSFER_TO_SCCP_JII_RECORD_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.workflows.firestore.us_me_work_release_form_record import (
    US_ME_COMPLETE_WORK_RELEASE_RECORD_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.workflows.firestore.us_mi_complete_add_in_person_security_classification_committee_review_form_record import (
    US_MI_COMPLETE_ADD_IN_PERSON_SECURITY_CLASSIFICATION_COMMITTEE_REVIEW_FORM_RECORD_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.workflows.firestore.us_mi_complete_classification_review_form_record import (
    US_MI_COMPLETE_CLASSIFICATION_REVIEW_FORM_RECORD_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.workflows.firestore.us_mi_complete_discharge_early_from_supervision_request_record import (
    US_MI_COMPLETE_DISCHARGE_EARLY_FROM_SUPERVISION_REQUEST_RECORD_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.workflows.firestore.us_mi_complete_full_term_discharge_from_supervision_request_record import (
    US_MI_COMPLETE_FULL_TERM_DISCHARGE_FROM_SUPERVISION_REQUEST_RECORD_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.workflows.firestore.us_mi_complete_reclassification_to_general_population_request_record import (
    US_MI_COMPLETE_RECLASSIFICATION_TO_GENERAL_POPULATION_REQUEST_RECORD_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.workflows.firestore.us_mi_complete_security_classification_committee_review_form_record import (
    US_MI_COMPLETE_SECURITY_CLASSIFICATION_COMMITTEE_REVIEW_FORM_RECORD_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.workflows.firestore.us_mi_complete_transfer_to_telephone_reporting_request_record import (
    US_MI_COMPLETE_TRANSFER_TO_TELEPHONE_REPORTING_REQUEST_RECORD_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.workflows.firestore.us_mi_complete_warden_in_person_security_classification_committee_review_form_record import (
    US_MI_COMPLETE_WARDEN_IN_PERSON_SECURITY_CLASSIFICATION_COMMITTEE_REVIEW_FORM_RECORD_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.workflows.firestore.us_mi_supervision_level_downgrade_record import (
    US_MI_SUPERVISION_LEVEL_DOWNGRADE_RECORD_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.workflows.firestore.us_mo_overdue_restrictive_housing_hearing import (
    US_MO_OVERDUE_RESTRICTIVE_HOUSING_HEARING_RECORD_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.workflows.firestore.us_mo_overdue_restrictive_housing_initial_hearing import (
    US_MO_OVERDUE_RESTRICTIVE_HOUSING_INITIAL_HEARING_RECORD_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.workflows.firestore.us_mo_overdue_restrictive_housing_release import (
    US_MO_OVERDUE_RESTRICTIVE_HOUSING_RELEASE_RECORD_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.workflows.firestore.us_mo_overdue_restrictive_housing_review_hearing import (
    US_MO_OVERDUE_RESTRICTIVE_HOUSING_REVIEW_HEARING_RECORD_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.workflows.firestore.us_nd_complete_discharge_early_from_supervision_record import (
    US_ND_COMPLETE_DISCHARGE_EARLY_FROM_SUPERVISION_RECORD_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.workflows.firestore.us_nd_transfer_to_atp_form_record import (
    US_ND_TRANSFER_TO_ATP_FORM_RECORD_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.workflows.firestore.us_nd_transfer_to_minimum_facility_form_record import (
    US_ND_TRANSFER_TO_MINIMUM_FACILITY_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.workflows.firestore.us_or_earned_discharge_record import (
    US_OR_EARNED_DISCHARGE_RECORD_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.workflows.firestore.us_pa_complete_transfer_to_special_circumstances_supervision_request_record import (
    US_PA_COMPLETE_TRANSFER_TO_SPECIAL_CIRCUMSTANCES_SUPERVISION_REQUEST_RECORD_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.workflows.firestore.us_pa_transfer_to_administrative_supervision_form_record import (
    US_PA_TRANSFER_TO_ADMINISTRATIVE_SUPERVISION_FORM_RECORD_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.workflows.firestore.us_tn_annual_reclassification_review_record import (
    US_TN_ANNUAL_RECLASSIFICATION_REVIEW_RECORD_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.workflows.firestore.us_tn_custody_level_downgrade_record import (
    US_TN_CUSTODY_LEVEL_DOWNGRADE_RECORD_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.workflows.firestore.us_tn_full_term_supervision_discharge_record import (
    US_TN_FULL_TERM_SUPERVISION_DISCHARGE_RECORD_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.workflows.firestore.us_tn_supervision_level_downgrade_record import (
    US_TN_SUPERVISION_LEVEL_DOWNGRADE_RECORD_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.workflows.firestore.us_tn_transfer_to_compliant_reporting_record import (
    US_TN_TRANSFER_TO_COMPLIANT_REPORTING_RECORD_VIEW_BUILDER,
)

FIRESTORE_VIEW_BUILDERS: List[BigQueryViewBuilder] = [
    CLIENT_RECORD_VIEW_BUILDER,
    RESIDENT_RECORD_VIEW_BUILDER,
    INCARCERATION_STAFF_RECORD_VIEW_BUILDER,
    SUPERVISION_STAFF_RECORD_VIEW_BUILDER,
    LOCATION_RECORD_VIEW_BUILDER,
    US_AZ_RELEASE_TO_TPR_REQUEST_VIEW_BUILDER,
    US_CA_SUPERVISION_LEVEL_DOWNGRADE_VIEW_BUILDER,
    US_IX_COMPLETE_DISCHARGE_EARLY_FROM_SUPERVISION_REQUEST_RECORD_VIEW_BUILDER,
    US_IX_COMPLETE_FULL_TERM_DISCHARGE_FROM_SUPERVISION_REQUEST_RECORD_VIEW_BUILDER,
    US_IX_COMPLETE_TRANSFER_TO_LIMITED_SUPERVISION_FORM_RECORD_VIEW_BUILDER,
    US_IX_SUPERVISION_LEVEL_DOWNGRADE_RECORD_VIEW_BUILDER,
    US_IX_SUPERVISION_TASKS_RECORD_VIEW_BUILDER,
    US_IX_TRANSFER_TO_XCRC_REQUEST_RECORD_VIEW_BUILDER,
    US_IX_TRANSFER_TO_CRC_WORK_RELEASE_REQUEST_RECORD_VIEW_BUILDER,
    US_IX_TRANSFER_TO_CRC_RESIDENT_WORKER_REQUEST_RECORD_VIEW_BUILDER,
    US_IX_TRANSFER_TO_LIMITED_SUPERVISION_VIEW_BUILDER,
    US_ND_COMPLETE_DISCHARGE_EARLY_FROM_SUPERVISION_RECORD_VIEW_BUILDER,
    US_ND_TRANSFER_TO_MINIMUM_FACILITY_VIEW_BUILDER,
    US_ND_TRANSFER_TO_ATP_FORM_RECORD_VIEW_BUILDER,
    US_TN_SUPERVISION_LEVEL_DOWNGRADE_RECORD_VIEW_BUILDER,
    US_ME_TRANSFER_TO_SCCP_RECORD_VIEW_BUILDER,
    US_ME_TRANSFER_TO_SCCP_JII_RECORD_VIEW_BUILDER,
    US_MI_COMPLETE_DISCHARGE_EARLY_FROM_SUPERVISION_REQUEST_RECORD_VIEW_BUILDER,
    US_ME_COMPLETE_EARLY_TERMINATION_RECORD_VIEW_BUILDER,
    US_ME_COMPLETE_FURLOUGH_RELEASE_RECORD_VIEW_BUILDER,
    US_ME_COMPLETE_WORK_RELEASE_RECORD_VIEW_BUILDER,
    US_ME_CUSTODY_LEVEL_DOWNGRADE_TO_MEDIUM_TRUSTEE_RECORD_VIEW_BUILDER,
    US_ME_RECLASSIFICATION_REVIEW_FORM_RECORD_VIEW_BUILDER,
    US_MI_COMPLETE_CLASSIFICATION_REVIEW_FORM_RECORD_VIEW_BUILDER,
    US_MI_COMPLETE_SECURITY_CLASSIFICATION_COMMITTEE_REVIEW_FORM_RECORD_VIEW_BUILDER,
    US_MI_COMPLETE_WARDEN_IN_PERSON_SECURITY_CLASSIFICATION_COMMITTEE_REVIEW_FORM_RECORD_VIEW_BUILDER,
    US_MI_COMPLETE_ADD_IN_PERSON_SECURITY_CLASSIFICATION_COMMITTEE_REVIEW_FORM_RECORD_VIEW_BUILDER,
    US_MI_COMPLETE_RECLASSIFICATION_TO_GENERAL_POPULATION_REQUEST_RECORD_VIEW_BUILDER,
    US_MI_COMPLETE_TRANSFER_TO_TELEPHONE_REPORTING_REQUEST_RECORD_VIEW_BUILDER,
    US_MI_COMPLETE_FULL_TERM_DISCHARGE_FROM_SUPERVISION_REQUEST_RECORD_VIEW_BUILDER,
    US_MI_SUPERVISION_LEVEL_DOWNGRADE_RECORD_VIEW_BUILDER,
    US_TN_FULL_TERM_SUPERVISION_DISCHARGE_RECORD_VIEW_BUILDER,
    US_TN_CUSTODY_LEVEL_DOWNGRADE_RECORD_VIEW_BUILDER,
    US_MO_OVERDUE_RESTRICTIVE_HOUSING_INITIAL_HEARING_RECORD_VIEW_BUILDER,
    US_MO_OVERDUE_RESTRICTIVE_HOUSING_REVIEW_HEARING_RECORD_VIEW_BUILDER,
    US_MO_OVERDUE_RESTRICTIVE_HOUSING_RELEASE_RECORD_VIEW_BUILDER,
    US_MO_OVERDUE_RESTRICTIVE_HOUSING_HEARING_RECORD_VIEW_BUILDER,
    US_TN_TRANSFER_TO_COMPLIANT_REPORTING_RECORD_VIEW_BUILDER,
    US_TN_ANNUAL_RECLASSIFICATION_REVIEW_RECORD_VIEW_BUILDER,
    US_OR_EARNED_DISCHARGE_RECORD_VIEW_BUILDER,
    US_PA_TRANSFER_TO_ADMINISTRATIVE_SUPERVISION_FORM_RECORD_VIEW_BUILDER,
    US_PA_COMPLETE_TRANSFER_TO_SPECIAL_CIRCUMSTANCES_SUPERVISION_REQUEST_RECORD_VIEW_BUILDER,
]
