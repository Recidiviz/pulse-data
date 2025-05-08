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
"""Reference views for Workflows Opportunity configurations"""
from enum import Enum

import attr

from recidiviz.big_query.big_query_view import (
    BigQueryViewBuilder,
    SimpleBigQueryViewBuilder,
)
from recidiviz.calculator.query.state.dataset_config import REFERENCE_VIEWS_DATASET
from recidiviz.calculator.query.state.views.workflows.firestore.us_ar_institutional_worker_status_record import (
    US_AR_INSTITUTIONAL_WORKER_STATUS_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.workflows.firestore.us_ar_work_release_record import (
    US_AR_WORK_RELEASE_RECORD_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.workflows.firestore.us_az_approaching_acis_or_recidiviz_dtp_request_record import (
    US_AZ_APPROACHING_ACIS_OR_RECIDIVIZ_DTP_REQUEST_RECORD_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.workflows.firestore.us_az_approaching_acis_or_recidiviz_tpr_request_record import (
    US_AZ_APPROACHING_ACIS_OR_RECIDIVIZ_TPR_REQUEST_RECORD_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.workflows.firestore.us_az_overdue_for_acis_dtp_request_record import (
    US_AZ_OVERDUE_FOR_ACIS_DTP_REQUEST_RECORD_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.workflows.firestore.us_az_overdue_for_acis_tpr_request_record import (
    US_AZ_OVERDUE_FOR_ACIS_TPR_REQUEST_RECORD_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.workflows.firestore.us_az_transfer_to_administrative_supervision_record import (
    US_AZ_TRANSFER_TO_ADMINISTRATIVE_SUPERVISION_RECORD_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.workflows.firestore.us_ca_supervision_level_downgrade_form_record import (
    US_CA_SUPERVISION_LEVEL_DOWNGRADE_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.workflows.firestore.us_ia_complete_early_discharge_form_record import (
    US_IA_COMPLETE_EARLY_DISCHARGE_FORM_RECORD_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.workflows.firestore.us_ix_complete_discharge_early_from_supervision_request_record import (
    US_IX_COMPLETE_DISCHARGE_EARLY_FROM_SUPERVISION_REQUEST_RECORD_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.workflows.firestore.us_ix_complete_full_term_discharge_from_supervision_request_record import (
    US_IX_COMPLETE_FULL_TERM_DISCHARGE_FROM_SUPERVISION_REQUEST_RECORD_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.workflows.firestore.us_ix_complete_transfer_to_limited_supervision_form_record import (
    US_IX_COMPLETE_TRANSFER_TO_LIMITED_SUPERVISION_FORM_RECORD_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.workflows.firestore.us_ix_custody_level_downgrade_record import (
    US_IX_CUSTODY_LEVEL_DOWNGRADE_RECORD_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.workflows.firestore.us_ix_supervision_level_downgrade_record import (
    US_IX_SUPERVISION_LEVEL_DOWNGRADE_RECORD_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.workflows.firestore.us_ix_transfer_to_crc_resident_worker_request_record import (
    US_IX_TRANSFER_TO_CRC_RESIDENT_WORKER_REQUEST_RECORD_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.workflows.firestore.us_ix_transfer_to_crc_work_release_request_record import (
    US_IX_TRANSFER_TO_CRC_WORK_RELEASE_REQUEST_RECORD_VIEW_BUILDER,
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
from recidiviz.calculator.query.state.views.workflows.firestore.us_me_overdue_for_discharge_request_record import (
    US_ME_OVERDUE_FOR_DISCHARGE_VIEW_BUILDER,
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
from recidiviz.calculator.query.state.views.workflows.firestore.us_ne_override_low_to_conditional_low_record import (
    US_NE_OVERRIDE_LOW_TO_CONDITIONAL_LOW_RECORD_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.workflows.firestore.us_ne_override_moderate_to_low_record import (
    US_NE_OVERRIDE_MODERATE_TO_LOW_RECORD_VIEW_BUILDER,
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
from recidiviz.calculator.query.state.views.workflows.firestore.us_tn_initial_classification_review_record import (
    US_TN_INITIAL_CLASSIFICATION_REVIEW_RECORD_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.workflows.firestore.us_tn_supervision_level_downgrade_record import (
    US_TN_SUPERVISION_LEVEL_DOWNGRADE_RECORD_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.workflows.firestore.us_tn_suspension_of_direct_supervision_record import (
    US_TN_SUSPENSION_OF_DIRECT_SUPERVISION_RECORD_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.workflows.firestore.us_tn_transfer_to_compliant_reporting_2025_policy_record import (
    US_TN_TRANSFER_TO_COMPLIANT_REPORTING_2025_POLICY_RECORD_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.workflows.firestore.us_tn_transfer_to_compliant_reporting_record import (
    US_TN_TRANSFER_TO_COMPLIANT_REPORTING_RECORD_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.workflows.firestore.us_tx_transfer_from_parole_to_annual_reporting_status import (
    US_TX_TRANSFER_FROM_PAROLE_TO_ANNUAL_REPORTING_STATUS_RECORD_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.workflows.firestore.us_tx_transfer_from_parole_to_early_release_from_supervision import (
    US_TX_TRANSFER_FROM_PAROLE_TO_EARLY_RELEASE_FROM_SUPERVISION_RECORD_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.workflows.firestore.us_ut_early_termination_from_supervision_request_record import (
    US_UT_EARLY_TERMINATION_FROM_SUPERVISION_RECORD_VIEW_BUILDER,
)
from recidiviz.common.constants.states import StateCode
from recidiviz.task_eligibility.task_completion_event_big_query_view_builder import (
    TaskCompletionEventType,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

WORKFLOWS_OPPORTUNITY_CONFIGS_VIEW_NAME = "workflows_opportunity_configs"

WORKFLOWS_OPPORTUNITY_CONFIGS_DESCRIPTION = (
    "Configurations for all Workflows opportunities"
)


class PersonRecordType(Enum):
    CLIENT = "client"
    RESIDENT = "resident"


@attr.s
class WorkflowsOpportunityConfig:
    """Configuration for a Workflows opportunity."""

    # The state code the Workflow applies to
    state_code: StateCode = attr.ib()

    # The string used to represent the opportunity in the front-end
    opportunity_type: str = attr.ib()

    # The experiment_id corresponding to this opportunity, which is defined in
    # the static_reference_tables.experiment_state_assignments view
    experiment_id: str = attr.ib()

    # The view builder for this workflow
    view_builder: BigQueryViewBuilder = attr.ib()

    # The task completion event specific to this workflow, used in TES views
    task_completion_event: TaskCompletionEventType = attr.ib()

    # The Firestore collection name used as the destination for the opportunity record export
    export_collection_name: str = attr.ib()

    # The string used to represent this opportunity in the workflows URL
    opportunity_type_path_str: str = attr.ib()

    # The type of person record to add JII's to, either 'client' or 'resident'
    person_record_type: PersonRecordType = attr.ib()

    # The name of the materialized view in workflows_views dataset with records for this workflow
    @property
    def opportunity_record_view_name(self) -> str:
        return self.view_builder.table_for_query.table_id

    # The name of the file in the GCS practices-etl-data bucket that is the source for the daily export to Firestore
    @property
    def source_filename(self) -> str:
        return f"{self.view_builder.view_id}.json"


WORKFLOWS_OPPORTUNITY_CONFIGS = [
    ###############################################
    ## Arkansas - AR
    ###############################################
    WorkflowsOpportunityConfig(
        state_code=StateCode.US_AR,
        opportunity_type="usArInstitutionalWorkerStatus",
        experiment_id="US_AR_INSTITUTIONAL_WORKER_STATUS_WORKFLOWS",
        view_builder=US_AR_INSTITUTIONAL_WORKER_STATUS_VIEW_BUILDER,
        task_completion_event=TaskCompletionEventType.GRANTED_INSTITUTIONAL_WORKER_STATUS,
        export_collection_name="US_AR-InstitutionalWorkerStatusReferrals",
        opportunity_type_path_str="institutionalWorkerStatus",
        person_record_type=PersonRecordType.RESIDENT,
    ),
    WorkflowsOpportunityConfig(
        state_code=StateCode.US_AR,
        opportunity_type="usArWorkRelease",
        experiment_id="US_AR_WORK_RELEASE_WORKFLOWS",
        view_builder=US_AR_WORK_RELEASE_RECORD_VIEW_BUILDER,
        task_completion_event=TaskCompletionEventType.GRANTED_WORK_RELEASE,
        export_collection_name="US_AR-WorkReleaseReferrals",
        opportunity_type_path_str="workRelease",
        person_record_type=PersonRecordType.RESIDENT,
    ),
    ###############################################
    ## Arizona - AZ
    ###############################################
    WorkflowsOpportunityConfig(
        state_code=StateCode.US_AZ,
        opportunity_type="usAzTransferToAdministrativeSupervision",
        experiment_id="US_AZ_TRANSFER_TO_ADMINISTRATIVE_SUPERVISION_WORKFLOWS",
        view_builder=US_AZ_TRANSFER_TO_ADMINISTRATIVE_SUPERVISION_RECORD_VIEW_BUILDER,
        task_completion_event=TaskCompletionEventType.TRANSFER_TO_LIMITED_SUPERVISION,
        export_collection_name="US_AZ-TransferToAdminSupervision",
        opportunity_type_path_str="AdminSupervision",
        person_record_type=PersonRecordType.CLIENT,
    ),
    WorkflowsOpportunityConfig(
        state_code=StateCode.US_AZ,
        opportunity_type="usAzReleaseToDTP",
        experiment_id="US_AZ_RELEASE_TO_DTP_WORKFLOWS",
        view_builder=US_AZ_APPROACHING_ACIS_OR_RECIDIVIZ_DTP_REQUEST_RECORD_VIEW_BUILDER,
        task_completion_event=TaskCompletionEventType.EARLY_RELEASE_TO_DRUG_PROGRAM_NOT_OVERDUE,
        export_collection_name="US_AZ-DTPReferrals",
        opportunity_type_path_str="DTP",
        person_record_type=PersonRecordType.RESIDENT,
    ),
    WorkflowsOpportunityConfig(
        state_code=StateCode.US_AZ,
        opportunity_type="usAzReleaseToTPR",
        experiment_id="US_AZ_RELEASE_TO_TPR_WORKFLOWS",
        view_builder=US_AZ_APPROACHING_ACIS_OR_RECIDIVIZ_TPR_REQUEST_RECORD_VIEW_BUILDER,
        task_completion_event=TaskCompletionEventType.EARLY_RELEASE_TO_COMMUNITY_CONFINEMENT_SUPERVISION_NOT_OVERDUE,
        export_collection_name="US_AZ-TPRReferrals",
        opportunity_type_path_str="TPR",
        person_record_type=PersonRecordType.RESIDENT,
    ),
    WorkflowsOpportunityConfig(
        state_code=StateCode.US_AZ,
        opportunity_type="usAzOverdueForACISDTP",
        experiment_id="OVERDUE_FOR_ACIS_DTP_WORKFLOWS",
        view_builder=US_AZ_OVERDUE_FOR_ACIS_DTP_REQUEST_RECORD_VIEW_BUILDER,
        task_completion_event=TaskCompletionEventType.EARLY_RELEASE_TO_DRUG_PROGRAM_OVERDUE,
        export_collection_name="US_AZ-OverdueForDTPReferrals",
        opportunity_type_path_str="OverdueForDTP",
        person_record_type=PersonRecordType.RESIDENT,
    ),
    WorkflowsOpportunityConfig(
        state_code=StateCode.US_AZ,
        opportunity_type="usAzOverdueForACISTPR",
        experiment_id="OVERDUE_FOR_ACIS_TPR_WORKFLOWS",
        view_builder=US_AZ_OVERDUE_FOR_ACIS_TPR_REQUEST_RECORD_VIEW_BUILDER,
        task_completion_event=TaskCompletionEventType.EARLY_RELEASE_TO_COMMUNITY_CONFINEMENT_SUPERVISION_OVERDUE,
        export_collection_name="US_AZ-OverdueForTPRReferrals",
        opportunity_type_path_str="OverdueForTPR",
        person_record_type=PersonRecordType.RESIDENT,
    ),
    ###############################################
    ## California - CA
    ###############################################
    WorkflowsOpportunityConfig(
        state_code=StateCode.US_CA,
        opportunity_type="usCaSupervisionLevelDowngrade",
        experiment_id="US_CA_SUPERVISION_LEVEL_DOWNGRADE",
        view_builder=US_CA_SUPERVISION_LEVEL_DOWNGRADE_VIEW_BUILDER,
        task_completion_event=TaskCompletionEventType.SUPERVISION_LEVEL_DOWNGRADE,
        export_collection_name="US_CA-supervisionLevelDowngrade",
        opportunity_type_path_str="supervisionLevelDowngrade",
        person_record_type=PersonRecordType.CLIENT,
    ),
    ###############################################
    ## Iowa - IA
    ###############################################
    WorkflowsOpportunityConfig(
        state_code=StateCode.US_IA,
        opportunity_type="usIaEarlyDischarge",
        experiment_id="US_IA_EARLY_DISCHARGE",
        view_builder=US_IA_COMPLETE_EARLY_DISCHARGE_FORM_RECORD_VIEW_BUILDER,
        task_completion_event=TaskCompletionEventType.EARLY_DISCHARGE,
        export_collection_name="US_IA-earlyDischarge",
        opportunity_type_path_str="earlyDischarge",
        person_record_type=PersonRecordType.CLIENT,
    ),
    ###############################################
    ## Idaho - IX
    ###############################################
    WorkflowsOpportunityConfig(
        state_code=StateCode.US_IX,
        opportunity_type="earnedDischarge",
        experiment_id="US_IX_LSU_ED_DISCHARGE_WORKFLOWS",
        view_builder=US_IX_COMPLETE_DISCHARGE_EARLY_FROM_SUPERVISION_REQUEST_RECORD_VIEW_BUILDER,
        task_completion_event=TaskCompletionEventType.EARLY_DISCHARGE,
        export_collection_name="US_ID-earnedDischargeReferrals",
        opportunity_type_path_str="earnedDischarge",
        person_record_type=PersonRecordType.CLIENT,
    ),
    WorkflowsOpportunityConfig(
        state_code=StateCode.US_IX,
        opportunity_type="LSU",
        experiment_id="US_IX_LSU_ED_DISCHARGE_WORKFLOWS",
        view_builder=US_IX_COMPLETE_TRANSFER_TO_LIMITED_SUPERVISION_FORM_RECORD_VIEW_BUILDER,
        task_completion_event=TaskCompletionEventType.TRANSFER_TO_LIMITED_SUPERVISION,
        export_collection_name="US_ID-LSUReferrals",
        opportunity_type_path_str="LSU",
        person_record_type=PersonRecordType.CLIENT,
    ),
    WorkflowsOpportunityConfig(
        state_code=StateCode.US_IX,
        opportunity_type="pastFTRD",
        experiment_id="US_IX_LSU_ED_DISCHARGE_WORKFLOWS",
        view_builder=US_IX_COMPLETE_FULL_TERM_DISCHARGE_FROM_SUPERVISION_REQUEST_RECORD_VIEW_BUILDER,
        task_completion_event=TaskCompletionEventType.FULL_TERM_DISCHARGE,
        export_collection_name="US_ID-pastFTRDReferrals",
        opportunity_type_path_str="pastFTRD",
        person_record_type=PersonRecordType.CLIENT,
    ),
    WorkflowsOpportunityConfig(
        state_code=StateCode.US_IX,
        opportunity_type="usIdSupervisionLevelDowngrade",
        experiment_id="US_IX_SUPERVISION_LEVEL_DOWNGRADE_WORKFLOWS",
        view_builder=US_IX_SUPERVISION_LEVEL_DOWNGRADE_RECORD_VIEW_BUILDER,
        task_completion_event=TaskCompletionEventType.SUPERVISION_LEVEL_DOWNGRADE,
        export_collection_name="US_ID-supervisionLevelDowngrade",
        opportunity_type_path_str="supervisionLevelMismatch",
        person_record_type=PersonRecordType.CLIENT,
    ),
    WorkflowsOpportunityConfig(
        state_code=StateCode.US_IX,
        opportunity_type="usIdCRCResidentWorker",
        experiment_id="US_IX_CRC_WORKFLOWS",
        view_builder=US_IX_TRANSFER_TO_CRC_RESIDENT_WORKER_REQUEST_RECORD_VIEW_BUILDER,
        task_completion_event=TaskCompletionEventType.GRANTED_INSTITUTIONAL_WORKER_STATUS,
        export_collection_name="US_ID-CRCResidentWorkerReferrals",
        opportunity_type_path_str="CRCResidentWorker",
        person_record_type=PersonRecordType.RESIDENT,
    ),
    WorkflowsOpportunityConfig(
        state_code=StateCode.US_IX,
        opportunity_type="usIdCRCWorkRelease",
        experiment_id="US_IX_CRC_WORKFLOWS",
        view_builder=US_IX_TRANSFER_TO_CRC_WORK_RELEASE_REQUEST_RECORD_VIEW_BUILDER,
        task_completion_event=TaskCompletionEventType.GRANTED_WORK_RELEASE,
        export_collection_name="US_ID-CRCWorkReleaseReferrals",
        opportunity_type_path_str="CRCWorkRelease",
        person_record_type=PersonRecordType.RESIDENT,
    ),
    WorkflowsOpportunityConfig(
        state_code=StateCode.US_IX,
        opportunity_type="usIdExpandedCRC",
        experiment_id="US_IX_CRC_WORKFLOWS",
        view_builder=US_IX_TRANSFER_TO_XCRC_REQUEST_RECORD_VIEW_BUILDER,
        task_completion_event=TaskCompletionEventType.RELEASE_TO_COMMUNITY_CONFINEMENT_SUPERVISION,
        export_collection_name="US_ID-expandedCRCReferrals",
        opportunity_type_path_str="expandedCRC",
        person_record_type=PersonRecordType.RESIDENT,
    ),
    WorkflowsOpportunityConfig(
        state_code=StateCode.US_IX,
        opportunity_type="usIdCustodyLevelDowngrade",
        experiment_id="US_IX_CLASSIFICATION_WORKFLOWS",
        view_builder=US_IX_CUSTODY_LEVEL_DOWNGRADE_RECORD_VIEW_BUILDER,
        task_completion_event=TaskCompletionEventType.CUSTODY_LEVEL_DOWNGRADE,
        export_collection_name="US_ID-custodyLevelDowngradeReferrals",
        opportunity_type_path_str="custodyLevelDowngrade",
        person_record_type=PersonRecordType.RESIDENT,
    ),
    ###############################################
    ## Maine - ME
    ###############################################
    WorkflowsOpportunityConfig(
        state_code=StateCode.US_ME,
        opportunity_type="usMeEarlyTermination",
        experiment_id="US_ME_EARLY_TERMINATION_WORKFLOWS",
        view_builder=US_ME_COMPLETE_EARLY_TERMINATION_RECORD_VIEW_BUILDER,
        task_completion_event=TaskCompletionEventType.EARLY_DISCHARGE,
        export_collection_name="US_ME-earlyTerminationReferrals",
        opportunity_type_path_str="earlyTermination",
        person_record_type=PersonRecordType.CLIENT,
    ),
    WorkflowsOpportunityConfig(
        state_code=StateCode.US_ME,
        opportunity_type="usMeOverdueForDischarge",
        experiment_id="US_ME_OVERDUE_FOR_DISCHARGE_WORKFLOWS",
        view_builder=US_ME_OVERDUE_FOR_DISCHARGE_VIEW_BUILDER,
        task_completion_event=TaskCompletionEventType.FULL_TERM_DISCHARGE,
        export_collection_name="US_ME-overdueForDischargeReferrals",
        opportunity_type_path_str="overdueForDischarge",
        person_record_type=PersonRecordType.CLIENT,
    ),
    WorkflowsOpportunityConfig(
        state_code=StateCode.US_ME,
        opportunity_type="usMeSCCP",
        experiment_id="US_ME_SCCP_WORKFLOWS",
        view_builder=US_ME_TRANSFER_TO_SCCP_RECORD_VIEW_BUILDER,
        task_completion_event=TaskCompletionEventType.RELEASE_TO_COMMUNITY_CONFINEMENT_SUPERVISION,
        export_collection_name="US_ME-SCCPReferrals",
        opportunity_type_path_str="SCCP",
        person_record_type=PersonRecordType.RESIDENT,
    ),
    WorkflowsOpportunityConfig(
        state_code=StateCode.US_ME,
        opportunity_type="usMeWorkRelease",
        experiment_id="US_ME_WORK_RELEASE_WORKFLOWS",
        view_builder=US_ME_COMPLETE_WORK_RELEASE_RECORD_VIEW_BUILDER,
        task_completion_event=TaskCompletionEventType.GRANTED_WORK_RELEASE,
        export_collection_name="US_ME-workReleaseReferrals",
        opportunity_type_path_str="workRelease",
        person_record_type=PersonRecordType.RESIDENT,
    ),
    WorkflowsOpportunityConfig(
        state_code=StateCode.US_ME,
        opportunity_type="usMeFurloughRelease",
        experiment_id="US_ME_FURLOUGH_RELEASE_WORKFLOWS",
        view_builder=US_ME_COMPLETE_FURLOUGH_RELEASE_RECORD_VIEW_BUILDER,
        task_completion_event=TaskCompletionEventType.GRANTED_FURLOUGH,
        export_collection_name="US_ME-furloughReleaseReferrals",
        opportunity_type_path_str="furloughRelease",
        person_record_type=PersonRecordType.RESIDENT,
    ),
    WorkflowsOpportunityConfig(
        state_code=StateCode.US_ME,
        opportunity_type="usMeMediumTrustee",
        experiment_id="US_ME_CUSTODY_LEVEL_DOWNGRADE_TO_MEDIUM_TRUSTEE_WORKFLOWS",
        view_builder=US_ME_CUSTODY_LEVEL_DOWNGRADE_TO_MEDIUM_TRUSTEE_RECORD_VIEW_BUILDER,
        task_completion_event=TaskCompletionEventType.CUSTODY_LEVEL_DOWNGRADE_TO_MEDIUM_TRUSTEE,
        export_collection_name="US_ME-mediumTrusteeReferrals",
        opportunity_type_path_str="mediumTrustee",
        person_record_type=PersonRecordType.RESIDENT,
    ),
    WorkflowsOpportunityConfig(
        state_code=StateCode.US_ME,
        opportunity_type="usMeReclassificationReview",
        experiment_id="US_ME_CUSTODY_RECLASSIFICATION_REVIEW_WORKFLOWS",
        view_builder=US_ME_RECLASSIFICATION_REVIEW_FORM_RECORD_VIEW_BUILDER,
        task_completion_event=TaskCompletionEventType.INCARCERATION_ASSESSMENT_COMPLETED,
        export_collection_name="US_ME-reclassificationReviewReferrals",
        opportunity_type_path_str="annualReclassification",
        person_record_type=PersonRecordType.RESIDENT,
    ),
    ###############################################
    ## Michigan - MI
    ###############################################
    WorkflowsOpportunityConfig(
        state_code=StateCode.US_MI,
        opportunity_type="usMiClassificationReview",
        experiment_id="US_MI_SUPERVISION_LEVEL_DOWNGRADE_WORKFLOWS",
        view_builder=US_MI_COMPLETE_CLASSIFICATION_REVIEW_FORM_RECORD_VIEW_BUILDER,
        task_completion_event=TaskCompletionEventType.SUPERVISION_LEVEL_DOWNGRADE_AFTER_INITIAL_CLASSIFICATION_REVIEW_DATE,
        export_collection_name="US_MI-classificationReviewReferrals",
        opportunity_type_path_str="classificationReview",
        person_record_type=PersonRecordType.CLIENT,
    ),
    WorkflowsOpportunityConfig(
        state_code=StateCode.US_MI,
        opportunity_type="usMiEarlyDischarge",
        experiment_id="US_MI_EARLY_DISCHARGE_WORKFLOWS",
        view_builder=US_MI_COMPLETE_DISCHARGE_EARLY_FROM_SUPERVISION_REQUEST_RECORD_VIEW_BUILDER,
        task_completion_event=TaskCompletionEventType.EARLY_DISCHARGE,
        export_collection_name="US_MI-earlyDischargeReferrals",
        opportunity_type_path_str="earlyDischarge",
        person_record_type=PersonRecordType.CLIENT,
    ),
    WorkflowsOpportunityConfig(
        state_code=StateCode.US_MI,
        opportunity_type="usMiMinimumTelephoneReporting",
        experiment_id="US_MI_MINIMUM_TELEPHONE_REPORTING_WORKFLOWS",
        view_builder=US_MI_COMPLETE_TRANSFER_TO_TELEPHONE_REPORTING_REQUEST_RECORD_VIEW_BUILDER,
        task_completion_event=TaskCompletionEventType.TRANSFER_TO_LIMITED_SUPERVISION,
        export_collection_name="US_MI-minimumTelephoneReporting",
        opportunity_type_path_str="minimumTelephoneReporting",
        person_record_type=PersonRecordType.CLIENT,
    ),
    WorkflowsOpportunityConfig(
        state_code=StateCode.US_MI,
        opportunity_type="usMiPastFTRD",
        experiment_id="US_MI_PAST_FTRD_WORKFLOWS",
        view_builder=US_MI_COMPLETE_FULL_TERM_DISCHARGE_FROM_SUPERVISION_REQUEST_RECORD_VIEW_BUILDER,
        task_completion_event=TaskCompletionEventType.FULL_TERM_DISCHARGE,
        export_collection_name="US_MI-pastFTRDReferrals",
        opportunity_type_path_str="pastFTRD",
        person_record_type=PersonRecordType.CLIENT,
    ),
    WorkflowsOpportunityConfig(
        state_code=StateCode.US_MI,
        opportunity_type="usMiSupervisionLevelDowngrade",
        experiment_id="US_MI_SUPERVISION_LEVEL_DOWNGRADE_WORKFLOWS",
        view_builder=US_MI_SUPERVISION_LEVEL_DOWNGRADE_RECORD_VIEW_BUILDER,
        task_completion_event=TaskCompletionEventType.SUPERVISION_LEVEL_DOWNGRADE_BEFORE_INITIAL_CLASSIFICATION_REVIEW_DATE,
        export_collection_name="US_MI-supervisionLevelDowngrade",
        opportunity_type_path_str="supervisionLevelMismatch",
        person_record_type=PersonRecordType.CLIENT,
    ),
    WorkflowsOpportunityConfig(
        state_code=StateCode.US_MI,
        opportunity_type="usMiSecurityClassificationCommitteeReview",
        experiment_id="US_MI_COMPLETE_SECURITY_CLASSIFICATION_COMMITTEE_REVIEW_FORM",
        view_builder=US_MI_COMPLETE_SECURITY_CLASSIFICATION_COMMITTEE_REVIEW_FORM_RECORD_VIEW_BUILDER,
        task_completion_event=TaskCompletionEventType.SECURITY_CLASSIFICATION_COMMITTEE_REVIEW,
        export_collection_name="US_MI-securityClassificationCommitteeReview",
        opportunity_type_path_str="securityClassificationCommitteeReview",
        person_record_type=PersonRecordType.RESIDENT,
    ),
    WorkflowsOpportunityConfig(
        state_code=StateCode.US_MI,
        opportunity_type="usMiAddInPersonSecurityClassificationCommitteeReview",
        experiment_id="US_MI_COMPLETE_ADD_IN_PERSON_SECURITY_CLASSIFICATION_COMMITTEE_REVIEW_FORM",
        view_builder=US_MI_COMPLETE_ADD_IN_PERSON_SECURITY_CLASSIFICATION_COMMITTEE_REVIEW_FORM_RECORD_VIEW_BUILDER,
        task_completion_event=TaskCompletionEventType.ADD_IN_PERSON_SECURITY_CLASSIFICATION_COMMITTEE_REVIEW,
        export_collection_name="US_MI-addInPersonSecurityClassificationCommitteeReview",
        opportunity_type_path_str="addInPersonSecurityClassificationCommitteeReview",
        person_record_type=PersonRecordType.RESIDENT,
    ),
    WorkflowsOpportunityConfig(
        state_code=StateCode.US_MI,
        opportunity_type="usMiWardenInPersonSecurityClassificationCommitteeReview",
        experiment_id="US_MI_COMPLETE_WARDEN_IN_PERSON_SECURITY_CLASSIFICATION_COMMITTEE_REVIEW_FORM",
        view_builder=US_MI_COMPLETE_WARDEN_IN_PERSON_SECURITY_CLASSIFICATION_COMMITTEE_REVIEW_FORM_RECORD_VIEW_BUILDER,
        task_completion_event=TaskCompletionEventType.WARDEN_IN_PERSON_SECURITY_CLASSIFICATION_COMMITTEE_REVIEW,
        export_collection_name="US_MI-wardenInPersonSecurityClassificationCommitteeReview",
        opportunity_type_path_str="wardenInPersonSecurityClassificationCommitteeReview",
        person_record_type=PersonRecordType.RESIDENT,
    ),
    WorkflowsOpportunityConfig(
        state_code=StateCode.US_MI,
        opportunity_type="usMiReclassificationRequest",
        experiment_id="US_MI_COMPLETE_RECLASSIFICATION_TO_GENERAL_POPULATION_REQUEST",
        view_builder=US_MI_COMPLETE_RECLASSIFICATION_TO_GENERAL_POPULATION_REQUEST_RECORD_VIEW_BUILDER,
        task_completion_event=TaskCompletionEventType.TRANSFER_OUT_OF_DISCIPLINARY_OR_TEMPORARY_SOLITARY_CONFINEMENT,
        export_collection_name="US_MI-reclassificationRequest",
        opportunity_type_path_str="reclassificationRequest",
        person_record_type=PersonRecordType.RESIDENT,
    ),
    ###############################################
    ## Missouri - MO
    ###############################################
    WorkflowsOpportunityConfig(
        state_code=StateCode.US_MO,
        opportunity_type="usMoOverdueRestrictiveHousingRelease",
        experiment_id="US_MO_RESTRICTIVE_HOUSING_WORKFLOWS",
        view_builder=US_MO_OVERDUE_RESTRICTIVE_HOUSING_RELEASE_RECORD_VIEW_BUILDER,
        task_completion_event=TaskCompletionEventType.TRANSFER_OUT_OF_SOLITARY_CONFINEMENT,
        export_collection_name="US_MO-overdueRestrictiveHousingReleaseReferrals",
        opportunity_type_path_str="overdueRestrictiveHousingRelease",
        person_record_type=PersonRecordType.RESIDENT,
    ),
    WorkflowsOpportunityConfig(
        state_code=StateCode.US_MO,
        opportunity_type="usMoOverdueRestrictiveHousingInitialHearing",
        experiment_id="US_MO_RESTRICTIVE_HOUSING_WORKFLOWS",
        view_builder=US_MO_OVERDUE_RESTRICTIVE_HOUSING_INITIAL_HEARING_RECORD_VIEW_BUILDER,
        task_completion_event=TaskCompletionEventType.HEARING_OCCURRED,
        export_collection_name="US_MO-overdueRestrictiveHousingInitialHearingReferrals",
        opportunity_type_path_str="overdueRestrictiveHousingInitialHearing",
        person_record_type=PersonRecordType.RESIDENT,
    ),
    WorkflowsOpportunityConfig(
        state_code=StateCode.US_MO,
        opportunity_type="usMoOverdueRestrictiveHousingReviewHearing",
        experiment_id="US_MO_RESTRICTIVE_HOUSING_WORKFLOWS",
        view_builder=US_MO_OVERDUE_RESTRICTIVE_HOUSING_REVIEW_HEARING_RECORD_VIEW_BUILDER,
        task_completion_event=TaskCompletionEventType.HEARING_OCCURRED,
        export_collection_name="US_MO-overdueRestrictiveHousingReviewHearingReferrals",
        opportunity_type_path_str="overdueRestrictiveHousingReviewHearing",
        person_record_type=PersonRecordType.RESIDENT,
    ),
    ###############################################
    ## North Dakota - ND
    ###############################################
    WorkflowsOpportunityConfig(
        state_code=StateCode.US_ND,
        opportunity_type="earlyTermination",
        experiment_id="US_ND_EARLY_TERMINATION_WORKFLOWS",
        view_builder=US_ND_COMPLETE_DISCHARGE_EARLY_FROM_SUPERVISION_RECORD_VIEW_BUILDER,
        task_completion_event=TaskCompletionEventType.EARLY_DISCHARGE,
        export_collection_name="earlyTerminationReferrals",
        opportunity_type_path_str="earlyTermination",
        person_record_type=PersonRecordType.CLIENT,
    ),
    WorkflowsOpportunityConfig(
        state_code=StateCode.US_ND,
        opportunity_type="usNdATP",
        experiment_id="US_ND_TRANSFER_TO_ATP_WORKFLOWS",
        view_builder=US_ND_TRANSFER_TO_ATP_FORM_RECORD_VIEW_BUILDER,
        task_completion_event=TaskCompletionEventType.GRANTED_WORK_RELEASE,
        export_collection_name="US_ND-AtpReferrals",
        opportunity_type_path_str="ATP",
        person_record_type=PersonRecordType.RESIDENT,
    ),
    WorkflowsOpportunityConfig(
        state_code=StateCode.US_ND,
        opportunity_type="usNdTransferToMinFacility",
        experiment_id="US_ND_TRANSFER_TO_MINIMUM_FACILITY_WORKFLOWS",
        view_builder=US_ND_TRANSFER_TO_MINIMUM_FACILITY_VIEW_BUILDER,
        task_completion_event=TaskCompletionEventType.TRANSFER_TO_MINIMUM_FACILITY,
        export_collection_name="US_ND-TransferToMinFacility",
        opportunity_type_path_str="TransferToMinFacility",
        person_record_type=PersonRecordType.RESIDENT,
    ),
    ###############################################
    ## Nebraska - NE
    ###############################################
    WorkflowsOpportunityConfig(
        state_code=StateCode.US_NE,
        opportunity_type="usNeConditionalLowRiskOverride",
        experiment_id="US_NE_CONDITIONAL_LOW_RISK_OVERRIDE_WORKFLOWS",
        view_builder=US_NE_OVERRIDE_LOW_TO_CONDITIONAL_LOW_RECORD_VIEW_BUILDER,
        task_completion_event=TaskCompletionEventType.OVERRIDE_TO_CONDITIONAL_LOW_RISK_SUPERVISION,
        export_collection_name="US_NE-ConditionalLowRiskOverrideReferrals",
        opportunity_type_path_str="ConditionalLowRiskOverride",
        person_record_type=PersonRecordType.CLIENT,
    ),
    WorkflowsOpportunityConfig(
        state_code=StateCode.US_NE,
        opportunity_type="usNeOverrideModerateToLow",
        experiment_id="US_NE_OVERRIDE_MODERATE_TO_LOW_WORKFLOWS",
        view_builder=US_NE_OVERRIDE_MODERATE_TO_LOW_RECORD_VIEW_BUILDER,
        task_completion_event=TaskCompletionEventType.OVERRIDE_TO_LOW_SUPERVISION,
        export_collection_name="US_NE-OverrideModerateToLowReferrals",
        opportunity_type_path_str="OverrideModerateToLow",
        person_record_type=PersonRecordType.CLIENT,
    ),
    ###############################################
    ## Pennsylvania - PA
    ###############################################
    WorkflowsOpportunityConfig(
        state_code=StateCode.US_PA,
        opportunity_type="usPaAdminSupervision",
        experiment_id="US_PA_ADMIN_SUPERVISION_WORKFLOWS",
        view_builder=US_PA_TRANSFER_TO_ADMINISTRATIVE_SUPERVISION_FORM_RECORD_VIEW_BUILDER,
        task_completion_event=TaskCompletionEventType.TRANSFER_TO_ADMINISTRATIVE_SUPERVISION,
        export_collection_name="US_PA-adminSupervisionReferrals",
        opportunity_type_path_str="adminSupervision",
        person_record_type=PersonRecordType.CLIENT,
    ),
    WorkflowsOpportunityConfig(
        state_code=StateCode.US_PA,
        opportunity_type="usPaSpecialCircumstancesSupervision",
        experiment_id="US_PA_SPECIAL_CIRCUMSTANCES_SUPERVISION_WORKFLOWS",
        view_builder=US_PA_COMPLETE_TRANSFER_TO_SPECIAL_CIRCUMSTANCES_SUPERVISION_REQUEST_RECORD_VIEW_BUILDER,
        task_completion_event=TaskCompletionEventType.TRANSFER_TO_SPECIAL_CIRCUMSTANCES_SUPERVISION,
        export_collection_name="US_PA-specialCircumstancesSupervisionReferrals",
        opportunity_type_path_str="specialCircumstancesSupervisionReferrals",
        person_record_type=PersonRecordType.CLIENT,
    ),
    ###############################################
    ## Tennessee - TN
    ###############################################
    WorkflowsOpportunityConfig(
        state_code=StateCode.US_TN,
        opportunity_type="compliantReporting",
        experiment_id="US_TN_COMPLIANT_REPORTING_WORKFLOWS",
        view_builder=US_TN_TRANSFER_TO_COMPLIANT_REPORTING_RECORD_VIEW_BUILDER,
        task_completion_event=TaskCompletionEventType.TRANSFER_TO_LIMITED_SUPERVISION,
        export_collection_name="compliantReportingReferrals",
        opportunity_type_path_str="compliantReporting",
        person_record_type=PersonRecordType.CLIENT,
    ),
    WorkflowsOpportunityConfig(
        state_code=StateCode.US_TN,
        opportunity_type="usTnCompliantReporting2025Policy",
        experiment_id="US_TN_COMPLIANT_REPORTING_2025_POLICY_WORKFLOWS",
        view_builder=US_TN_TRANSFER_TO_COMPLIANT_REPORTING_2025_POLICY_RECORD_VIEW_BUILDER,
        task_completion_event=TaskCompletionEventType.TRANSFER_TO_LIMITED_SUPERVISION,
        export_collection_name="usTnCompliantReporting2025PolicyReferrals",
        opportunity_type_path_str="usTnCompliantReporting2025Policy",
        person_record_type=PersonRecordType.CLIENT,
    ),
    WorkflowsOpportunityConfig(
        state_code=StateCode.US_TN,
        opportunity_type="usTnCustodyLevelDowngrade",
        experiment_id="US_TN_CLASSIFICATION_WORKFLOWS",
        view_builder=US_TN_CUSTODY_LEVEL_DOWNGRADE_RECORD_VIEW_BUILDER,
        task_completion_event=TaskCompletionEventType.CUSTODY_LEVEL_DOWNGRADE,
        export_collection_name="US_TN-custodyLevelDowngradeReferrals",
        opportunity_type_path_str="custodyLevelDowngrade",
        person_record_type=PersonRecordType.RESIDENT,
    ),
    WorkflowsOpportunityConfig(
        state_code=StateCode.US_TN,
        opportunity_type="usTnExpiration",
        experiment_id="US_TN_DISCHARGE_WORKFLOWS",
        view_builder=US_TN_FULL_TERM_SUPERVISION_DISCHARGE_RECORD_VIEW_BUILDER,
        task_completion_event=TaskCompletionEventType.FULL_TERM_DISCHARGE,
        export_collection_name="US_TN-expirationReferrals",
        opportunity_type_path_str="expiration",
        person_record_type=PersonRecordType.CLIENT,
    ),
    WorkflowsOpportunityConfig(
        state_code=StateCode.US_TN,
        opportunity_type="supervisionLevelDowngrade",
        experiment_id="US_TN_SUPERVISION_LEVEL_DOWNGRADE_WORKFLOWS",
        view_builder=US_TN_SUPERVISION_LEVEL_DOWNGRADE_RECORD_VIEW_BUILDER,
        task_completion_event=TaskCompletionEventType.SUPERVISION_LEVEL_DOWNGRADE,
        export_collection_name="US_TN-supervisionLevelDowngrade",
        opportunity_type_path_str="supervisionLevelDowngrade",
        person_record_type=PersonRecordType.CLIENT,
    ),
    WorkflowsOpportunityConfig(
        state_code=StateCode.US_TN,
        opportunity_type="usTnSuspensionOfDirectSupervision",
        experiment_id="US_TN_SUSPENSION_OF_DIRECT_SUPERVISION_WORKFLOWS",
        view_builder=US_TN_SUSPENSION_OF_DIRECT_SUPERVISION_RECORD_VIEW_BUILDER,
        task_completion_event=TaskCompletionEventType.TRANSFER_TO_NO_CONTACT_PAROLE,
        export_collection_name="US_TN-suspensionOfDirectSupervisionReferrals",
        opportunity_type_path_str="suspensionOfDirectSupervision",
        person_record_type=PersonRecordType.CLIENT,
    ),
    WorkflowsOpportunityConfig(
        state_code=StateCode.US_TN,
        opportunity_type="usTnAnnualReclassification",
        experiment_id="US_TN_CLASSIFICATION_WORKFLOWS",
        view_builder=US_TN_ANNUAL_RECLASSIFICATION_REVIEW_RECORD_VIEW_BUILDER,
        task_completion_event=TaskCompletionEventType.INCARCERATION_ASSESSMENT_COMPLETED,
        export_collection_name="US_TN-annualReclassificationReferrals",
        opportunity_type_path_str="annualReclassification",
        person_record_type=PersonRecordType.RESIDENT,
    ),
    WorkflowsOpportunityConfig(
        state_code=StateCode.US_TN,
        opportunity_type="usTnInitialClassification",
        experiment_id="US_TN_CLASSIFICATION_WORKFLOWS",
        view_builder=US_TN_INITIAL_CLASSIFICATION_REVIEW_RECORD_VIEW_BUILDER,
        task_completion_event=TaskCompletionEventType.INCARCERATION_ASSESSMENT_COMPLETED,
        export_collection_name="US_TN-initialClassificationReferrals",
        opportunity_type_path_str="initialClassification",
        person_record_type=PersonRecordType.RESIDENT,
    ),
    ###############################################
    ## Texas - TX
    ###############################################
    WorkflowsOpportunityConfig(
        state_code=StateCode.US_TX,
        opportunity_type="usTxAnnualReportingStatus",
        experiment_id="US_TX_TRANSFER_FROM_PAROLE_TO_ANNUAL_REPORTING_STATUS_WORKFLOWS",
        view_builder=US_TX_TRANSFER_FROM_PAROLE_TO_ANNUAL_REPORTING_STATUS_RECORD_VIEW_BUILDER,
        task_completion_event=TaskCompletionEventType.TRANSFER_TO_LIMITED_SUPERVISION,
        export_collection_name="US_TX-annualReportingStatusReferrals",
        opportunity_type_path_str="AnnualReportingStatus",
        person_record_type=PersonRecordType.CLIENT,
    ),
    WorkflowsOpportunityConfig(
        state_code=StateCode.US_TX,
        opportunity_type="usTxEarlyReleaseFromSupervision",
        experiment_id="US_TX_TRANSFER_FROM_PAROLE_TO_EARLY_RELEASE_FROM_SUPERVISION_WORKFLOWS",
        view_builder=US_TX_TRANSFER_FROM_PAROLE_TO_EARLY_RELEASE_FROM_SUPERVISION_RECORD_VIEW_BUILDER,
        task_completion_event=TaskCompletionEventType.TRANSFER_TO_UNSUPERVISED_PAROLE,
        export_collection_name="US_TX-earlyReleaseFromSupervisionReferrals",
        opportunity_type_path_str="EarlyReleaseFromSupervision",
        person_record_type=PersonRecordType.CLIENT,
    ),
    ###############################################
    ## Utah - UT
    ###############################################
    WorkflowsOpportunityConfig(
        state_code=StateCode.US_UT,
        opportunity_type="usUtEarlyTermination",
        experiment_id="US_UT_EARLY_TERMINATION_FROM_SUPERVISION_WORKFLOWS",
        view_builder=US_UT_EARLY_TERMINATION_FROM_SUPERVISION_RECORD_VIEW_BUILDER,
        task_completion_event=TaskCompletionEventType.EARLY_DISCHARGE,
        export_collection_name="US_UT-earlyTerminationReferrals",
        opportunity_type_path_str="EarlyTermination",
        person_record_type=PersonRecordType.CLIENT,
    ),
]

WORKFLOWS_OPPORTUNITY_CONFIG_MAP = {
    config.opportunity_type: config for config in WORKFLOWS_OPPORTUNITY_CONFIGS
}

WORKFLOWS_OPPORTUNITY_CONFIGS_QUERY_TEMPLATE = "UNION ALL".join(
    [
        f"""
SELECT
    "{config.state_code.value}" AS state_code,
    "{config.opportunity_type}" AS opportunity_type,
    "{config.experiment_id}" AS experiment_id,
    "{config.opportunity_record_view_name}" AS opportunity_record_view_name,
    "{config.task_completion_event.name}" AS completion_event_type,
    "{config.source_filename}" AS source_filename,
    "{config.export_collection_name}" AS export_collection_name,
    "{config.opportunity_type_path_str}" AS opportunity_type_path_str,
    "{config.person_record_type.name}" AS person_record_type,
"""
        for config in WORKFLOWS_OPPORTUNITY_CONFIGS
    ]
)

WORKFLOWS_OPPORTUNITY_CONFIGS_VIEW_BUILDER = SimpleBigQueryViewBuilder(
    dataset_id=REFERENCE_VIEWS_DATASET,
    view_id=WORKFLOWS_OPPORTUNITY_CONFIGS_VIEW_NAME,
    view_query_template=WORKFLOWS_OPPORTUNITY_CONFIGS_QUERY_TEMPLATE,
    description=WORKFLOWS_OPPORTUNITY_CONFIGS_DESCRIPTION,
    should_materialize=True,
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        WORKFLOWS_OPPORTUNITY_CONFIGS_VIEW_BUILDER.build_and_print()
