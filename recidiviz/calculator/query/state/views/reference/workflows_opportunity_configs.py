# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2023 Recidiviz, Inc.
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

from recidiviz.big_query.big_query_view import SimpleBigQueryViewBuilder
from recidiviz.calculator.query.state.dataset_config import REFERENCE_VIEWS_DATASET
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
    # The state code the Workflow applies to
    state_code: StateCode = attr.ib()

    # The string used to represent the opportunity in the front-end
    opportunity_type: str = attr.ib()

    # The experiment_id corresponding to this opportunity, which is defined in
    # the static_reference_tables.experiment_state_assignments view
    experiment_id: str = attr.ib()

    # The name of the materialized view in workflows_views dataset with records for this workflow
    opportunity_record_view_name: str = attr.ib()

    # The task completion event specific to this workflow, used in TES views
    task_completion_event: TaskCompletionEventType = attr.ib()

    # The name of the file in the GCS practices-etl-data bucket that is the source for the daily export to Firestore
    # TODO(Recidiviz/recidiviz-dashboards#5003): Once CR TES migration is completed, this field can be consolidated with opportunity_record_view_name
    source_filename: str = attr.ib()

    # The Firestore collection name used as the destination for the opportunity record export
    export_collection_name: str = attr.ib()

    # The string used to represent this opportunity in the workflows URL
    opportunity_type_path_str: str = attr.ib()

    # The type of person record to add JII's to, either 'client' or 'resident'
    person_record_type: PersonRecordType = attr.ib()


WORKFLOWS_OPPORTUNITY_CONFIGS = [
    WorkflowsOpportunityConfig(
        state_code=StateCode.US_AZ,
        opportunity_type="usAzReleaseToTPR",
        experiment_id="US_AZ_RELEASE_TO_TPR_WORKFLOWS",
        opportunity_record_view_name="us_az_approaching_acis_or_recidiviz_tpr_request_record_materialized",
        # TODO(#33655): Update this to the correct task completion event
        task_completion_event=TaskCompletionEventType.EARLY_DISCHARGE,
        source_filename="us_az_approaching_acis_or_recidiviz_tpr_request_record.json",
        export_collection_name="US_AZ-TPRReferrals",
        opportunity_type_path_str="TPR",
        person_record_type=PersonRecordType.RESIDENT,
    ),
    WorkflowsOpportunityConfig(
        state_code=StateCode.US_AZ,
        opportunity_type="usAzOverdueForACISDTP",
        experiment_id="OVERDUE_FOR_ACIS_DTP_WORKFLOWS",
        opportunity_record_view_name="us_az_overdue_for_acis_dtp_request_record_materialized",
        # TODO(#33655): Correct task completion event
        task_completion_event=TaskCompletionEventType.EARLY_DISCHARGE,
        source_filename="us_az_overdue_for_acis_dtp_request_record.json",
        export_collection_name="US_AZ-OverdueForDTPReferrals",
        opportunity_type_path_str="OverdueForDTP",
        person_record_type=PersonRecordType.RESIDENT,
    ),
    WorkflowsOpportunityConfig(
        state_code=StateCode.US_AZ,
        opportunity_type="usAzOverdueForACISTPR",
        experiment_id="OVERDUE_FOR_ACIS_TPR_WORKFLOWS",
        opportunity_record_view_name="us_az_overdue_for_acis_tpr_request_record_materialized",
        # TODO(#33655): Correct task completion event
        task_completion_event=TaskCompletionEventType.EARLY_DISCHARGE,
        source_filename="us_az_overdue_for_acis_tpr_request_record.json",
        export_collection_name="US_AZ-OverdueForTPRReferrals",
        opportunity_type_path_str="OverdueForTPR",
        person_record_type=PersonRecordType.RESIDENT,
    ),
    WorkflowsOpportunityConfig(
        state_code=StateCode.US_CA,
        opportunity_type="usCaSupervisionLevelDowngrade",
        experiment_id="US_CA_SUPERVISION_LEVEL_DOWNGRADE",
        opportunity_record_view_name="us_ca_supervision_level_downgrade_form_record_materialized",
        task_completion_event=TaskCompletionEventType.SUPERVISION_LEVEL_DOWNGRADE,
        source_filename="us_ca_supervision_level_downgrade_form_record.json",
        export_collection_name="US_CA-supervisionLevelDowngrade",
        opportunity_type_path_str="supervisionLevelDowngrade",
        person_record_type=PersonRecordType.CLIENT,
    ),
    WorkflowsOpportunityConfig(
        state_code=StateCode.US_IX,
        opportunity_type="earnedDischarge",
        experiment_id="US_IX_LSU_ED_DISCHARGE_WORKFLOWS",
        opportunity_record_view_name="us_ix_complete_discharge_early_from_supervision_request_record_materialized",
        task_completion_event=TaskCompletionEventType.EARLY_DISCHARGE,
        source_filename="us_ix_complete_discharge_early_from_supervision_request_record.json",
        export_collection_name="US_ID-earnedDischargeReferrals",
        opportunity_type_path_str="earnedDischarge",
        person_record_type=PersonRecordType.CLIENT,
    ),
    WorkflowsOpportunityConfig(
        state_code=StateCode.US_IX,
        opportunity_type="LSU",
        experiment_id="US_IX_LSU_ED_DISCHARGE_WORKFLOWS",
        opportunity_record_view_name="us_ix_complete_transfer_to_limited_supervision_form_record_materialized",
        task_completion_event=TaskCompletionEventType.TRANSFER_TO_LIMITED_SUPERVISION,
        source_filename="us_ix_complete_transfer_to_limited_supervision_form_record.json",
        export_collection_name="US_ID-LSUReferrals",
        opportunity_type_path_str="LSU",
        person_record_type=PersonRecordType.CLIENT,
    ),
    WorkflowsOpportunityConfig(
        state_code=StateCode.US_IX,
        opportunity_type="pastFTRD",
        experiment_id="US_IX_LSU_ED_DISCHARGE_WORKFLOWS",
        opportunity_record_view_name="us_ix_complete_full_term_discharge_from_supervision_request_record_materialized",
        task_completion_event=TaskCompletionEventType.FULL_TERM_DISCHARGE,
        source_filename="us_ix_complete_full_term_discharge_from_supervision_request_record.json",
        export_collection_name="US_ID-pastFTRDReferrals",
        opportunity_type_path_str="pastFTRD",
        person_record_type=PersonRecordType.CLIENT,
    ),
    WorkflowsOpportunityConfig(
        state_code=StateCode.US_IX,
        opportunity_type="usIdSupervisionLevelDowngrade",
        experiment_id="US_IX_SUPERVISION_LEVEL_DOWNGRADE_WORKFLOWS",
        opportunity_record_view_name="us_ix_supervision_level_downgrade_record_materialized",
        task_completion_event=TaskCompletionEventType.SUPERVISION_LEVEL_DOWNGRADE,
        source_filename="us_ix_supervision_level_downgrade_record.json",
        export_collection_name="US_ID-supervisionLevelDowngrade",
        opportunity_type_path_str="supervisionLevelMismatch",
        person_record_type=PersonRecordType.CLIENT,
    ),
    WorkflowsOpportunityConfig(
        state_code=StateCode.US_IX,
        opportunity_type="usIdCRCResidentWorker",
        experiment_id="US_IX_CRC_WORKFLOWS",
        opportunity_record_view_name="us_ix_transfer_to_crc_resident_worker_request_record_materialized",
        task_completion_event=TaskCompletionEventType.TRANSFER_TO_MINIMUM_FACILITY,
        source_filename="us_ix_transfer_to_crc_resident_worker_request_record.json",
        export_collection_name="US_ID-CRCResidentWorkerReferrals",
        opportunity_type_path_str="CRCResidentWorker",
        person_record_type=PersonRecordType.RESIDENT,
    ),
    WorkflowsOpportunityConfig(
        state_code=StateCode.US_IX,
        opportunity_type="usIdCRCWorkRelease",
        experiment_id="US_IX_CRC_WORKFLOWS",
        opportunity_record_view_name="us_ix_transfer_to_crc_work_release_request_record_materialized",
        task_completion_event=TaskCompletionEventType.GRANTED_WORK_RELEASE,
        source_filename="us_ix_transfer_to_crc_work_release_request_record.json",
        export_collection_name="US_ID-CRCWorkReleaseReferrals",
        opportunity_type_path_str="CRCWorkRelease",
        person_record_type=PersonRecordType.RESIDENT,
    ),
    WorkflowsOpportunityConfig(
        state_code=StateCode.US_IX,
        opportunity_type="usIdExpandedCRC",
        experiment_id="US_IX_CRC_WORKFLOWS",
        opportunity_record_view_name="us_ix_transfer_to_xcrc_request_record_materialized",
        task_completion_event=TaskCompletionEventType.RELEASE_TO_COMMUNITY_CONFINEMENT_SUPERVISION,
        source_filename="us_ix_transfer_to_xcrc_request_record.json",
        export_collection_name="US_ID-expandedCRCReferrals",
        opportunity_type_path_str="expandedCRC",
        person_record_type=PersonRecordType.RESIDENT,
    ),
    WorkflowsOpportunityConfig(
        state_code=StateCode.US_ME,
        opportunity_type="usMeEarlyTermination",
        experiment_id="US_ME_EARLY_TERMINATION_WORKFLOWS",
        opportunity_record_view_name="us_me_complete_early_termination_record_materialized",
        task_completion_event=TaskCompletionEventType.EARLY_DISCHARGE,
        source_filename="us_me_complete_early_termination_record.json",
        export_collection_name="US_ME-earlyTerminationReferrals",
        opportunity_type_path_str="earlyTermination",
        person_record_type=PersonRecordType.CLIENT,
    ),
    WorkflowsOpportunityConfig(
        state_code=StateCode.US_ME,
        opportunity_type="usMeSCCP",
        experiment_id="US_ME_SCCP_WORKFLOWS",
        opportunity_record_view_name="us_me_complete_transfer_to_sccp_form_record_materialized",
        task_completion_event=TaskCompletionEventType.RELEASE_TO_COMMUNITY_CONFINEMENT_SUPERVISION,
        source_filename="us_me_complete_transfer_to_sccp_form_record.json",
        export_collection_name="US_ME-SCCPReferrals",
        opportunity_type_path_str="SCCP",
        person_record_type=PersonRecordType.RESIDENT,
    ),
    WorkflowsOpportunityConfig(
        state_code=StateCode.US_ME,
        opportunity_type="usMeWorkRelease",
        experiment_id="US_ME_WORK_RELEASE_WORKFLOWS",
        opportunity_record_view_name="us_me_work_release_form_record_materialized",
        task_completion_event=TaskCompletionEventType.GRANTED_WORK_RELEASE,
        source_filename="us_me_work_release_form_record.json",
        export_collection_name="US_ME-workReleaseReferrals",
        opportunity_type_path_str="workRelease",
        person_record_type=PersonRecordType.RESIDENT,
    ),
    WorkflowsOpportunityConfig(
        state_code=StateCode.US_ME,
        opportunity_type="usMeFurloughRelease",
        experiment_id="US_ME_FURLOUGH_RELEASE_WORKFLOWS",
        opportunity_record_view_name="us_me_furlough_release_form_record_materialized",
        task_completion_event=TaskCompletionEventType.GRANTED_FURLOUGH,
        source_filename="us_me_furlough_release_form_record.json",
        export_collection_name="US_ME-furloughReleaseReferrals",
        opportunity_type_path_str="furloughRelease",
        person_record_type=PersonRecordType.RESIDENT,
    ),
    WorkflowsOpportunityConfig(
        state_code=StateCode.US_ME,
        opportunity_type="usMeMediumTrustee",
        experiment_id="US_ME_CUSTODY_LEVEL_DOWNGRADE_TO_MEDIUM_TRUSTEE_WORKFLOWS",
        opportunity_record_view_name="us_me_custody_level_downgrade_to_medium_trustee_request_record_materialized",
        task_completion_event=TaskCompletionEventType.CUSTODY_LEVEL_DOWNGRADE,
        source_filename="us_me_custody_level_downgrade_to_medium_trustee_request_record.json",
        export_collection_name="US_ME-mediumTrusteeReferrals",
        opportunity_type_path_str="mediumTrustee",
        person_record_type=PersonRecordType.RESIDENT,
    ),
    WorkflowsOpportunityConfig(
        state_code=StateCode.US_ME,
        opportunity_type="usMeReclassificationReview",
        experiment_id="US_ME_CUSTODY_RECLASSIFICATION_REVIEW_WORKFLOWS",
        opportunity_record_view_name="us_me_custody_reclassification_review_form_record_materialized",
        task_completion_event=TaskCompletionEventType.INCARCERATION_ASSESSMENT_COMPLETED,
        source_filename="us_me_custody_reclassification_review_form_record.json",
        export_collection_name="US_ME-reclassificationReviewReferrals",
        opportunity_type_path_str="annualReclassification",
        person_record_type=PersonRecordType.RESIDENT,
    ),
    WorkflowsOpportunityConfig(
        state_code=StateCode.US_MI,
        opportunity_type="usMiClassificationReview",
        experiment_id="US_MI_SUPERVISION_LEVEL_DOWNGRADE_WORKFLOWS",
        opportunity_record_view_name="us_mi_complete_classification_review_form_record_materialized",
        task_completion_event=TaskCompletionEventType.SUPERVISION_LEVEL_DOWNGRADE_AFTER_INITIAL_CLASSIFICATION_REVIEW_DATE,
        source_filename="us_mi_complete_classification_review_form_record.json",
        export_collection_name="US_MI-classificationReviewReferrals",
        opportunity_type_path_str="classificationReview",
        person_record_type=PersonRecordType.CLIENT,
    ),
    WorkflowsOpportunityConfig(
        state_code=StateCode.US_MI,
        opportunity_type="usMiEarlyDischarge",
        experiment_id="US_MI_EARLY_DISCHARGE_WORKFLOWS",
        opportunity_record_view_name="us_mi_complete_discharge_early_from_supervision_request_record_materialized",
        task_completion_event=TaskCompletionEventType.EARLY_DISCHARGE,
        source_filename="us_mi_complete_discharge_early_from_supervision_request_record.json",
        export_collection_name="US_MI-earlyDischargeReferrals",
        opportunity_type_path_str="earlyDischarge",
        person_record_type=PersonRecordType.CLIENT,
    ),
    WorkflowsOpportunityConfig(
        state_code=StateCode.US_MI,
        opportunity_type="usMiMinimumTelephoneReporting",
        experiment_id="US_MI_MINIMUM_TELEPHONE_REPORTING_WORKFLOWS",
        opportunity_record_view_name="us_mi_complete_transfer_to_telephone_reporting_request_record_materialized",
        task_completion_event=TaskCompletionEventType.TRANSFER_TO_LIMITED_SUPERVISION,
        source_filename="us_mi_complete_transfer_to_telephone_reporting_request_record.json",
        export_collection_name="US_MI-minimumTelephoneReporting",
        opportunity_type_path_str="minimumTelephoneReporting",
        person_record_type=PersonRecordType.CLIENT,
    ),
    WorkflowsOpportunityConfig(
        state_code=StateCode.US_MI,
        opportunity_type="usMiPastFTRD",
        experiment_id="US_MI_PAST_FTRD_WORKFLOWS",
        opportunity_record_view_name="us_mi_complete_full_term_discharge_from_supervision_request_record_materialized",
        task_completion_event=TaskCompletionEventType.FULL_TERM_DISCHARGE,
        source_filename="us_mi_complete_full_term_discharge_from_supervision_request_record.json",
        export_collection_name="US_MI-pastFTRDReferrals",
        opportunity_type_path_str="pastFTRD",
        person_record_type=PersonRecordType.CLIENT,
    ),
    WorkflowsOpportunityConfig(
        state_code=StateCode.US_MI,
        opportunity_type="usMiSupervisionLevelDowngrade",
        experiment_id="US_MI_SUPERVISION_LEVEL_DOWNGRADE_WORKFLOWS",
        opportunity_record_view_name="us_mi_supervision_level_downgrade_record_materialized",
        task_completion_event=TaskCompletionEventType.SUPERVISION_LEVEL_DOWNGRADE_BEFORE_INITIAL_CLASSIFICATION_REVIEW_DATE,
        source_filename="us_mi_supervision_level_downgrade_record.json",
        export_collection_name="US_MI-supervisionLevelDowngrade",
        opportunity_type_path_str="supervisionLevelMismatch",
        person_record_type=PersonRecordType.CLIENT,
    ),
    WorkflowsOpportunityConfig(
        state_code=StateCode.US_MI,
        opportunity_type="usMiSecurityClassificationCommitteeReview",
        experiment_id="US_MI_COMPLETE_SECURITY_CLASSIFICATION_COMMITTEE_REVIEW_FORM",
        opportunity_record_view_name="us_mi_complete_security_classification_committee_review_form_record_materialized",
        task_completion_event=TaskCompletionEventType.SECURITY_CLASSIFICATION_COMMITTEE_REVIEW,
        source_filename="us_mi_complete_security_classification_committee_review_form_record.json",
        export_collection_name="US_MI-securityClassificationCommitteeReview",
        opportunity_type_path_str="securityClassificationCommitteeReview",
        person_record_type=PersonRecordType.RESIDENT,
    ),
    WorkflowsOpportunityConfig(
        state_code=StateCode.US_MI,
        opportunity_type="usMiAddInPersonSecurityClassificationCommitteeReview",
        experiment_id="US_MI_COMPLETE_ADD_IN_PERSON_SECURITY_CLASSIFICATION_COMMITTEE_REVIEW_FORM",
        opportunity_record_view_name="us_mi_complete_add_in_person_security_classification_committee_review_form_record_materialized",
        task_completion_event=TaskCompletionEventType.ADD_IN_PERSON_SECURITY_CLASSIFICATION_COMMITTEE_REVIEW,
        source_filename="us_mi_complete_add_in_person_security_classification_committee_review_form_record.json",
        export_collection_name="US_MI-addInPersonSecurityClassificationCommitteeReview",
        opportunity_type_path_str="addInPersonSecurityClassificationCommitteeReview",
        person_record_type=PersonRecordType.RESIDENT,
    ),
    WorkflowsOpportunityConfig(
        state_code=StateCode.US_MI,
        opportunity_type="usMiWardenInPersonSecurityClassificationCommitteeReview",
        experiment_id="US_MI_COMPLETE_WARDEN_IN_PERSON_SECURITY_CLASSIFICATION_COMMITTEE_REVIEW_FORM",
        opportunity_record_view_name="us_mi_complete_warden_in_person_security_classification_committee_review_form_record_materialized",
        task_completion_event=TaskCompletionEventType.WARDEN_IN_PERSON_SECURITY_CLASSIFICATION_COMMITTEE_REVIEW,
        source_filename="us_mi_complete_warden_in_person_security_classification_committee_review_form_record.json",
        export_collection_name="US_MI-wardenInPersonSecurityClassificationCommitteeReview",
        opportunity_type_path_str="wardenInPersonSecurityClassificationCommitteeReview",
        person_record_type=PersonRecordType.RESIDENT,
    ),
    WorkflowsOpportunityConfig(
        state_code=StateCode.US_MI,
        opportunity_type="usMiReclassificationRequest",
        experiment_id="US_MI_COMPLETE_RECLASSIFICATION_TO_GENERAL_POPULATION_REQUEST",
        opportunity_record_view_name="us_mi_complete_reclassification_to_general_population_request_record_materialized",
        task_completion_event=TaskCompletionEventType.TRANSFER_OUT_OF_DISCIPLINARY_OR_TEMPORARY_SOLITARY_CONFINEMENT,
        source_filename="us_mi_complete_reclassification_to_general_population_request_record.json",
        export_collection_name="US_MI-reclassificationRequest",
        opportunity_type_path_str="reclassificationRequest",
        person_record_type=PersonRecordType.RESIDENT,
    ),
    WorkflowsOpportunityConfig(
        state_code=StateCode.US_MO,
        opportunity_type="usMoOverdueRestrictiveHousingRelease",
        experiment_id="US_MO_RESTRICTIVE_HOUSING_WORKFLOWS",
        opportunity_record_view_name="us_mo_overdue_restrictive_housing_release_record_materialized",
        task_completion_event=TaskCompletionEventType.TRANSFER_OUT_OF_SOLITARY_CONFINEMENT,
        source_filename="us_mo_overdue_restrictive_housing_release_record.json",
        export_collection_name="US_MO-overdueRestrictiveHousingReleaseReferrals",
        opportunity_type_path_str="overdueRestrictiveHousingRelease",
        person_record_type=PersonRecordType.RESIDENT,
    ),
    WorkflowsOpportunityConfig(
        state_code=StateCode.US_MO,
        opportunity_type="usMoOverdueRestrictiveHousingInitialHearing",
        experiment_id="US_MO_RESTRICTIVE_HOUSING_WORKFLOWS",
        opportunity_record_view_name="us_mo_overdue_restrictive_housing_initial_hearing_record_materialized",
        task_completion_event=TaskCompletionEventType.HEARING_OCCURRED,
        source_filename="us_mo_overdue_restrictive_housing_initial_hearing_record.json",
        export_collection_name="US_MO-overdueRestrictiveHousingInitialHearingReferrals",
        opportunity_type_path_str="overdueRestrictiveHousingInitialHearing",
        person_record_type=PersonRecordType.RESIDENT,
    ),
    WorkflowsOpportunityConfig(
        state_code=StateCode.US_MO,
        opportunity_type="usMoOverdueRestrictiveHousingReviewHearing",
        experiment_id="US_MO_RESTRICTIVE_HOUSING_WORKFLOWS",
        opportunity_record_view_name="us_mo_overdue_restrictive_housing_review_hearing_record_materialized",
        task_completion_event=TaskCompletionEventType.HEARING_OCCURRED,
        source_filename="us_mo_overdue_restrictive_housing_review_hearing_record.json",
        export_collection_name="US_MO-overdueRestrictiveHousingReviewHearingReferrals",
        opportunity_type_path_str="overdueRestrictiveHousingReviewHearing",
        person_record_type=PersonRecordType.RESIDENT,
    ),
    WorkflowsOpportunityConfig(
        state_code=StateCode.US_ND,
        opportunity_type="earlyTermination",
        experiment_id="US_ND_EARLY_TERMINATION_WORKFLOWS",
        opportunity_record_view_name="us_nd_complete_discharge_early_from_supervision_record_materialized",
        task_completion_event=TaskCompletionEventType.EARLY_DISCHARGE,
        source_filename="us_nd_complete_discharge_early_from_supervision_record.json",
        export_collection_name="earlyTerminationReferrals",
        opportunity_type_path_str="earlyTermination",
        person_record_type=PersonRecordType.CLIENT,
    ),
    WorkflowsOpportunityConfig(
        state_code=StateCode.US_ND,
        opportunity_type="usNdATP",
        experiment_id="US_ND_TRANSFER_TO_ATP_WORKFLOWS",
        opportunity_record_view_name="us_nd_transfer_to_atp_form_record_materialized",
        task_completion_event=TaskCompletionEventType.GRANTED_WORK_RELEASE,
        source_filename="us_nd_transfer_to_atp_form_record.json",
        export_collection_name="US_ND-AtpReferrals",
        opportunity_type_path_str="ATP",
        person_record_type=PersonRecordType.RESIDENT,
    ),
    WorkflowsOpportunityConfig(
        state_code=StateCode.US_ND,
        opportunity_type="usNdTransferToMinFacility",
        experiment_id="US_ND_TRANSFER_TO_MINIMUM_FACILITY_WORKFLOWS",
        opportunity_record_view_name="us_nd_transfer_to_minimum_facility_form_record_materialized",
        task_completion_event=TaskCompletionEventType.TRANSFER_TO_MINIMUM_FACILITY,
        source_filename="us_nd_transfer_to_minimum_facility_form_record.json",
        export_collection_name="US_ND-TransferToMinFacility",
        opportunity_type_path_str="TransferToMinFacility",
        person_record_type=PersonRecordType.RESIDENT,
    ),
    WorkflowsOpportunityConfig(
        state_code=StateCode.US_OR,
        opportunity_type="usOrEarnedDischarge",
        experiment_id="US_OR_EARNED_DISCHARGE_WORKFLOWS",
        opportunity_record_view_name="us_or_earned_discharge_record_materialized",
        task_completion_event=TaskCompletionEventType.EARLY_DISCHARGE,
        source_filename="us_or_earned_discharge_record.json",
        export_collection_name="US_OR-earnedDischarge",
        opportunity_type_path_str="earnedDischarge",
        person_record_type=PersonRecordType.CLIENT,
    ),
    WorkflowsOpportunityConfig(
        state_code=StateCode.US_OR,
        opportunity_type="usOrEarnedDischargeSentence",
        experiment_id="US_OR_EARNED_DISCHARGE_WORKFLOWS",
        opportunity_record_view_name="us_or_earned_discharge_sentence_record_materialized",
        task_completion_event=TaskCompletionEventType.EARLY_DISCHARGE,
        source_filename="us_or_earned_discharge_sentence_record.json",
        export_collection_name="US_OR-earnedDischargeSentence",
        opportunity_type_path_str="earnedDischargeSentence",
        person_record_type=PersonRecordType.CLIENT,
    ),
    WorkflowsOpportunityConfig(
        state_code=StateCode.US_PA,
        opportunity_type="usPaAdminSupervision",
        experiment_id="US_PA_ADMIN_SUPERVISION_WORKFLOWS",
        opportunity_record_view_name="us_pa_transfer_to_administrative_supervision_form_record_materialized",
        task_completion_event=TaskCompletionEventType.TRANSFER_TO_ADMINISTRATIVE_SUPERVISION,
        source_filename="us_pa_transfer_to_administrative_supervision_form_record.json",
        export_collection_name="US_PA-adminSupervisionReferrals",
        opportunity_type_path_str="adminSupervision",
        person_record_type=PersonRecordType.CLIENT,
    ),
    WorkflowsOpportunityConfig(
        state_code=StateCode.US_PA,
        opportunity_type="usPaSpecialCircumstancesSupervision",
        experiment_id="US_PA_SPECIAL_CIRCUMSTANCES_SUPERVISION_WORKFLOWS",
        opportunity_record_view_name="us_pa_complete_transfer_to_special_circumstances_supervision_request_record_materialized",
        task_completion_event=TaskCompletionEventType.TRANSFER_TO_SPECIAL_CIRCUMSTANCES_SUPERVISION,
        source_filename="us_pa_complete_transfer_to_special_circumstances_supervision_request_record.json",
        export_collection_name="US_PA-specialCircumstancesSupervisionReferrals",
        opportunity_type_path_str="specialCircumstancesSupervisionReferrals",
        person_record_type=PersonRecordType.CLIENT,
    ),
    # TODO(Recidiviz/recidiviz-dashboards#5003) - Replace with us_tn_transfer_to_compliant_reporting_record when we move over to new data entirely
    WorkflowsOpportunityConfig(
        state_code=StateCode.US_TN,
        opportunity_type="compliantReporting",
        experiment_id="US_TN_COMPLIANT_REPORTING_WORKFLOWS",
        opportunity_record_view_name="us_tn_transfer_to_compliant_reporting_record_materialized",
        task_completion_event=TaskCompletionEventType.TRANSFER_TO_LIMITED_SUPERVISION,
        source_filename="us_tn_transfer_to_compliant_reporting_record.json",
        export_collection_name="compliantReportingReferrals",
        opportunity_type_path_str="compliantReporting",
        person_record_type=PersonRecordType.CLIENT,
    ),
    WorkflowsOpportunityConfig(
        state_code=StateCode.US_TN,
        opportunity_type="usTnCustodyLevelDowngrade",
        experiment_id="US_TN_CLASSIFICATION_WORKFLOWS",
        opportunity_record_view_name="us_tn_custody_level_downgrade_record_materialized",
        task_completion_event=TaskCompletionEventType.CUSTODY_LEVEL_DOWNGRADE,
        source_filename="us_tn_custody_level_downgrade_record.json",
        export_collection_name="US_TN-custodyLevelDowngradeReferrals",
        opportunity_type_path_str="custodyLevelDowngrade",
        person_record_type=PersonRecordType.RESIDENT,
    ),
    WorkflowsOpportunityConfig(
        state_code=StateCode.US_TN,
        opportunity_type="usTnExpiration",
        experiment_id="US_TN_DISCHARGE_WORKFLOWS",
        opportunity_record_view_name="us_tn_full_term_supervision_discharge_record_materialized",
        task_completion_event=TaskCompletionEventType.FULL_TERM_DISCHARGE,
        source_filename="us_tn_full_term_supervision_discharge_record.json",
        export_collection_name="US_TN-expirationReferrals",
        opportunity_type_path_str="expiration",
        person_record_type=PersonRecordType.CLIENT,
    ),
    WorkflowsOpportunityConfig(
        state_code=StateCode.US_TN,
        opportunity_type="supervisionLevelDowngrade",
        experiment_id="US_TN_SUPERVISION_LEVEL_DOWNGRADE_WORKFLOWS",
        opportunity_record_view_name="us_tn_supervision_level_downgrade_record_materialized",
        task_completion_event=TaskCompletionEventType.SUPERVISION_LEVEL_DOWNGRADE,
        source_filename="us_tn_supervision_level_downgrade_record.json",
        export_collection_name="US_TN-supervisionLevelDowngrade",
        opportunity_type_path_str="supervisionLevelDowngrade",
        person_record_type=PersonRecordType.CLIENT,
    ),
    WorkflowsOpportunityConfig(
        state_code=StateCode.US_TN,
        opportunity_type="usTnAnnualReclassification",
        experiment_id="US_TN_CLASSIFICATION_WORKFLOWS",
        opportunity_record_view_name="us_tn_annual_reclassification_review_record_materialized",
        task_completion_event=TaskCompletionEventType.INCARCERATION_ASSESSMENT_COMPLETED,
        source_filename="us_tn_annual_reclassification_review_record.json",
        export_collection_name="US_TN-annualReclassificationReferrals",
        opportunity_type_path_str="annualReclassification",
        person_record_type=PersonRecordType.RESIDENT,
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
