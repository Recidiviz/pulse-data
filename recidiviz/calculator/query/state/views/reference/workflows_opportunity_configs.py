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
    # TODO(#18193): Once CR TES migration is completed, this field can be consolidated with opportunity_record_view_name
    source_filename: str = attr.ib()

    # The Firestore collection name used as the destination for the opportunity record export
    export_collection_name: str = attr.ib()

    # The string used to represent this opportunity in the workflows URL
    opportunity_type_path_str: str = attr.ib()

    # The type of person record to add JII's to, either 'client' or 'resident'
    person_record_type: PersonRecordType = attr.ib()


WORKFLOWS_OPPORTUNITY_CONFIGS = [
    WorkflowsOpportunityConfig(
        state_code=StateCode.US_TN,
        opportunity_type="compliantReporting",
        experiment_id="US_TN_COMPLIANT_REPORTING_WORKFLOWS",
        opportunity_record_view_name="us_tn_compliant_reporting_logic_materialized",
        task_completion_event=TaskCompletionEventType.TRANSFER_TO_LIMITED_SUPERVISION,
        source_filename="compliant_reporting_referral_record.json",
        export_collection_name="compliantReportingReferrals",
        opportunity_type_path_str="compliantReporting",
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
        state_code=StateCode.US_MO,
        opportunity_type="usMoRestrictiveHousingStatusHearing",
        experiment_id="US_MO_ADMINISTRATIVE_SEGREGATION_WORKFLOWS",
        opportunity_record_view_name="us_mo_upcoming_restrictive_housing_hearing_record_materialized",
        task_completion_event=TaskCompletionEventType.SCHEDULED_HEARING_OCCURRED,
        source_filename="us_mo_upcoming_restrictive_housing_hearing_record.json",
        export_collection_name="US_MO-restrictiveHousingStatusHearingReferrals",
        opportunity_type_path_str="restrictiveHousingStatusHearing",
        person_record_type=PersonRecordType.RESIDENT,
    ),
    WorkflowsOpportunityConfig(
        state_code=StateCode.US_MI,
        opportunity_type="usMiClassificationReview",
        experiment_id="US_MI_SUPERVISION_LEVEL_DOWNGRADE_WORKFLOWS",
        opportunity_record_view_name="us_mi_complete_classification_review_form_record_materialized",
        task_completion_event=TaskCompletionEventType.SUPERVISION_LEVEL_DOWNGRADE,
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
]

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
