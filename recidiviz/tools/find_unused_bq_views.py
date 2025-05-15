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
"""Script that prints out views that are candidates for deletion because they or their
children are not referenced by any known downstream user.

Usage:
    python -m recidiviz.tools.find_unused_bq_views
"""

from typing import Dict, Optional, Set

from recidiviz.aggregated_metrics.dataset_config import AGGREGATED_METRICS_DATASET_ID
from recidiviz.big_query.big_query_address import BigQueryAddress
from recidiviz.big_query.big_query_view import BigQueryView
from recidiviz.big_query.big_query_view_dag_walker import BigQueryViewDagWalker
from recidiviz.big_query.build_views_to_update import build_views_to_update
from recidiviz.calculator.query.experiments_metadata.views.officer_assignments import (
    OFFICER_ASSIGNMENTS_VIEW_BUILDER,
)
from recidiviz.calculator.query.experiments_metadata.views.state_assignments import (
    STATE_ASSIGNMENTS_VIEW_BUILDER,
)
from recidiviz.calculator.query.externally_shared_views.dataset_config import (
    EXTERNALLY_SHARED_VIEWS_DATASET,
)
from recidiviz.calculator.query.state.dataset_config import (
    DATAFLOW_METRICS_MATERIALIZED_DATASET,
    IMPACT_REPORTS_DATASET_ID,
    POPULATION_PROJECTION_DATASET,
    SPARK_OUTPUT_DATASET_MOST_RECENT,
)
from recidiviz.calculator.query.state.views.analyst_data.early_discharge_reports_per_officer import (
    EARLY_DISCHARGE_REPORTS_PER_OFFICER_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.analyst_data.early_discharge_sessions_with_officer_and_supervisor import (
    EARLY_DISCHARGE_SESSIONS_WITH_OFFICER_AND_SUPERVISOR_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.analyst_data.population_density_by_supervision_office import (
    POPULATION_DENSITY_BY_SUPERVISION_OFFICE_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.analyst_data.psa_risk_scores import (
    PSA_RISK_SCORES_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.analyst_data.us_az.us_az_action_queue import (
    US_AZ_ACTION_QUEUE_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.analyst_data.us_ma.us_ma_projected_dates import (
    US_MA_PROJECTED_DATES_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.analyst_data.us_mo.us_mo_mosop_prio_groups import (
    US_MO_MOSOP_PRIO_GROUPS_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.analyst_data.us_mo.us_mo_program_tracks import (
    US_MO_PROGRAM_TRACKS_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.analyst_data.us_mo.us_mo_sentencing_dates_preprocessed import (
    US_MO_SENTENCING_DATES_PREPROCESSED_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.analyst_data.us_tn.us_tn_segregation_lists import (
    US_TN_SEGREGATION_LISTS_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.analyst_data.workflows_person_marked_ineligible_status_session_details import (
    WORKFLOWS_PERSON_MARKED_INELIGIBLE_STATUS_SESSION_DETAILS_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.outliers.supervision_impact_metrics_outlier_officers import (
    SUPERVISION_IMPACT_METRICS_OUTLIER_OFFICERS_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.outliers.supervision_impact_metrics_supervisors import (
    SUPERVISION_IMPACT_METRICS_SUPERVISORS_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.outliers.supervision_officer_metrics_archive import (
    SUPERVISION_OFFICER_METRICS_ARCHIVE_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.outliers.supervision_officer_supervisors_archive import (
    SUPERVISION_OFFICER_SUPERVISORS_ARCHIVE_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.outliers.supervision_officers_archive import (
    SUPERVISION_OFFICERS_ARCHIVE_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.outliers.supervision_state_metrics import (
    SUPERVISION_STATE_METRICS_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.outliers.supervision_usage_metrics import (
    SUPERVISION_USAGE_METRICS_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.prototypes.case_note_search.case_notes_data_store import (
    CASE_NOTES_DATA_STORE_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.reentry.case_manager import (
    REENTRY_CASE_MANAGER_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.reentry.client import (
    REENTRY_CLIENT_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.reentry.supervision_officer import (
    REENTRY_SUPERVISION_OFFICER_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.sentence_sessions.sentence_imposed_group_summary import (
    SENTENCE_IMPOSED_GROUP_SUMMARY_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.sentence_sessions.sentence_to_consecutive_parent_sentence import (
    CONSECUTIVE_SENTENCES_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.sentence_sessions_v2_all.sentence_sessions_v2_all_views import (
    SENTENCE_SESSIONS_V2_ALL_VIEW_BUILDERS,
)
from recidiviz.calculator.query.state.views.sentencing.recidivism_event import (
    RECIDIVISM_EVENT_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.sentencing.sentence_cohort import (
    SENTENCE_COHORT_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.sessions.assessment_lsir_responses import (
    ASSESSMENT_LSIR_RESPONSES_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.sessions.assessment_lsir_scoring_key import (
    ASSESSMENT_LSIR_SCORING_KEY_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.sessions.compartment_level_2_super_sessions import (
    COMPARTMENT_LEVEL_2_SUPER_SESSIONS_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.sessions.incarceration_staff_attribute_sessions import (
    INCARCERATION_STAFF_ATTRIBUTE_SESSIONS_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.sessions.incarceration_staff_caseload_count_spans import (
    INCARCERATION_STAFF_CASELOAD_COUNT_SPANS_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.sessions.parole_board_hearing_decisions import (
    PAROLE_BOARD_HEARING_DECISIONS_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.sessions.us_ar.us_ar_non_traditional_bed_sessions_preprocessed import (
    US_AR_NON_TRADITIONAL_BED_SESSIONS_PREPROCESSED_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.sessions.us_nd.us_nd_raw_lsir_assessments import (
    US_ND_RAW_LSIR_ASSESSMENTS_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.sessions.us_tn.us_tn_parole_board_hearing_decisions import (
    US_TN_PAROLE_BOARD_HEARING_DECISIONS_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.sessions_validation.session_incarceration_admissions_to_dataflow_disaggregated import (
    SESSION_INCARCERATION_ADMISSIONS_TO_DATAFLOW_DISAGGREGATED_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.sessions_validation.session_incarceration_population_to_dataflow_disaggregated import (
    SESSION_INCARCERATION_POPULATION_TO_DATAFLOW_DISAGGREGATED_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.sessions_validation.session_incarceration_releases_to_dataflow_disaggregated import (
    SESSION_INCARCERATION_RELEASES_TO_DATAFLOW_DISAGGREGATED_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.sessions_validation.session_supervision_out_of_state_population_to_dataflow_disaggregated import (
    SESSION_SUPERVISION_OUT_OF_STATE_POPULATION_TO_DATAFLOW_DISAGGREGATED_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.sessions_validation.session_supervision_population_to_dataflow_disaggregated import (
    SESSION_SUPERVISION_POPULATION_TO_DATAFLOW_DISAGGREGATED_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.sessions_validation.session_supervision_starts_to_dataflow_disaggregated import (
    SESSION_SUPERVISION_STARTS_TO_DATAFLOW_DISAGGREGATED_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.sessions_validation.session_supervision_terminations_to_dataflow_disaggregated import (
    SESSION_SUPERVISION_TERMINATIONS_TO_DATAFLOW_DISAGGREGATED_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.user_metrics.insights_user_available_actions import (
    INSIGHTS_USER_AVAILABLE_ACTIONS_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.user_metrics.officer_monthly_usage_report import (
    OFFICER_MONTHLY_USAGE_REPORT_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.user_metrics.us_nd_transfer_to_min_eligible_marked_ineligible import (
    US_ND_TRANSFER_TO_MIN_ELIGIBLE_MARKED_INELIGIBLE_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.user_metrics.us_nd_transfer_to_min_eligible_pending_approval import (
    US_ND_TRANSFER_TO_MIN_ELIGIBLE_PENDING_APPROVAL_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.user_metrics.us_nd_transfer_to_min_eligible_waiting_for_transfer import (
    US_ND_TRANSFER_TO_MIN_ELIGIBLE_WAITING_FOR_TRANSFER_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.user_metrics.us_nd_transfer_to_min_eligible_without_referral import (
    US_ND_TRANSFER_TO_MIN_ELIGIBLE_WITHOUT_REFERRAL_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.user_metrics.workflows_facilities_user_available_actions import (
    WORKFLOWS_FACILITIES_USER_AVAILABLE_ACTIONS_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.user_metrics.workflows_supervision_user_available_actions import (
    WORKFLOWS_SUPERVISION_USER_AVAILABLE_ACTIONS_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.workflows.all_funnel_events import (
    ALL_FUNNEL_EVENTS_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.workflows.clients_milestones_congratulated_another_way import (
    CLIENTS_MILESTONES_CONGRATULATED_ANOTHER_WAY_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.workflows.clients_milestones_side_panel_opened import (
    CLIENTS_MILESTONES_SIDE_PANEL_OPENED_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.workflows.current_impact_funnel_status import (
    CURRENT_IMPACT_FUNNEL_STATUS_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.workflows.milestones_funnel import (
    MILESTONES_FUNNEL_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.workflows.workflows_usage import (
    WORKFLOWS_USAGE_VIEW_BUILDER,
)
from recidiviz.common.constants.states import StateCode
from recidiviz.ingest.direct.dataset_config import raw_latest_views_dataset_for_region
from recidiviz.ingest.direct.types.direct_ingest_instance import DirectIngestInstance
from recidiviz.ingest.views.dataset_config import STATE_BASE_VIEWS_DATASET
from recidiviz.metrics.export.export_config import VIEW_COLLECTION_EXPORT_INDEX
from recidiviz.monitoring.platform_kpis.dataset_config import PLATFORM_KPIS_DATASET
from recidiviz.observations.dataset_config import dataset_for_observation_type_cls
from recidiviz.observations.event_observation_big_query_view_builder import (
    MetricUnitOfObservationType,
)
from recidiviz.observations.event_type import EventType
from recidiviz.observations.span_type import SpanType
from recidiviz.outcome_metrics.dataset_config import TRANSITIONS_DATASET
from recidiviz.persistence.entity.entities_bq_schema import (
    get_bq_schema_for_entities_module,
)
from recidiviz.persistence.entity.state import entities as state_entities
from recidiviz.source_tables.collect_all_source_table_configs import (
    get_source_table_datasets,
)
from recidiviz.task_eligibility.candidate_populations.general.non_temporary_custody_incarceration_population import (
    VIEW_BUILDER as NON_TEMPORARY_CUSTODY_INCARCERATION_POPULATION_VIEW_BUILDER,
)
from recidiviz.task_eligibility.criteria.general.on_minimum_after_unassigned import (
    VIEW_BUILDER as ON_MINIMUM_AFTER_UNASSIGNED,
)
from recidiviz.utils.environment import DATA_PLATFORM_GCP_PROJECTS
from recidiviz.utils.metadata import local_project_id_override
from recidiviz.validation.configured_validations import get_all_validations
from recidiviz.validation.views.dataset_config import EXTERNAL_ACCURACY_DATASET
from recidiviz.view_registry.deployed_views import deployed_view_builders

# List of views that are definitely referenced in Looker (as of 11/29/23). This list is
# incomplete and you should add to this list / update the date in this comment as you
# work with this script.
LOOKER_REFERENCED_ADDRESSES: Set[BigQueryAddress] = {
    WORKFLOWS_USAGE_VIEW_BUILDER.address,
    CURRENT_IMPACT_FUNNEL_STATUS_VIEW_BUILDER.address,
    # These views are referenced by a sessions validation dashboard in Looker
    SESSION_INCARCERATION_POPULATION_TO_DATAFLOW_DISAGGREGATED_VIEW_BUILDER.address,
    SESSION_SUPERVISION_POPULATION_TO_DATAFLOW_DISAGGREGATED_VIEW_BUILDER.address,
    SESSION_SUPERVISION_OUT_OF_STATE_POPULATION_TO_DATAFLOW_DISAGGREGATED_VIEW_BUILDER.address,
    SESSION_INCARCERATION_ADMISSIONS_TO_DATAFLOW_DISAGGREGATED_VIEW_BUILDER.address,
    SESSION_SUPERVISION_STARTS_TO_DATAFLOW_DISAGGREGATED_VIEW_BUILDER.address,
    SESSION_INCARCERATION_RELEASES_TO_DATAFLOW_DISAGGREGATED_VIEW_BUILDER.address,
    SESSION_SUPERVISION_TERMINATIONS_TO_DATAFLOW_DISAGGREGATED_VIEW_BUILDER.address,
}

# List of views that are not referenced in Looker but should still be kept around,
# listed along with the reason we still need to keep them. Please be as descriptive
# as possible when updating this list, including a point of contact and date we were
# still using this view where possible.
UNREFERENCED_ADDRESSES_TO_KEEP_WITH_REASON: Dict[BigQueryAddress, str] = {
    **{
        BigQueryAddress(
            dataset_id=STATE_BASE_VIEWS_DATASET,
            table_id=f"{table_id}_view",
        ): (
            "These views materialize to form the `state` dataset which should not be "
            "referenced by any downstream view (Anna Geiduschek, 2024-09-18)"
        )
        for table_id in get_bq_schema_for_entities_module(state_entities)
    },
    SENTENCE_IMPOSED_GROUP_SUMMARY_VIEW_BUILDER.address: (
        "This is a new table in the sentence_sessions dataset which will soon be used in "
        "PSI work (Nick Tallant, 2025-01-30)"
    ),
    **{
        VIEW_BUILDER.address: (
            "These views mirror the views in the `sentence_sessions` dataset, but without any exclusion of states based "
            "on migration status. They are used primarily for validation and development work but some are referenced in"
            "TES critiera. The list below encompasses those not yet referenced downstream (Andrew Gaidus, 2025-03-18)"
        )
        for VIEW_BUILDER in SENTENCE_SESSIONS_V2_ALL_VIEW_BUILDERS
        if VIEW_BUILDER.address.table_id
        in [
            "overlapping_sentence_serving_periods",
            "sentence_id_to_consecutive_sentence_id",
            "sentence_imposed_group_summary",
            "sentence_serving_start_date",
        ]
    },
    CONSECUTIVE_SENTENCES_VIEW_BUILDER.address: (
        "This is going to be used in revamped sessions views that referenced the sentencing v2 schema "
        "(Nick Tallant, 2024-09-11)"
    ),
    ASSESSMENT_LSIR_SCORING_KEY_VIEW_BUILDER.address: (
        "This is a generic view that helps understand LSI-R scoring which may be "
        "useful for future analysis. (Anna 5/16/24)"
    ),
    EARLY_DISCHARGE_REPORTS_PER_OFFICER_VIEW_BUILDER.address: (
        "This view aggregates early discharge stats at the officer-level. It is used "
        "to generate reports that can be used by supervisors to identify officers who "
        "are not completing early discharges in a timely manner. These only include "
        "information for the most recent complete months. (Hugo S 12/21/23)"
    ),
    EARLY_DISCHARGE_SESSIONS_WITH_OFFICER_AND_SUPERVISOR_VIEW_BUILDER.address: (
        "View of early_discharge_sessions with additional information related to"
        "1) the officer who supervised the individual during the early discharge, and"
        "2) the supervisor who supervised the officer during the early discharge (Hugo S 12/21/23)"
    ),
    POPULATION_DENSITY_BY_SUPERVISION_OFFICE_VIEW_BUILDER.address: (
        "Not currently referenced but has been used for ad-hoc related research questions "
        "in the past and could become useful for census-based looker dashboards in the future"
        "(mayukas 12/21/23)"
    ),
    US_MA_PROJECTED_DATES_VIEW_BUILDER.address: (
        "Will be used in person_projected_date_sessions (n-damiani 05/12/2025)"
    ),
    US_MO_MOSOP_PRIO_GROUPS_VIEW_BUILDER.address: (
        "Used for ongoing MOSOP work (n-damiani 12/21/23)"
    ),
    US_MO_PROGRAM_TRACKS_VIEW_BUILDER.address: (
        "Used for ongoing MOSOP work (Damini Sharma 12/21/23)"
    ),
    US_MO_SENTENCING_DATES_PREPROCESSED_VIEW_BUILDER.address: (
        "Used for ongoing MOSOP work (Damini Sharma 12/21/23)"
    ),
    OFFICER_ASSIGNMENTS_VIEW_BUILDER.address: (
        "These views are still referenced by existing looker infra and will likely become relevant "
        "to future templatized dashboards tracking pre-post rollout trends, which is one of the "
        "pieces of tooling requested by DA's (mayukas 12/21/23)"
    ),
    STATE_ASSIGNMENTS_VIEW_BUILDER.address: (
        "These views are still referenced by existing looker infra and will likely become relevant "
        "to future templatized dashboards tracking pre-post rollout trends, which is one of the "
        "pieces of tooling requested by DA's (mayukas 12/21/23)"
    ),
    ASSESSMENT_LSIR_RESPONSES_VIEW_BUILDER.address: (
        "Not currently referenced but captures state-specific logic that may eventually be relevant to "
        "assessment schema and could assist with PSI-shaped work (mayukas 12/21/23)"
    ),
    PAROLE_BOARD_HEARING_DECISIONS_VIEW_BUILDER.address: (
        "This is a state-specific preprocessing view that is useful for ad-hoc analysis and "
        "provides a template for an eventual  schema addition to support parole boards info. "
        "Will also likely be relevant to upcoming best path work (mayukas 12/21/23)"
    ),
    US_ND_RAW_LSIR_ASSESSMENTS_VIEW_BUILDER.address: (
        "Not currently referenced but captures state-specific logic that may eventually be relevant "
        "to assessment schema and could assist with PSI-shaped work (mayukas 12/21/23)"
    ),
    US_TN_PAROLE_BOARD_HEARING_DECISIONS_VIEW_BUILDER.address: (
        "This is a state-specific preprocessing view that is useful for ad-hoc analysis and "
        "provides a template for an eventual  schema addition to support parole boards info. "
        "Will also likely be relevant to upcoming best path work (mayukas 12/21/23)"
    ),
    US_AR_NON_TRADITIONAL_BED_SESSIONS_PREPROCESSED_VIEW_BUILDER.address: (
        "Used in AR non-traditional bed analysis notebooks (dallen5 1/15/25)"
    ),
    US_AZ_ACTION_QUEUE_VIEW_BUILDER.address: (
        "Used in ongoing AZ reentry planning analysis. (EG)"
    ),
    BigQueryAddress(
        dataset_id=DATAFLOW_METRICS_MATERIALIZED_DATASET,
        table_id="most_recent_incarceration_admission_metrics_not_included_in_state_population",
    ): (
        'Keeping this for now because it is a parallel to the "in state population" version of this metric. '
        "When we revisit how we calculate in state / out of state populations we may be able to revisit."
        "(Anna Geiduschek 1/8/24)"
    ),
    BigQueryAddress(
        dataset_id=DATAFLOW_METRICS_MATERIALIZED_DATASET,
        table_id="most_recent_incarceration_commitment_from_supervision_metrics_not_included_in_state_population",
    ): (
        'Keeping this for now because it is a parallel to the "in state population" version of this metric. '
        "When we revisit how we calculate in state / out of state populations we may be able to revisit."
        "(Anna Geiduschek 1/8/24)"
    ),
    BigQueryAddress(
        dataset_id=DATAFLOW_METRICS_MATERIALIZED_DATASET,
        table_id="most_recent_incarceration_release_metrics_not_included_in_state_population",
    ): (
        'Keeping this for now because it is a parallel to the "in state population" version of this metric. '
        "When we revisit how we calculate in state / out of state populations we may be able to revisit."
        "(Anna Geiduschek 1/8/24)"
    ),
    BigQueryAddress(
        dataset_id=DATAFLOW_METRICS_MATERIALIZED_DATASET,
        table_id="most_recent_supervision_out_of_state_population_metrics",
    ): (
        'Keeping this for now because it is a parallel to the "in state population" version of this metric. '
        "When we revisit how we calculate in state / out of state populations we may be able to revisit."
        "(Anna Geiduschek 1/8/24)"
    ),
    PSA_RISK_SCORES_VIEW_BUILDER.address: (
        "Past intern work may be picked up so this view should be kept. See #26726. (Damini Sharma 1/22/24)"
    ),
    COMPARTMENT_LEVEL_2_SUPER_SESSIONS_VIEW_BUILDER.address: (
        "Not currently referenced but is used in downstream analytical work for which "
        "we are interested in aggregating across in state and out of state (Andrew Gaidus 1/25/24)"
    ),
    US_TN_SEGREGATION_LISTS_VIEW_BUILDER.address: (
        "Used to send ad hoc reports to TN every quarter. These views will eventually be deprecated if"
        "#21518 is completed (Damini Sharma 1/25/24)"
    ),
    SUPERVISION_OFFICER_METRICS_ARCHIVE_VIEW_BUILDER.address: (
        "Will be referenced for zero grants impact tracking (see #34607) (Gina Valderrama 10/29/24)"
    ),
    SUPERVISION_OFFICER_SUPERVISORS_ARCHIVE_VIEW_BUILDER.address: (
        "Will be referenced to support Outliers analytics work (see #27576) (Alexa Batino 2/14/24)"
    ),
    SUPERVISION_OFFICERS_ARCHIVE_VIEW_BUILDER.address: (
        "Will be referenced to support Outliers analytics work (see #27576) (Alexa Batino 2/14/24)"
    ),
    CLIENTS_MILESTONES_SIDE_PANEL_OPENED_VIEW_BUILDER.address: (
        "Will be referenced to support Workflows milestones_funnel work (see #28875) (Michelle Orden 4/11/24)"
    ),
    SUPERVISION_USAGE_METRICS_VIEW_BUILDER.address: (
        "Will be used for Insights analytics (see #29096) (Jen Overgaag 4/17/24)"
    ),
    SUPERVISION_IMPACT_METRICS_SUPERVISORS_VIEW_BUILDER.address: (
        "Will be used for Insights analytics (see #29096) (Jen Overgaag 4/17/24)"
    ),
    SUPERVISION_IMPACT_METRICS_OUTLIER_OFFICERS_VIEW_BUILDER.address: (
        "Will be used for Insights analytics (see #29096) (Jen Overgaag 4/17/24)"
    ),
    CLIENTS_MILESTONES_CONGRATULATED_ANOTHER_WAY_VIEW_BUILDER.address: (
        "Will be referenced to support Workflows milestones_funnel work (see #28875) (Michelle Orden 4/29/24)"
    ),
    ALL_FUNNEL_EVENTS_VIEW_BUILDER.address: (
        "Will be referenced to support Workflows Milestones Impact Tracking (see #28874) (Michelle Orden 4/23/24)"
    ),
    MILESTONES_FUNNEL_VIEW_BUILDER.address: (
        "Will be referenced to support Workflows Milestones Impact Tracking (see #28874) (Michelle Orden 5/22/24)"
    ),
    RECIDIVISM_EVENT_VIEW_BUILDER.address: (
        "Referenced by PSI Case Insights BigQuery writer (see #30876) (Ben Packer 7/9/24) "
    ),
    SENTENCE_COHORT_VIEW_BUILDER.address: (
        "Referenced by PSI Case Insights BigQuery writer (see #30876) (Ben Packer 7/9/24) "
    ),
    NON_TEMPORARY_CUSTODY_INCARCERATION_POPULATION_VIEW_BUILDER.address: (
        "Will be used for MO RH eligibility spans (see #31337) (Daniel Allen 7/24/24)"
    ),
    CASE_NOTES_DATA_STORE_VIEW_BUILDER.address: (
        "This view backs the datastore for Vertex AI search (Roshan Agrawal 10/24/2024)"
    ),
    WORKFLOWS_PERSON_MARKED_INELIGIBLE_STATUS_SESSION_DETAILS_VIEW_BUILDER.address: (
        "Will be referenced for Looker view of disaggregated marked ineligible status sessions and snooze "
        "details (see looker#616) (Jenna Bellassai 11/18/2024)"
    ),
    OFFICER_MONTHLY_USAGE_REPORT_VIEW_BUILDER.address: (
        "Referenced by the Usage by User connected sheet which is shared with DOC leadership monthly"
        " (Justine Kunz 12/03/2024)"
    ),
    INCARCERATION_STAFF_ATTRIBUTE_SESSIONS_VIEW_BUILDER.address: (
        "Will be used by roster sync for incarceration staff (Mayuka Sarukkai 02/25/2025)"
    ),
    INCARCERATION_STAFF_CASELOAD_COUNT_SPANS_VIEW_BUILDER.address: (
        "Will be used by roster sync for incarceration staff (Mayuka Sarukkai 02/25/2025)"
    ),
    ON_MINIMUM_AFTER_UNASSIGNED.address: (
        "Will be used after TN launches full new policy B. See #38506 (Damini Sharma 03/03/2025)"
    ),
    INSIGHTS_USER_AVAILABLE_ACTIONS_VIEW_BUILDER.address: (
        "Referenced by the automated email login reminder process (Justine Kunz 03/31/2025)"
    ),
    WORKFLOWS_SUPERVISION_USER_AVAILABLE_ACTIONS_VIEW_BUILDER.address: (
        "Referenced by the automated email login reminder process (Justine Kunz 03/31/2025)"
    ),
    US_ND_TRANSFER_TO_MIN_ELIGIBLE_WAITING_FOR_TRANSFER_VIEW_BUILDER.address: (
        "Will be used to power ND transfer to min security unit report for leadership (Maggie Taylor 03/20/2025)"
    ),
    US_ND_TRANSFER_TO_MIN_ELIGIBLE_MARKED_INELIGIBLE_VIEW_BUILDER.address: (
        "Will be used to power ND transfer to min security unit report for leadership (Maggie Taylor 03/20/2025)"
    ),
    US_ND_TRANSFER_TO_MIN_ELIGIBLE_PENDING_APPROVAL_VIEW_BUILDER.address: (
        "Will be used to power ND transfer to min security unit report for leadership (Maggie Taylor 03/20/2025)"
    ),
    US_ND_TRANSFER_TO_MIN_ELIGIBLE_WITHOUT_REFERRAL_VIEW_BUILDER.address: (
        "Will be used to power ND transfer to min security unit report for leadership (Maggie Taylor 03/20/2025)"
    ),
    REENTRY_CASE_MANAGER_VIEW_BUILDER.address: (
        "Will be used to power reentry tooling (Roshan Agrawal 04/22/2025)"
    ),
    REENTRY_CLIENT_VIEW_BUILDER.address: (
        "Will be used to power reentry tooling (Roshan Agrawal 04/22/2025)"
    ),
    REENTRY_SUPERVISION_OFFICER_VIEW_BUILDER.address: (
        "Will be used to power reentry tooling (Roshan Agrawal 04/22/2025)"
    ),
    SUPERVISION_STATE_METRICS_VIEW_BUILDER.address: (
        "Read directly in the insights 'prototype' application that powers the monthly emails to PA (Dana Hoffman 5/5/25)"
    ),
    WORKFLOWS_FACILITIES_USER_AVAILABLE_ACTIONS_VIEW_BUILDER.address: (
        "Referenced by the automated email login reminder process (Ryan Guan 05/07/2025)"
    ),
}

DATASETS_REFERENCED_BY_MISC_PROCESSES = {
    # All views in this dataset are referenced in Looker via autogenerated views
    AGGREGATED_METRICS_DATASET_ID,
    # These are validation views that compare external reference data to actual counts.
    EXTERNAL_ACCURACY_DATASET,
    # Views used in CSG export
    EXTERNALLY_SHARED_VIEWS_DATASET,
    # These views are inputs to Spark population projection modeling
    POPULATION_PROJECTION_DATASET,
    # Contains views referencing the output of Spark population projection runs. These
    # views are used to build Spark reports.
    SPARK_OUTPUT_DATASET_MOST_RECENT,
    # Views in this dataset are aggregated metrics related to impact reports
    IMPACT_REPORTS_DATASET_ID,
    # Views in this dataset are used by platform kpi dashboards
    PLATFORM_KPIS_DATASET,
    # Views in the dataset are used for org-wide impact tracking
    TRANSITIONS_DATASET,
    # Views in observation span and event datasets are used for generating
    # custom metrics in looker or analyst notebooks
    *[
        dataset_for_observation_type_cls(unit_of_observation_type, EventType)
        for unit_of_observation_type in MetricUnitOfObservationType
    ],
    *[
        dataset_for_observation_type_cls(unit_of_observation_type, SpanType)
        for unit_of_observation_type in MetricUnitOfObservationType
    ],
}


def _get_all_metric_export_addresses() -> Set[BigQueryAddress]:
    export_addresses = set()
    for export_config in VIEW_COLLECTION_EXPORT_INDEX.values():
        export_addresses |= {vb.address for vb in export_config.view_builders_to_export}
    return export_addresses


def _should_ignore_unused_address(address: BigQueryAddress) -> bool:
    """Returns true for views that may not be in use but can be ignored if they happen
    to be. These views are not tracked in UNREFERENCED_ADDRESSES_TO_KEEP_WITH_REASON because
    they aren't views that are important enough to keep that parent views should be
    marked as "used".
    """

    all_validations = get_all_validations()
    views_with_configured_validations: Set[BigQueryAddress] = {
        v.view_builder.address for v in all_validations
    } | {v.error_view_builder.address for v in all_validations}
    if address in views_with_configured_validations:
        # We don't mark these views as "used" because the existence of a validation does
        # not in itself mean we need to keep a parent view, but we generally don't
        # expect these views to be referenced by other views.
        return True

    if address.dataset_id in {
        # We autogenerate these views out of convenience and don't expect all to be used
        *{
            raw_latest_views_dataset_for_region(state_code, instance)
            for state_code in StateCode
            for instance in DirectIngestInstance
        },
    }:
        return True

    if address.dataset_id.startswith("task_eligibility") and address.table_id in {
        # TES autogenerated convenience views
        "all_general_completion_events",
        "all_state_specific_completion_events",
        "all_criteria",
        "all_general_criteria",
        "all_state_specific_criteria",
        "all_general_candidate_populations",
        "all_candidate_populations",
    }:
        return True

    return False


def _get_single_project_unused_addresses(
    all_views_dag_walker: BigQueryViewDagWalker, ignore_exemptions: bool
) -> Set[BigQueryAddress]:
    """Returns views that are unused within a single project. The
    `metadata.project_id()` must be set before this function is called.
    """

    all_in_use_addresses = (
        _get_all_metric_export_addresses() | LOOKER_REFERENCED_ADDRESSES
    )
    if not ignore_exemptions:
        all_in_use_addresses = all_in_use_addresses | set(
            UNREFERENCED_ADDRESSES_TO_KEEP_WITH_REASON
        )

    def is_view_used(v: BigQueryView, child_results: Dict[BigQueryView, bool]) -> bool:
        if v.address in all_in_use_addresses:
            return True

        if v.address.dataset_id in DATASETS_REFERENCED_BY_MISC_PROCESSES:
            return True

        return any(child_results.values())

    view_results = all_views_dag_walker.process_dag(
        view_process_fn=is_view_used, reverse=True, synchronous=False
    ).view_results

    return {
        v.address
        for v, is_used in view_results.items()
        if not is_used and not _should_ignore_unused_address(v.address)
    }


def get_unused_across_all_projects_addresses_from_all_views_dag(
    ignore_exemptions: bool = False,
) -> Set[BigQueryAddress]:
    """Returns the addresses of all views that are not referenced by a product, Looker,
    or marked as used via the UNREFERENCED_ADDRESSES_TO_KEEP_WITH_REASON exemptions
    list. If |ignore_exemptions| is True, will return ALL views that are unused, even if
    that view or one of its children is exempted in
    UNREFERENCED_ADDRESSES_TO_KEEP_WITH_REASON.
    """
    unused_addresses: Optional[Set[BigQueryAddress]] = None
    for project in DATA_PLATFORM_GCP_PROJECTS:
        with local_project_id_override(project):
            project_dag_walker = BigQueryViewDagWalker(
                build_views_to_update(
                    view_source_table_datasets=get_source_table_datasets(project),
                    candidate_view_builders=deployed_view_builders(),
                    sandbox_context=None,
                )
            )

            if len(project_dag_walker.views) == 0:
                raise ValueError(f"Failed to collect views for project {project}.")

            unused_addresses_in_project = _get_single_project_unused_addresses(
                project_dag_walker, ignore_exemptions=ignore_exemptions
            )
            if unused_addresses is None:
                unused_addresses = unused_addresses_in_project
                continue
            # Only keep addresses that were unused in all previous projects
            unused_addresses = unused_addresses.intersection(
                unused_addresses_in_project
            )
    if unused_addresses is None:
        raise ValueError(f"Expected nonnull {unused_addresses} at this point.")
    return unused_addresses


def find_unused_views() -> None:
    print("\nLooking for views that are unused across all projects ... ")
    unused_addresses = get_unused_across_all_projects_addresses_from_all_views_dag()

    if not unused_addresses:
        print("✅ Found no unused BQ views that are eligible for deletion")
        return

    print(
        f"⚠️ Found {len(unused_addresses)} BQ views that are not used in any project "
        f"and may be eligible for deletion:\n"
    )
    for address in sorted(unused_addresses):
        print(address.to_str())

    print(
        "\n⚠️ PLEASE NOTE ⚠️: The information this script has is incomplete. Please "
        "check with the view owner and verify that the view is not referenced in "
        "Looker before deleting."
    )


if __name__ == "__main__":
    find_unused_views()
