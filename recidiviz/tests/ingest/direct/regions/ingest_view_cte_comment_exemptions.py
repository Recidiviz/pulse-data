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
"""
These ingest views have undocumented CTEs.

Assume comments for a CTE go above the alias, like so:

WITH
-- this explains table 1
table_1 AS (
    SELECT * FROM A
),
-- this explains table 2
table_2 AS (
    SELECT * FROM B JOIN C USING(col)
)
SELECT * FROM table_1 UNION ALL SELECT * FROM table_2
"""

from typing import Dict, List

from recidiviz.common.constants.states import StateCode

# state_code -> ingest_view_name -> cte name
# We don't pass the state code Enum because the test class heirarchy that
# gets us to this point uses a str :(

# When we check ingest view CTEs, we will check the ENTIRE view.
# This data structure tracking individual CTEs is to make it easy
# to see what currently needs updating.
# This does mean that updating an ingest view with CTE documentation
# means updating the entire ingest view.
THESE_INGEST_VIEWS_HAVE_UNDOCUMENTED_CTES: Dict[StateCode, Dict[str, List[str]]] = {
    StateCode.US_AR: {
        "employment_period": ["employer_address"],
        "incarceration_incident": ["unpivoted_violation_outcomes", "violations"],
        "person": ["op_cleaned", "ora_deduped"],
        "supervision_period": [
            "cleaned_se",
            "constructed_periods",
            "loc_changes",
            "periods_with_info",
            "se_unique_dates",
            "special_status_changes",
            "supervisory_changes",
            "tl_periods",
        ],
        "supervision_violation": ["offenses", "offenses_and_sanctions", "sanctions"],
    },
    StateCode.US_PA: {
        "board_action": ["distinct_codes", "sci_actions"],
        "ccis_incarceration_period": [
            "admission_movements",
            "all_movements",
            "all_movements_without_invalid_edges",
            "full_periods",
            "inmate_number_with_control_numbers",
            "movements_base",
            "movements_with_single_control_number",
            "periods",
            "program_base",
            "program_movements",
            "release_movements",
            "transfer_movements",
            "valid_periods",
        ],
        "dbo_tblInmTestScore": ["all_test_scores"],
        "doc_person_info": [
            "bad_address_field_values",
            "info_ranked_by_recency",
            "most_recent_info",
            "people",
            "recidiviz_primary_person_ids",
            "search_inmate_info_with_primary_ids",
        ],
        "person_external_ids": ["recidiviz_primary_person_ids"],
        "program_assignment": [
            "TrtClassCode_decode",
            "TrtClassCode_trim",
            "program_data",
        ],
        "sci_incarceration_period": [
            "critical_movements",
            "movements",
            "movements_base",
            "movements_with_inflection_indicators",
            "periods",
            "recidiviz_primary_person_ids",
            "sentence_types",
        ],
        "supervision_period_v4": [
            "agent_employee_numbers",
            "agent_history_base",
            "agent_update_dates",
            "agent_update_edges_with_district",
            "all_update_dates",
            "conditions_by_parole_count_id",
            "edges_with_sequence_numbers",
            "end_count_edges",
            "filtered_edges",
            "hydrated_edges",
            "hydrated_edges_better_districts",
            "parole_count_id_level_info",
            "parole_count_id_level_info_base",
            "start_count_edges",
            "supervision_periods",
        ],
        "supervision_staff": [
            "cleaned_staff_from_agent_history",
            "cleaned_supervisors_from_agent_history",
            "raw_staff_from_agent_history",
            "raw_supervisors_from_agent_history",
            "staff_from_contacts",
            "staff_from_roster",
        ],
        "supervision_staff_caseload_type_period": [
            "all_periods",
            "critical_dates",
            "roster_data",
        ],
        "supervision_staff_location_period": [
            "all_periods",
            "cleaned_data",
            "contacts_data",
            "critical_dates",
            "location_external_ids",
            "roster_data",
        ],
        "supervision_staff_role_period": [
            "all_periods",
            "critical_dates_from_roster",
            "prelim_roster_periods",
            "staff_from_agent_history",
            "staff_from_contacts",
            "staff_from_roster",
            "supervisors_from_agent_history",
        ],
        "supervision_staff_supervisor_period": [
            "all_periods",
            "critical_dates",
            "filtered_data",
            "roster_data",
        ],
        "supervision_violation": ["base_violations"],
        "supervision_violation_response": ["base_sanctions"],
    },
    StateCode.US_ME: {
        "CURRENT_STATUS_incarceration_periods_v2": [
            "all_bed_assignments",
            "bed_assignment_periods",
            "continuous_bed_assignment_periods",
            "get_next_and_prev_statuses",
            "get_status_end_dates",
            "incarceration_periods",
            "join_statuses_to_bed_assignments",
            "join_statuses_to_movements",
            "movements_with_next_values",
            "order_status_dates_chronologically",
            "previous_movements_for_deduplication",
            "ranked_movements",
            "ranked_movements_release",
            "select_next_effective_datetime",
            "statuses",
            "statuses_and_transfers_with_parsed_dates",
            "transfers",
        ],
        "assessments": [
            "all_assessments",
            "lsi_assessments",
            "lsi_ratings",
            "non_lsi_assessments",
        ],
        "incarceration_sentences_v2": [
            "charges",
            "conditions",
            "judges",
            "sentences",
            "terms",
        ],
        "incarceration_task_deadline": ["sentences", "sorted", "terms"],
        "supervision_periods": [
            "join_statuses_and_officers",
            "order_status_dates_chronologically",
            "select_next_effective_datetime",
            "statuses",
            "statuses_and_officers_with_prev_and_next",
            "statuses_and_transfers_with_parsed_dates",
            "supervision_officer_assignments",
            "supervision_officer_assignments_dates",
            "supervision_periods",
            "transfers",
        ],
        "supervision_sentences_v2": [
            "charges",
            "conditions",
            "judges",
            "sentences",
            "terms",
        ],
        "supervision_task_deadline": ["sentences", "sorted", "terms"],
    },
    StateCode.US_NC: {
        "incarceration_periods": ["remove_nested_periods", "sentences"],
    },
    StateCode.US_IX: {
        "discharge_from_incarceration_deadline": [
            "SentenceOrderDates",
            "lag_cte",
            "last_val_cte",
            "rows_with_eligible",
            "sentences_base",
            "termOrderDates",
            "term_base",
        ],
        "discharge_from_supervision_deadline": ["SentenceBase", "lag_cte"],
        "early_discharge_parole": [
            "all_relevant_info_w_request_groupings",
            "all_requests",
        ],
        "incarceration_period": [
            "security_level_periods_cte",
            "transfer_periods_incarceration_cte",
        ],
        "incarceration_sentence_v2": [
            "RelatedSentence",
            "SentenceBase",
            "final_sentences",
        ],
        "state_staff": ["unioned"],
        "state_staff_caseload_type_periods": [
            "aggregated_periods",
            "all_employee_periods",
            "caseload_periods",
            "general_caseload_periods",
            "preliminary_periods",
            "specialized_caseload_periods",
            "transitional_periods",
        ],
        "state_staff_role_location_periods": [
            "final_periods",
            "preliminary_periods",
            "ref_Employee_latest",
        ],
        "state_staff_role_location_periods_legacy": [
            "all_periods",
            "final_periods",
            "preliminary_periods",
        ],
        "state_staff_supervisor_periods": [
            "aggregated_periods",
            "final_supervisor_periods",
            "preliminary_periods",
        ],
        "state_staff_supervisor_role_periods": [
            "aggregated_periods",
            "all_dates",
            "all_end_dates",
            "final_supervisor_periods",
            "preliminary_periods",
        ],
        "supervision_contacts": ["atlas_contacts", "contact_modes", "legacy_contacts"],
        "supervision_period": [
            "supervision_periods",
            "transfer_periods_supervision_cte",
        ],
        "supervision_sentence_v2": [
            "ProbationSupervision",
            "RelatedSentence",
            "SentenceBase",
            "final_sentences",
        ],
        "supervision_violation_legacy": ["stacked"],
        "transfer_to_supervision_deadline": ["SentenceBase", "lag_cte"],
    },
    StateCode.US_MI: {
        "assessments_v3": ["COMPAS"],
        "incarceration_incident": ["all_info", "offenses_info", "penalties_info"],
        "incarceration_periods_v3": [
            "ad_seg_designation",
            "all_dates",
            "deduped_lock_records",
            "deduped_movement_records",
            "final_lock_records",
            "final_movement_records",
            "final_movements",
            "internal_movements",
            "periods_basic",
            "periods_with_info",
        ],
        "sentences_v2": ["sentence_records_grouped"],
        "state_persons_v2": ["latest_booking_profiles"],
        "state_staff": [
            "compas",
            "corrected_adh_employee_additional_info",
            "corrected_adh_shuser",
            "omni",
            "omni_base",
        ],
        "state_staff_role_period_COMS": ["case_manager_dates"],
        "supervision_periods_v2": [
            "COMS_assignments",
            "COMS_levels",
            "coms_level_periods_base",
            "offender_booking_assignment",
            "offender_supervision_periods",
        ],
    },
    StateCode.US_TN: {
        "AssignedStaffSupervisionPeriod_v2": [
            "all_supervision_periods",
            "cleaned_assignment_periods",
            "close_oos_periods",
            "combined_officer_and_level",
            "last_movement_from_OffenderMovement",
            "officer_names",
            "override_backdated_discharges",
            "raw_supervision_level_periods",
            "split_officer_names",
            "supervision_level_sessions",
            "supervision_level_sessions_padded",
            "ungrouped_supervision_level_sessions",
        ],
        "CAFScoreAssessment": ["CAF_base", "latest_Classification"],
        "DisciplinaryIncarcerationIncident": [
            "disc_outcome",
            "disciplinary_base",
            "full_inc_and_out",
            "inc_base",
        ],
        "InferredViolations": [
            "abs_contact_notes",
            "all_inferred_violations",
            "cleaned_contact_note_type",
            "documented_violations",
            "grouping_likely_related_events",
            "identifying_likely_related_events",
            "ordered_vwars_vrpts",
        ],
        "OffenderName": ["filtered_out_nicknames", "normalized_rows"],
        "SentencesChargesAndCourtCases_v4": [
            "all_latest_sentences_joined",
            "all_sentence_sources_joined",
            "cleaned_Diversion_view",
            "cleaned_ISCSentence_view",
            "cleaned_Sentence_view",
            "consecutive_ISCRelated_sentences",
            "discharge_task_deadline_array",
            "most_recent_sentence_information",
            "order_sentence_actions_by_date_per_sentence",
            "special_conditions_aggregation",
            "special_conditions_date_grouping",
        ],
        "Staff": [
            "most_recent_staff_email_information",
            "most_recent_staff_information",
        ],
        "StaffRoleLocationPeriods": [
            "construct_periods",
            "create_unique_rows",
            "first_reported_title",
            "key_status_change_dates",
            "ranked_rows",
        ],
        "StaffSupervisorPeriods": [
            "construct_periods",
            "create_unique_rows",
            "first_reported_supervisor",
            "ranked_rows",
        ],
        "SupervisionContacts": ["contact_note_type_view"],
        "ViolationsAndSanctions": ["sanctions_groupings"],
    },
    StateCode.US_MO: {
        "offender_sentence_institution": [
            "most_recent_status_by_sentence",
            "sentence_status_xref",
            "shock_sentence",
        ],
        "offender_sentence_supervision": [
            "classified_status_bw",
            "collapsed_sentence_status_type_classification",
            "distinct_supervision_sentence_ids",
            "full_supervision_sentence_info",
            "most_recent_fso_and_status_for_sentence",
            "non_investigation_supervision_sentences_bu",
            "ranked_supervision_sentence_status_xref",
            "shock_sentence",
            "supervision_sentence_status_xref_bv",
            "supervision_sentence_status_xref_with_types",
            "supervision_sentence_type_classifier",
            "valid_sentences_with_status_xref",
        ],
        "oras_assessments_weekly_v2": [
            "assessments_with_duplicates",
            "duplicate_counts",
        ],
        "supervision_staff": ["APFX_ALL"],
        "supervision_staff_role_periods": [
            "apfx90_periods",
            "apfx91_periods",
            "apfx_all",
            "collapsed",
        ],
        "tak028_tak042_tak076_tak024_violation_reports": [
            "all_officers",
            "conditions_violated_cf",
            "finally_formed_violations_e6",
            "non_investigation_supervision_sentences_bu",
            "normalized_all_officers",
            "officers_with_recent_role",
            "officers_with_role_recency_ranks",
            "valid_sentences_cz",
        ],
        "tak034_tak026_tak039_apfx90_apfx91_supervision_enhancements_supervision_periods": [
            "all_officers",
            "all_supv_period_critical_dates",
            "augmented_field_assignments",
            "basic_supervision_periods",
            "field_assignments_ce",
            "field_assignments_next_beg_dt",
            "field_assignments_with_unique_date_spans",
            "field_assignments_with_valid_region",
            "first_non_inv_start_status_code",
            "non_inv_start_status_codes",
            "non_investigation_supv_period_spans",
            "normalized_all_officers",
            "officer_role_spans",
            "officers_with_role_time_ranks",
            "periods_with_officer_and_case_type_info",
            "periods_with_officer_case_type_and_supervision_type_info",
            "periods_with_officer_info",
            "status_bw",
            "statuses_on_days",
            "supervision_case_types",
            "supervision_type_assessments",
            "supervision_type_spans",
            "supervision_type_with_seq_num",
            "supv_period_partition_statuses",
            "supv_period_spans",
        ],
        "tak158_tak026_incarceration_periods": [
            "all_sub_sub_cycle_critical_dates",
            "cleaned_facility_locations",
            "cleaned_housing_details",
            "end_date_cte",
            "most_recent_status_updates",
            "start_and_end_dates_cte",
            "start_and_end_dates_different_days",
            "start_and_end_dates_join",
            "start_and_end_dates_same_day_facility",
            "start_and_end_dates_same_day_housing",
            "start_and_end_dates_same_day_status",
            "start_date_cte",
            "start_end_facility",
            "start_end_facility_housing",
            "status_bw",
            "sub_cycle_partition_statuses",
            "sub_subcycle_spans",
            "subcycle_close_status_change_dates",
            "subcycle_open_status_change_dates",
            "subcycle_partition_status_change_dates",
        ],
        "tak291_tak292_tak024_citations": [
            "citations_with_multiple_violations_jt",
            "finally_formed_citations_e6",
            "non_investigation_supervision_sentences_bu",
            "valid_sentences_js",
        ],
    },
    StateCode.US_CA: {
        "staff": [
            "add_emails",
            "prioritized",
            "staff_from_AgentParole",
            "staff_from_PersonParole",
            "unioned",
        ],
        "staff_location_and_role_period": ["pa_1_location_periods"],
    },
    StateCode.US_OZ: {
        "ageid_staff_supervisor": ["critical_dates"],
    },
    StateCode.US_CO: {
        "IncarcerationIncident": [
            "id",
            "inc_and_out",
            "inc_type",
            "incident_base",
            "incidents",
            "loc",
            "outcome",
        ],
        "IncarcerationPeriod": [
            "classified_movements",
            "final",
            "isp",
            "missing_releases_handled",
            "movements",
            "movements_with_direction",
            "ordered_movements",
            "periods",
            "permanent_moves",
        ],
        "IncarcerationSentence": ["commitprefix", "sequences"],
        "StatePerson": ["normalized_rows"],
    },
}
