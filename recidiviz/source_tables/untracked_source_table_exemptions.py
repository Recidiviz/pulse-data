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
"""Tables that exist in source table datasets but do not have source table YAML
configs. These are legacy tables grandfathered in before source table management
was established.

Any additions to this file should be reviewed by the Doppler team.
"""

# Legacy tables that existed before we had source table management, or have other
# valid exemption reasons.
# These are grandfathered in and won't trigger validation failures
ALLOWED_TABLES_IN_SOURCE_TABLE_DATASETS_WITH_NO_CONFIG: dict[str, set[str]] = {
    # All billing data
    "all_billing_data": {
        "gcp_billing_export_raw",
        "gcp_billing_export_resource_raw",
    },
    # Auth0 datasets
    "auth0_events": {
        "failed_login",
        "tracks",
    },
    "auth0_prod_action_logs": {
        "tracks",
    },
    # Case planning
    "case_planning_production": {
        "frontend_cpa_client_intake_chat_history_viewed",
        "identifies",
        "pages",
        "tracks",
        "users",
    },
    # External data sources
    "sendgrid_email_data": {
        "raw_sendgrid_email_data",
    },
    "vera_data": {
        "bridged_race_population_estimates",
        "incarceration_trends_rates_100k",
        "iob_race_gender_pop",
        "iob_state",
        "old_ky_state_aggregates_with_fips",
    },
    # GCS-backed tables
    "gcs_backed_tables": {
        "county_fips",
        "county_resident_adult_populations",
        "county_resident_populations",
        "us_co_incarceration_facility_map",
        "us_co_incarceration_facility_names",
        "us_id_incarceration_facility_map",
        "us_id_incarceration_facility_names",
        "us_id_supervision_district_names",
        "us_id_supervision_unit_to_district_map",
        "us_me_incarceration_facility_names",
        "us_mi_incarceration_facility_names",
        "us_mo_incarceration_facility_names",
        "us_nd_incarceration_facility_names",
        "us_tn_incarceration_facility_map",
        "us_tn_incarceration_facility_names",
        "us_tn_supervision_facility_names",
    },
    # Static reference tables
    "static_reference_tables": {
        "CIS_200_CASE_PLAN",
        "CIS_2010_GOAL_TYPE",
        "CIS_2011_DOMAIN_GOAL_TYPE",
        "CIS_2012_GOAL_STATUS",
        "CIS_201_GOALS",
        "CIS_404_DOCKET",
        "CIS_404_DOCKET_materialized",
        "CORRAL_REPORTS_RECLASS_ME",
        "admission_start_reason_dedup_priority",
        "ars_periods_explore",
        "county_to_supervision_office",
        "dopro_participant",
        "experiment_assignments",
        "experiment_assignments_materialized",
        "experiment_officer_assignments_materialized",
        "experiment_state_assignments_materialized",
        "experiments",
        "experiments_materialized",
        "geo_cis_referrals_matched",
        "ingest_timeline_tracker",
        "leadership_3",
        "leadership_eligible",
        "leadership_eligible_2",
        "line_staff_user_permissions",
        "ncic_codes",
        "ncic_to_nibrs_to_uccs",
        "offense_description_to_labels",
        "offense_description_to_labels_materialized",
        "pa_admin_supervision_current_pop",
        "pa_admin_supervision_current_pop_2",
        "po_monthly_report_manual_exclusions",
        "population_projection_facilities",
        "recidiviz_unified_product_test_users",
        "release_termination_reason_dedup_priority",
        "session_inferred_end_reasons",
        "session_inferred_start_reasons",
        "sessions_location_type_lookup",
        "sessions_location_type_lookup_materialized",
        "state_community_correction_facilities",
        "state_gender_population_counts",
        "state_incarceration_facility_capacity",
        "state_race_ethnicity_population_counts",
        "supervision_district_office_to_county",
        "synthetic_state_weights",
        "synthetic_state_weights_materialized",
        "team_1_absonsions_by_parole_agents",
        "team_1_discharge_in_3_months",
        "team_1_discharge_in_6_months",
        "team_1_revocations_over_window",
        "team_1_violation_free_6_months",
        "team_1_violations_over_window",
        "team_2_absonsions_by_parole_agents",
        "team_2_discharge_in_3_months",
        "team_2_discharge_in_6_months",
        "team_2_revocations_over_window",
        "team_2_violation_free_6_months",
        "team_2_violations_over_window",
        "team_3_absonsions_by_parole_agents",
        "team_3_discharge_in_3_months",
        "team_3_discharge_in_6_months",
        "team_3_revocations_over_window",
        "team_3_violation_free_6_months",
        "team_3_violations_over_window",
        "team_4_absonsions_by_parole_agents",
        "team_4_discharge_in_3_months",
        "team_4_discharge_in_6_months",
        "team_4_revocations_over_window",
        "team_4_violation_free_6_months",
        "team_4_violations_over_window",
        "team_5_absonsions_by_parole_agents",
        "team_5_discharge_in_3_months",
        "team_5_discharge_in_6_months",
        "team_5_revocations_over_window",
        "team_5_violation_free_6_months",
        "team_5_violations_over_window",
        "team_6_absonsions_by_parole_agents",
        "team_6_discharge_in_3_months",
        "team_6_discharge_in_6_months",
        "team_6_revocations_over_window",
        "team_6_violation_free_1_year",
        "team_6_violation_free_6_months",
        "team_6_violations_over_window",
        "test_assignments",
        "test_state_pop_view",
        "us_ca_milestones_team_1_discharges_in_3_months",
        "us_ca_validation_birthdays_cdcr",
        "us_ca_validation_violation_cdcr",
        "us_ca_validation_violation_recidiviz",
        "us_co_leadership_users",
        "us_id_leadership_users",
        "us_id_roster",
        "us_id_shelter_names",
        "us_ix_leadership_users",
        "us_ix_lsu_jii_d2_d4_d6_assignments",
        "us_ix_lsu_jii_d3_assignments",
        "us_ix_lsu_jii_sms_sent",
        "us_ix_lsu_jii_sms_sent_materialized",
        "us_ix_roster",
        "us_ix_rough_supervision_periods",
        "us_me_CIS_426_PROG_STATUS",
        "us_me_cis_908_ccs_location",
        "us_me_leadership_users",
        "us_me_roster",
        "us_mi_exclusion_lists",
        "us_mi_leadership_users",
        "us_mi_line_staff_email_reminders_workflows_assignments",
        "us_mo_rh_workflows_roster",
        "us_mo_tak025_sentence_status_xref",
        "us_mo_tak026_sentence_status",
        "us_mo_tak146_status_code_descriptions",
        "us_nd_covid_release_person_external_id",
        "us_nd_leadership_users",
        "us_nd_offense_codes",
        "us_nd_offense_in_custody_and_pos_report_data",
        "us_pa_location_id_to_district_map",
        "us_tn_contact_codes",
        "us_tn_leadership_users",
        "us_tn_roster",
        "us_tn_standards_admin",
        "zipcode_county_map",
    },
    # Population projection output data
    "population_projection_output_data": {
        "microsim_validation_data_raw",
        "validation_data",
    },
    # Hydration archive
    "hydration_archive": {
        "normalized_state_hydration_archive_old",
    },
    # TODO(#57653): Delete this table from BigQuery and remove this exemption
    "supplemental_data": {
        "us_id_case_note_matched_entities",
    },
    # Sentencing
    "sentencing": {"case_insights_rates_test"},
    # TODO(#57961): Delete these tables from BigQuery and remove this exemption
    "us_ar_raw_data": {
        "ARORA_INTAKES_After_Control_Intake_After_2_1_2017",
        "ARORA_Intake_Assessments_after_Control_Intake_Prior_2_1_2017_Date",
        "External_movements_No_Date_Exclusions",
        "Inmate_Program_Referrals__No_Date_Exclusions",
        "Probation_Revocations_After_Control_Intake_Dates___SUPERVISION_HISTORY_",
        "Probation_Sentences_No_Date_Exclusions",
        "Program_Achievements_in_ADC_Or_CCC_No_Date_Exclusions",
        "Program_Referrals_Post_Intake",
        "Reassessments_after_Date_1_Intakes",
        "SIS_Sentences_No_Date_Exclusions",
        "SSP_Sanction_Hearings_ParoleCases_After_Control_Intake_Dates",
        "State_Prison_and_Judicial_Transfer_Convictions_No_Date_Exclusions",
        "Supervision_Contacts_After_First__Study_Period_Intake",
        "Supervision_History_Events",
        "_PAROLE_REVO_HEARING_After_Control_Intake_Dates",
    },
    "us_ar_raw_data_secondary": {
        "ARORA_INTAKES_After_Control_Intake_After_2_1_2017",
        "ARORA_Intake_Assessments_after_Control_Intake_Prior_2_1_2017_Date",
        "External_movements_No_Date_Exclusions",
        "Inmate_Program_Referrals__No_Date_Exclusions",
        "Probation_Revocations_After_Control_Intake_Dates___SUPERVISION_HISTORY_",
        "Probation_Sentences_No_Date_Exclusions",
        "Program_Achievements_in_ADC_Or_CCC_No_Date_Exclusions",
        "Program_Referrals_Post_Intake",
        "Reassessments_after_Date_1_Intakes",
        "SIS_Sentences_No_Date_Exclusions",
        "SSP_Sanction_Hearings_ParoleCases_After_Control_Intake_Dates",
        "State_Prison_and_Judicial_Transfer_Convictions_No_Date_Exclusions",
        "Supervision_Contacts_After_First__Study_Period_Intake",
        "Supervision_History_Events",
        "_PAROLE_REVO_HEARING_After_Control_Intake_Dates",
    },
    # TODO(#57962): Delete these tables from BigQuery and remove this exemption
    "us_co_raw_data": {
        "eomis_CustodyClass",
        "eomis_admissionsummary",
        "eomis_bedassignment",
        "eomis_facilitybed",
        "eomis_gatepass",
        "eomis_offendernamealias",
        "eomis_plannedmovement",
    },
    "us_co_raw_data_secondary": {
        "eomis_CustodyClass",
        "eomis_admissionsummary",
        "eomis_bedassignment",
        "eomis_facilitybed",
        "eomis_gatepass",
        "eomis_offendernamealias",
        "eomis_plannedmovement",
    },
    # TODO(#57804): Delete this table from BigQuery and remove this exemption
    "us_az_raw_data": {"AZ_CS_OMS_SENTENCE_INFO"},
    # TODO(#57805): Delete these tables from BigQuery and remove this exemption
    "us_ia_raw_data": {
        "IA_DOC_CustodyClassificationFemale",
        "IA_DOC_CustodyClassificationMale",
        "IA_DOC_CustodyReclassificationFemale",
        "IA_DOC_CustodyReclassificationMale",
    },
    # TODO(#57806): Delete this table from BigQuery and remove this exemption
    "us_ix_raw_data": {"crs_OfdCourseEnrollmentStatus"},
    # TODO(#57807): Delete this table from BigQuery and remove this exemption
    "us_mi_raw_data": {"ADH_OFFENDER_SENTENCE_INFO", "Table_Data_TRANSCASE_FORMS_Prod"},
    "us_mi_raw_data_secondary": {
        "ADH_OFFENDER_SENTENCE_INFO",
        "Table_Data_TRANSCASE_FORMS_Prod",
    },
    # TODO(#57808): Delete these tables from BigQuery and remove this exemption
    "us_nd_raw_data": {
        "docstars_lsi_need",
        "elite_assessment_results",
        "elite_assessment_supervisors",
        "elite_assessments",
        "elite_courseactivities",
        "elite_offender_assessments",
        "elite_offenderassessmentitems",
        "elite_programservices",
        "recidiviz_elite_offences",
    },
    # TODO(#57809): Delete these tables from BigQuery and remove this exemption
    "us_ne_raw_data": {
        "view_BMS_lnkEventResponse",
        "view_BMS_lnkEventVio",
        "view_BMS_tbl_NoncompliantBehaviors",
        "view_BMS_tbl_NoncompliantResponses",
    },
    # TODO(#57810): Delete these tables from BigQuery and remove this exemption
    "us_ny_raw_data": {"ADMISSIONS", "RELEASES"},
    # TODO(#57811): Delete this table from BigQuery and remove this exemption
    "us_or_raw_data": {"RCDVZ_CISPRDDTA_CMOFRO"},
    # TODO(#57963): Delete this table from BigQuery and remove this exemption
    "us_tx_raw_data": {"ScheduledMeetings"},
    "us_tx_raw_data_secondary": {"ScheduledMeetings"},
    # TODO(#57812): Delete this table from BigQuery and remove this exemption
    "us_tn_raw_data": {"RECIDIVIZ_REFERENCE_incident_infraction_codes"},
    # Legacy validation oneoff tables
    "us_tn_validation_oneoffs": {
        "TDPOP_20220314",
        "tdpop_02_15_2022",
    },
    "us_ix_validation_oneoffs": {
        "sentence_info_parole_and_incarceration",
    },
    "us_pa_validation_oneoffs": {
        "2019_12_PA_reference_incarceration_population",
        "2019_12_PA_reference_supervision_population",
        "2019_12_PA_reference_supervision_termination",
        "2020",
        "2020 Dec Admissions",
        "2020_01_PA_reference_incarceration_population",
        "2020_01_PA_reference_supervision_population",
        "2020_02_PA_reference_incarceration_population",
        "2020_02_PA_reference_supervision_population",
        "2020_03_PA_reference_incarceration_population",
        "2020_03_PA_reference_supervision_population",
        "2020_04_PA_reference_incarceration_population",
        "2020_04_PA_reference_supervision_population",
        "2020_05_PA_reference_incarceration_population",
        "2020_05_PA_reference_supervision_population",
        "2020_1_PA_reference_supervision_termination",
        "2020_2_PA_reference_supervision_termination",
        "2020_3_PA_reference_supervision_termination",
        "2020_4_PA_reference_supervision_termination",
        "2020_5_PA_reference_supervision_termination",
        "PA_reference_Delaware_inmates_2019_12",
        "PA_reference_Delaware_inmates_2020_01",
        "PA_reference_Delaware_inmates_2020_02",
        "PA_reference_Delaware_inmates_2020_03",
        "PA_reference_Delaware_inmates_2020_04",
        "PA_reference_Delaware_inmates_2020_05",
        "incarceration_release_person_level_raw",
    },
    "us_nd_validation_oneoffs": {
        "ParoleCount",
        "ParoleCount_20210825",
        "cohort_2015",
        "cohort_2016",
        "cohort_2017",
        "cohort_2018",
        "cohort_2018_exclusions",
        "docr_recidivism",
        "recidivism_people_2016",
        "returns_2018",
        "updated_cohort_2016",
    },
    "us_mi_validation_oneoffs": {
        "person_race_20210730",
    },
    # Segment metrics datasets
    "jii_auth0_production_segment_metrics": {
        "auth0_login_succeeded",
        "tracks",
    },
    "jii_backend_production_segment_metrics": {
        "backend_edovo_login_denied",
        "backend_edovo_login_failed",
        "backend_edovo_login_internal_error",
        "backend_edovo_login_succeeded",
        "tracks",
    },
    "pulse_dashboard_segment_metrics": {
        "hello",
        "pages",
        "pages_view",
        "tracks",
        "tracks_view",
        "users",
    },
    # JII texting dashboards
    "jii_texting_dashboards_db_us_ix": {
        "Contact",
        "Person",
        "WorkflowExecution",
        "_GroupToPerson",
    },
    "jii_texting_dashboards_db_us_tx": {
        "Contact",
        "Person",
        "WorkflowExecution",
        "_GroupToPerson",
    },
    # Legacy ingest view results tables
    "us_ar_ingest_view_results": {
        "incarceration_sentence",
        "supervision_sentence",
    },
    "us_az_ingest_view_results": {
        "assessment",
        "state_charge",
        "state_task_deadline",
        "state_task_deadline_dtp",
        "state_task_deadline_tpr",
    },
    "us_mi_ingest_view_results": {
        "sentences_v2",
        "state_charge_v2",
    },
    "us_mo_ingest_view_results": {
        "incarceration_sentence",
        "offender_sentence_institution",
        "offender_sentence_supervision",
        "supervision_sentence",
        "supervision_staff_role_periods",
        "tak028_tak042_tak076_tak024_violation_reports",
        "tak034_tak026_tak039_apfx90_apfx91_supervision_enhancements_supervision_periods",
        "tak291_tak292_tak024_citations",
    },
    "us_nc_ingest_view_results": {
        "incarceration_periods",
        "incarceration_state_person",
    },
    "us_nd_ingest_view_results": {
        "elite_alias",
        "elite_sentence_group",
        "task_deadlines",
    },
    "us_or_ingest_view_results": {"Sentences"},
    "us_pa_ingest_view_results": {
        "supervision_period_v4",
        "supervision_staff",
        "supervision_staff_caseload_type_period",
        "supervision_staff_location_period",
        "supervision_staff_role_period",
        "supervision_staff_supervisor_period",
    },
    "us_tn_ingest_view_results": {
        "SupervisionContacts",
        "isc_sentence_status",
        "sentence_status",
    },
    # Legacy raw data tables in secondary datasets
    # TODO(#57804): Delete this table from BigQuery and remove this exemption
    "us_az_raw_data_secondary": {
        "AZ_CS_OMS_SENTENCE_INFO",
        "validation_daily_count_sheet_by_facility",
    },
    # TODO(#57805): Delete these tables from BigQuery and remove this exemption
    "us_ia_raw_data_secondary": {
        "IA_DOC_CustodyClassificationFemale",
        "IA_DOC_CustodyClassificationMale",
        "IA_DOC_CustodyReclassificationFemale",
        "IA_DOC_CustodyReclassificationMale",
    },
    # TODO(#57806): Delete this table from BigQuery and remove this exemption
    "us_ix_raw_data_secondary": {"crs_OfdCourseEnrollmentStatus"},
    # TODO(#57808): Delete these tables from BigQuery and remove this exemption
    "us_nd_raw_data_secondary": {
        "elite_assessment_results",
        "elite_assessment_supervisors",
        "elite_assessments",
        "elite_courseactivities",
        "elite_offender_assessments",
        "elite_offenderassessmentitems",
        "elite_offenderprogramprofiles",
        "elite_programservices",
        "recidiviz_elite_offences",
    },
    # TODO(#57809): Delete these tables from BigQuery and remove this exemption
    "us_ne_raw_data_secondary": {
        "view_BMS_lnkEventResponse",
        "view_BMS_lnkEventVio",
        "view_BMS_tbl_NoncompliantBehaviors",
        "view_BMS_tbl_NoncompliantResponses",
    },
    # TODO(#57810): Delete these tables from BigQuery and remove this exemption
    "us_ny_raw_data_secondary": {"ADMISSIONS", "RELEASES"},
    # TODO(#57811): Delete this table from BigQuery and remove this exemption
    "us_or_raw_data_secondary": {"RCDVZ_CISPRDDTA_CMOFRO"},
    # TODO(#57812): Delete this table from BigQuery and remove this exemption
    "us_tn_raw_data_secondary": {"RECIDIVIZ_REFERENCE_incident_infraction_codes"},
}
