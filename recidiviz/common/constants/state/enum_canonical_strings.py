# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2019 Recidiviz, Inc.
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
Strings used as underlying representation of enum values for state-specific application and database code.

NOTE: Changing ANY STRING VALUE in this file will require a database migration. The Python values pointing to the
strings can be renamed without issue.

SQLAlchemy represents SQL enums as strings, and uses the string representation to pass values to the database. This
means any change to the string values requires a database migration. Therefore in order to keep the code as flexible
as possible, the string values should never be used directly. Storing the strings in this file and only referring to
them by their values here allows us to structure the application layer code any way we want, while only requiring the
database to be updated when an enum value is created or removed.
"""

from typing import Dict

# Shared

# This value should be used ONLY in cases where the external data source
# explicitly specifies a value as "unknown". It should NOT be treated as a
# default value for enums that are not provided (which should be represented
# with None/NULL).
external_unknown = "EXTERNAL_UNKNOWN"
# This value is used by status enums to denote that no status for an entity was
# provided by the source, but the entity itself was found in the source.
present_without_info = "PRESENT_WITHOUT_INFO"
# This value is used when the external data source specifies a known value for
# an enum field, but we internally don't have an enum value that it should map
# to. This should NOT be treated as a default value for enums fields that are
# not provided.
internal_unknown = "INTERNAL_UNKNOWN"


SHARED_ENUM_VALUE_DESCRIPTIONS: Dict[str, str] = {
    internal_unknown: """This value is used when the external data source specifies a known value for an enum field, but we internally don't have an enum value that it should map to. This should NOT be treated as a default value for enums fields that are not provided.""",
    external_unknown: """This value is used ONLY in cases where the external data source explicitly specifies a value as "unknown". It should NOT be treated as a default value for enums that are not provided (which should be represented with None/NULL).""",
    present_without_info: """This value is used by status enums to denote that no status for an entity was provided by the source, but the entity itself was found in the source.""",
}


# state_agent.py
state_agent_correctional_officer = "CORRECTIONAL_OFFICER"
state_agent_judge = "JUDGE"
state_agent_justice = "JUSTICE"
state_agent_supervision_officer = "SUPERVISION_OFFICER"

# state_assessment.py
state_assessment_class_risk = "RISK"
state_assessment_class_sex_offense = "SEX_OFFENSE"
state_assessment_class_social = "SOCIAL"
state_assessment_class_substance_abuse = "SUBSTANCE_ABUSE"

state_assessment_type_cssm = "CSSM"
state_assessment_type_hiq = "HIQ"
state_assessment_type_j_soap = "J_SOAP"
state_assessment_type_lsir = "LSIR"
state_assessment_type_odara = "ODARA"
state_assessment_type_oras_community_supervision = "ORAS_COMMUNITY_SUPERVISION"
state_assessment_type_oras_community_supervision_screening = (
    "ORAS_COMMUNITY_SUPERVISION_SCREENING"
)
state_assessment_type_oras_misdemeanor_assessment = "ORAS_MISDEMEANOR_ASSESSMENT"
state_assessment_type_oras_misdemeanor_screening = "ORAS_MISDEMEANOR_SCREENING"
state_assessment_type_oras_pre_trial = "ORAS_PRE_TRIAL"
state_assessment_type_oras_prison_screening = "ORAS_PRISON_SCREENING"
state_assessment_type_oras_prison_intake = "ORAS_PRISON_INTAKE"
state_assessment_type_oras_reentry = "ORAS_REENTRY"
state_assessment_type_oras_supplemental_reentry = "ORAS_SUPPLEMENTAL_REENTRY"
state_assessment_type_oyas = "OYAS"
state_assessment_type_pa_rst = "PA_RST"
state_assessment_type_psa = "PSA"
state_assessment_type_sorac = "SORAC"
state_assessment_type_sotips = "SOTIPS"
state_assessment_type_spin_w = "SPIN_W"
state_assessment_type_stable = "STABLE"
state_assessment_type_static_99 = "STATIC_99"
state_assessment_type_strong_r = "STRONG_R"
state_assessment_type_tcu_drug_screen = "TCU_DRUG_SCREEN"

state_assessment_level_minimum = "MINIMUM"
state_assessment_level_low = "LOW"
state_assessment_level_low_medium = "LOW_MEDIUM"
state_assessment_level_medium = "MEDIUM"
state_assessment_level_medium_high = "MEDIUM_HIGH"
state_assessment_level_moderate = "MODERATE"
state_assessment_level_high = "HIGH"
state_assessment_level_very_high = "VERY_HIGH"
state_assessment_level_maximum = "MAXIMUM"

# state_charge.py
state_charge_classification_type_civil = "CIVIL"
state_charge_classification_type_felony = "FELONY"
state_charge_classification_type_infraction = "INFRACTION"
state_charge_classification_type_misdemeanor = "MISDEMEANOR"
state_charge_classification_type_other = "OTHER"

state_charge_status_acquitted = "ACQUITTED"
state_charge_status_adjudicated = "ADJUDICATED"
state_charge_status_completed = "COMPLETED_SENTENCE"
state_charge_status_convicted = "CONVICTED"
state_charge_status_dropped = "DROPPED"
state_charge_status_inferred_dropped = "INFERRED_DROPPED"
state_charge_status_pending = "PENDING"
state_charge_status_pretrial = "PRETRIAL"
state_charge_status_sentenced = "SENTENCED"
state_charge_status_transferred_away = "TRANSFERRED_AWAY"

# state_court_case.py

# state_incarceration.py
state_incarceration_type_county_jail = "COUNTY_JAIL"
state_incarceration_type_federal_prison = "FEDERAL_PRISON"
state_incarceration_type_out_of_state = "OUT_OF_STATE"
state_incarceration_type_state_prison = "STATE_PRISON"

# state_incarceration_incident.py
state_incarceration_incident_type_contraband = "CONTRABAND"
state_incarceration_incident_type_disorderly_conduct = "DISORDERLY_CONDUCT"
state_incarceration_incident_type_escape = "ESCAPE"
state_incarceration_incident_type_minor_offense = "MINOR_OFFENSE"
state_incarceration_incident_type_positive = "POSITIVE"
state_incarceration_incident_type_report = "REPORT"
state_incarceration_incident_type_violence = "VIOLENCE"

state_incarceration_incident_outcome_cell_confinement = "CELL_CONFINEMENT"
state_incarceration_incident_outcome_disciplinary_labor = "DISCIPLINARY_LABOR"
state_incarceration_incident_outcome_dismissed = "DISMISSED"
state_incarceration_incident_outcome_external_prosecution = "EXTERNAL_PROSECUTION"
state_incarceration_incident_outcome_financial_penalty = "FINANCIAL_PENALTY"
state_incarceration_incident_outcome_good_time_loss = "GOOD_TIME_LOSS"
state_incarceration_incident_outcome_miscellaneous = "MISCELLANEOUS"
state_incarceration_incident_outcome_not_guilty = "NOT_GUILTY"
state_incarceration_incident_outcome_privilege_loss = "PRIVILEGE_LOSS"
state_incarceration_incident_outcome_restricted_confinement = "RESTRICTED_CONFINEMENT"
state_incarceration_incident_outcome_solitary = "SOLITARY"
state_incarceration_incident_outcome_treatment = "TREATMENT"
state_incarceration_incident_outcome_warning = "WARNING"

# state_incarceration_period.py
state_incarceration_period_admission_reason_admitted_in_error = "ADMITTED_IN_ERROR"
state_incarceration_period_admission_reason_admitted_from_supervision = (
    "ADMITTED_FROM_SUPERVISION"
)
state_incarceration_period_admission_reason_new_admission = "NEW_ADMISSION"
state_incarceration_period_admission_reason_revocation = "REVOCATION"
state_incarceration_period_admission_reason_return_from_court = "RETURN_FROM_COURT"
state_incarceration_period_admission_reason_return_from_erroneous_release = (
    "RETURN_FROM_ERRONEOUS_RELEASE"
)
state_incarceration_period_admission_reason_return_from_escape = "RETURN_FROM_ESCAPE"
state_incarceration_period_admission_reason_return_from_temporary_release = (
    "RETURN_FROM_TEMPORARY_RELEASE"
)
state_incarceration_period_admission_reason_sanction_admission = "SANCTION_ADMISSION"
state_incarceration_period_admission_reason_status_change = "STATUS_CHANGE"
state_incarceration_period_admission_reason_temporary_custody = "TEMPORARY_CUSTODY"
state_incarceration_period_admission_reason_transfer = "TRANSFER"
state_incarceration_period_admission_reason_transfer_from_other_jurisdiction = (
    "TRANSFER_FROM_OTHER_JURISDICTION"
)

state_incarceration_period_release_reason_commuted = "COMMUTED"
state_incarceration_period_release_reason_compassionate = "COMPASSIONATE"
state_incarceration_period_release_reason_conditional_release = "CONDITIONAL_RELEASE"
state_incarceration_period_release_reason_court_order = "COURT_ORDER"
state_incarceration_period_release_reason_death = "DEATH"
state_incarceration_period_release_reason_escape = "ESCAPE"
state_incarceration_period_release_reason_execution = "EXECUTION"
state_incarceration_period_release_reason_pardoned = "PARDONED"
state_incarceration_period_release_reason_released_from_erroneous_admission = (
    "RELEASED_FROM_ERRONEOUS_ADMISSION"
)
state_incarceration_period_release_reason_released_from_temporary_custody = (
    "RELEASED_FROM_TEMPORARY_CUSTODY"
)
state_incarceration_period_release_reason_released_in_error = "RELEASED_IN_ERROR"
state_incarceration_period_release_reason_released_to_supervision = (
    "RELEASED_TO_SUPERVISION"
)
state_incarceration_period_release_reason_temporary_release = "TEMPORARY_RELEASE"
state_incarceration_period_release_reason_sentence_served = "SENTENCE_SERVED"
state_incarceration_period_release_reason_status_change = "STATUS_CHANGE"
state_incarceration_period_release_reason_transfer = "TRANSFER"
state_incarceration_period_release_reason_transfer_to_other_jurisdiction = (
    "TRANSFER_TO_OTHER_JURISDICTION"
)
state_incarceration_period_release_reason_vacated = "VACATED"

# state_person_alias.py
state_person_alias_alias_type_affiliation_name = "AFFILIATION_NAME"
state_person_alias_alias_type_alias = "ALIAS"
state_person_alias_alias_type_given_name = "GIVEN_NAME"
state_person_alias_alias_type_maiden_name = "MAIDEN_NAME"
state_person_alias_alias_type_nickname = "NICKNAME"

state_gender_female = "FEMALE"
state_gender_male = "MALE"
state_gender_other = "OTHER"
state_gender_trans = "TRANS"
state_gender_trans_female = "TRANS_FEMALE"
state_gender_trans_male = "TRANS_MALE"

state_race_american_indian = "AMERICAN_INDIAN_ALASKAN_NATIVE"
state_race_asian = "ASIAN"
state_race_black = "BLACK"
state_race_hawaiian = "NATIVE_HAWAIIAN_PACIFIC_ISLANDER"
state_race_other = "OTHER"
state_race_white = "WHITE"

state_ethnicity_hispanic = "HISPANIC"
state_ethnicity_not_hispanic = "NOT_HISPANIC"

state_residency_status_homeless = "HOMELESS"
state_residency_status_permanent = "PERMANENT"
state_residency_status_transient = "TRANSIENT"

# state_sentence.py
state_sentence_status_commuted = "COMMUTED"
state_sentence_status_completed = "COMPLETED"
state_sentence_status_pardoned = "PARDONED"
state_sentence_status_pending = "PENDING"
state_sentence_status_revoked = "REVOKED"
state_sentence_status_sanctioned = "SANCTIONED"
state_sentence_status_serving = "SERVING"
state_sentence_status_suspended = "SUSPENDED"
state_sentence_status_vacated = "VACATED"

# state_supervision_sentence.py
state_supervision_sentence_supervision_type_civil_commitment = "CIVIL_COMMITMENT"
state_supervision_sentence_supervision_type_community_corrections = (
    "COMMUNITY_CORRECTIONS"
)
state_supervision_sentence_supervision_type_halfway_house = "HALFWAY_HOUSE"
state_supervision_sentence_supervision_type_parole = "PAROLE"
state_supervision_sentence_supervision_type_post_confinement = "POST_CONFINEMENT"
state_supervision_sentence_supervision_type_pre_confinement = "PRE_CONFINEMENT"
state_supervision_sentence_supervision_type_probation = "PROBATION"

# state_supervision_period.py
state_supervision_period_supervision_type_absconsion = "ABSCONSION"
state_supervision_period_supervision_type_informal_probation = "INFORMAL_PROBATION"
state_supervision_period_supervision_type_investigation = "INVESTIGATION"
state_supervision_period_supervision_type_parole = "PAROLE"
state_supervision_period_supervision_type_probation = "PROBATION"
state_supervision_period_supervision_type_dual = "DUAL"
state_supervision_period_supervision_type_community_confinement = (
    "COMMUNITY_CONFINEMENT"
)
state_supervision_period_supervision_type_bench_warrant = "BENCH_WARRANT"

state_supervision_period_admission_reason_absconsion = "ABSCONSION"
state_supervision_period_admission_reason_conditional_release = "CONDITIONAL_RELEASE"
state_supervision_period_admission_reason_court_sentence = "COURT_SENTENCE"
state_supervision_period_admission_reason_investigation = "INVESTIGATION"
state_supervision_period_admission_reason_transfer_from_other_jurisdiction = (
    "TRANSFER_FROM_OTHER_JURISDICTION"
)
state_supervision_period_admission_reason_transfer_within_state = (
    "TRANSFER_WITHIN_STATE"
)
state_supervision_period_admission_reason_return_from_absconsion = (
    "RETURN_FROM_ABSCONSION"
)
state_supervision_period_admission_reason_return_from_suspension = (
    "RETURN_FROM_SUSPENSION"
)

state_supervision_period_status_terminated = "TERMINATED"
state_supervision_period_status_under_supervision = "UNDER_SUPERVISION"

state_supervision_period_supervision_level_minimum = "MINIMUM"
state_supervision_period_supervision_level_medium = "MEDIUM"
state_supervision_period_supervision_level_high = "HIGH"
state_supervision_period_supervision_level_maximum = "MAXIMUM"
state_supervision_period_supervision_level_incarcerated = "INCARCERATED"
state_supervision_period_supervision_level_in_custody = "IN_CUSTODY"
state_supervision_period_supervision_level_diversion = "DIVERSION"
state_supervision_period_supervision_level_interstate_compact = "INTERSTATE_COMPACT"
state_supervision_period_supervision_level_electronic_monitoring_only = (
    "ELECTRONIC_MONITORING_ONLY"
)
state_supervision_period_supervision_level_limited = "LIMITED"
state_supervision_period_supervision_level_unsupervised = "UNSUPERVISED"
state_supervision_period_supervision_level_unassigned = "UNASSIGNED"

state_supervision_period_termination_reason_absconsion = "ABSCONSION"
state_supervision_period_termination_reason_commuted = "COMMUTED"
state_supervision_period_termination_reason_death = "DEATH"
state_supervision_period_termination_reason_discharge = "DISCHARGE"
state_supervision_period_termination_reason_dismissed = "DISMISSED"
state_supervision_period_termination_reason_expiration = "EXPIRATION"
state_supervision_period_termination_reason_investigation = "INVESTIGATION"
state_supervision_period_termination_reason_pardoned = "PARDONED"
state_supervision_period_termination_reason_transfer_to_other_jurisdiction = (
    "TRANSFER_TO_OTHER_JURISDICTION"
)
state_supervision_period_termination_reason_transfer_within_state = (
    "TRANSFER_WITHIN_STATE"
)
state_supervision_period_termination_reason_return_from_absconsion = (
    "RETURN_FROM_ABSCONSION"
)
state_supervision_period_termination_reason_return_to_incarceration = (
    "RETURN_TO_INCARCERATION"
)
state_supervision_period_termination_reason_revocation = "REVOCATION"
state_supervision_period_termination_reason_suspension = "SUSPENSION"

# state_supervision_case_type_entry.py
state_supervision_case_type_alcohol_drug = "ALCOHOL_DRUG"
state_supervision_case_type_domestic_violence = "DOMESTIC_VIOLENCE"
state_supervision_case_type_drug_court = "DRUG_COURT"
state_supervision_case_type_family_court = "FAMILY_COURT"
state_supervision_case_type_general = "GENERAL"
state_supervision_case_type_mental_health_court = "MENTAL_HEALTH_COURT"
state_supervision_case_type_serious_mental_illness = "SERIOUS_MENTAL_ILLNESS"
state_supervision_case_type_sex_offense = "SEX_OFFENSE"
state_supervision_case_type_veterans_court = "VETERANS_COURT"

# state_supervision_violation.py
state_supervision_violation_type_absconded = "ABSCONDED"
state_supervision_violation_type_escaped = "ESCAPED"
state_supervision_violation_type_felony = "FELONY"
state_supervision_violation_type_law = "LAW"
state_supervision_violation_type_misdemeanor = "MISDEMEANOR"
state_supervision_violation_type_municipal = "MUNICIPAL"
state_supervision_violation_type_technical = "TECHNICAL"

# state_supervision_violation_response.py
state_supervision_violation_response_type_citation = "CITATION"
state_supervision_violation_response_type_violation_report = "VIOLATION_REPORT"
state_supervision_violation_response_type_permanent_decision = "PERMANENT_DECISION"

state_supervision_violation_response_decision_community_service = "COMMUNITY_SERVICE"
state_supervision_violation_response_decision_continuance = "CONTINUANCE"
state_supervision_violation_response_decision_delayed_action = "DELAYED_ACTION"
state_supervision_violation_response_decision_extension = "EXTENSION"
state_supervision_violation_response_decision_new_conditions = "NEW_CONDITIONS"
state_supervision_violation_response_decision_other = "OTHER"
state_supervision_violation_response_decision_revocation = "REVOCATION"
state_supervision_violation_response_decision_privileges_revoked = "PRIVILEGES_REVOKED"
state_supervision_violation_response_decision_service_termination = (
    "SERVICE_TERMINATION"
)
state_supervision_violation_response_decision_shock_incarceration = (
    "SHOCK_INCARCERATION"
)
state_supervision_violation_response_decision_specialized_court = "SPECIALIZED_COURT"
state_supervision_violation_response_decision_suspension = "SUSPENSION"
state_supervision_violation_response_decision_treatment_in_prison = (
    "TREATMENT_IN_PRISON"
)
state_supervision_violation_response_decision_treatment_in_field = "TREATMENT_IN_FIELD"
state_supervision_violation_response_decision_violation_unfounded = (
    "VIOLATION_UNFOUNDED"
)
state_supervision_violation_response_decision_warning = "WARNING"
state_supervision_violation_response_decision_warrant_issued = "WARRANT_ISSUED"

state_supervision_violation_response_deciding_body_type_court = "COURT"
state_supervision_violation_response_deciding_body_parole_board = "PAROLE_BOARD"
state_supervision_violation_response_deciding_body_type_supervision_officer = (
    "SUPERVISION_OFFICER"
)

# state_program_assignment.py
state_program_assignment_participation_status_denied = "DENIED"
state_program_assignment_participation_status_discharged = "DISCHARGED"
state_program_assignment_participation_status_in_progress = "IN_PROGRESS"
state_program_assignment_participation_status_pending = "PENDING"
state_program_assignment_participation_status_refused = "REFUSED"

state_program_assignment_discharge_reason_absconded = "ABSCONDED"
state_program_assignment_discharge_reason_adverse_termination = "ADVERSE_TERMINATION"
state_program_assignment_discharge_reason_completed = "COMPLETED"
state_program_assignment_discharge_reason_moved = "MOVED"
state_program_assignment_discharge_reason_opted_out = "OPTED_OUT"
state_program_assignment_discharge_reason_program_transfer = "PROGRAM_TRANSFER"
state_program_assignment_discharge_reason_reincarcerated = "REINCARCERATED"

# state_specialized_purpose_for_incarceration
state_specialized_purpose_for_incarceration_general = "GENERAL"
state_specialized_purpose_for_incarceration_parole_board_hold = "PAROLE_BOARD_HOLD"
state_specialized_purpose_for_incarceration_shock_incarceration = "SHOCK_INCARCERATION"
state_specialized_purpose_for_incarceration_treatment_in_prison = "TREATMENT_IN_PRISON"
state_specialized_purpose_for_incarceration_temporary_custody = "TEMPORARY_CUSTODY"
state_specialized_purpose_for_incarceration_weekend_confinement = "WEEKEND_CONFINEMENT"

# state_early_discharge.py

state_early_discharge_decision_request_denied = "REQUEST_DENIED"
state_early_discharge_decision_sentence_termination_granted = (
    "SENTENCE_TERMINATION_GRANTED"
)
state_early_discharge_decision_unsupervised_probation_granted = (
    "UNSUPERVISED_PROBATION_GRANTED"
)

state_early_discharge_decision_status_pending = "PENDING"
state_early_discharge_decision_status_decided = "DECIDED"
state_early_discharge_decision_status_invalid = "INVALID"

# state_supervision_contact.py
state_supervision_contact_type_direct = "DIRECT"
state_supervision_contact_type_collateral = "COLLATERAL"
state_supervision_contact_type_both_collateral_and_direct = "BOTH_COLLATERAL_AND_DIRECT"

state_supervision_contact_method_telephone = "TELEPHONE"
state_supervision_contact_method_written_message = "WRITTEN_MESSAGE"
state_supervision_contact_method_virtual = "VIRTUAL"
state_supervision_contact_method_in_person = "IN_PERSON"

state_supervision_contact_reason_emergency_contact = "EMERGENCY_CONTACT"
state_supervision_contact_reason_general_contact = "GENERAL_CONTACT"
state_supervision_contact_reason_initial_contact = "INITIAL_CONTACT"

state_supervision_contact_status_attempted = "ATTEMPTED"
state_supervision_contact_status_completed = "COMPLETED"

state_supervision_contact_location_court = "COURT"
state_supervision_contact_location_field = "FIELD"
state_supervision_contact_location_jail = "JAIL"
state_supervision_contact_location_place_of_employment = "PLACE_OF_EMPLOYMENT"
state_supervision_contact_location_residence = "RESIDENCE"
state_supervision_contact_location_supervision_office = "SUPERVISION_OFFICE"
state_supervision_contact_location_treatment_provider = "TREATMENT_PROVIDER"
state_supervision_contact_location_law_enforcement_agency = "LAW_ENFORCEMENT_AGENCY"
state_supervision_contact_location_parole_commission = "PAROLE_COMMISSION"
state_supervision_contact_location_alternative_work_site = "ALTERNATIVE_WORK_SITE"

# state_shared_enums.py

state_acting_body_type_court = "COURT"
state_acting_body_type_parole_board = "PAROLE_BOARD"
state_acting_body_type_supervision_officer = "SUPERVISION_OFFICER"
state_acting_body_type_sentenced_person = "SENTENCED_PERSON"


state_custodial_authority_court = "COURT"
state_custodial_authority_federal = "FEDERAL"
state_custodial_authority_other_country = "OTHER_COUNTRY"
state_custodial_authority_other_state = "OTHER_STATE"
state_custodial_authority_supervision_authority = "SUPERVISION_AUTHORITY"
state_custodial_authority_state_prison = "STATE_PRISON"
