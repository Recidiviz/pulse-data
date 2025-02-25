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

# state_agent.py
state_agent_correctional_officer = 'CORRECTIONAL_OFFICER'
state_agent_judge = 'JUDGE'
state_agent_parole_board_member = 'PAROLE_BOARD_MEMBER'
state_agent_supervision_officer = 'SUPERVISION_OFFICER'
state_agent_unit_supervisor = 'UNIT_SUPERVISOR'

# state_assessment.py
state_assessment_class_mental_health = 'MENTAL_HEALTH'
state_assessment_class_risk = 'RISK'
state_assessment_class_security_classification = 'SECURITY_CLASSIFICATION'
state_assessment_class_substance_abuse = 'SUBSTANCE_ABUSE'

state_assessment_type_asi = 'ASI'
state_assessment_type_lsir = 'LSIR'
# TODO(2668): Deprecated - Delete this enum value from our schema.
state_assessment_type_oras = 'ORAS'
state_assessment_type_oras_community_supervision = 'ORAS_COMMUNITY_SUPERVISION'
state_assessment_type_oras_community_supervision_screening = 'ORAS_COMMUNITY_SUPERVISION_SCREENING'
state_assessment_type_oras_misdemeanor_assessment = 'ORAS_MISDEMEANOR_ASSESSMENT'
state_assessment_type_oras_misdemeanor_screening = 'ORAS_MISDEMEANOR_SCREENING'
state_assessment_type_oras_pre_trial = 'ORAS_PRE_TRIAL'
state_assessment_type_oras_prison_screening = 'ORAS_PRISON_SCREENING'
state_assessment_type_oras_prison_intake = 'ORAS_PRISON_INTAKE'
state_assessment_type_oras_reentry = 'ORAS_REENTRY'
state_assessment_type_oras_static = 'ORAS_STATIC'
state_assessment_type_oras_supplemental_reentry = 'ORAS_SUPPLEMENTAL_REENTRY'
state_assessment_type_psa = 'PSA'
state_assessment_type_sorac = 'SORAC'

state_assessment_level_low = 'LOW'
state_assessment_level_low_medium = 'LOW_MEDIUM'
state_assessment_level_medium = 'MEDIUM'
state_assessment_level_medium_high = 'MEDIUM_HIGH'
state_assessment_level_moderate = 'MODERATE'
state_assessment_level_high = 'HIGH'
state_assessment_level_very_high = 'VERY_HIGH'
state_assessment_level_not_applicable = 'NOT_APPLICABLE'
state_assessment_level_undetermined = 'UNDETERMINED'

# state_charge.py
state_charge_classification_type_civil = 'CIVIL'
state_charge_classification_type_felony = 'FELONY'
state_charge_classification_type_infraction = 'INFRACTION'
state_charge_classification_type_misdemeanor = 'MISDEMEANOR'
state_charge_classification_type_other = 'OTHER'

# state_court_case.py
# TODO(1697): Add enum strings here

# state_fine.py
state_fine_status_paid = 'PAID'
state_fine_status_unpaid = 'UNPAID'

# state_incarceration.py
state_incarceration_type_county_jail = 'COUNTY_JAIL'
state_incarceration_type_state_prison = 'STATE_PRISON'

# state_incarceration_incident.py
state_incarceration_incident_type_contraband = 'CONTRABAND'
state_incarceration_incident_type_disorderly_conduct = 'DISORDERLY_CONDUCT'
state_incarceration_incident_type_escape = 'ESCAPE'
state_incarceration_incident_type_minor_offense = 'MINOR_OFFENSE'
state_incarceration_incident_type_positive = 'POSITIVE'
state_incarceration_incident_type_report = 'REPORT'
state_incarceration_incident_type_violence = 'VIOLENCE'

state_incarceration_incident_outcome_disciplinary_labor = 'DISCIPLINARY_LABOR'
state_incarceration_incident_outcome_dismissed = 'DISMISSED'
state_incarceration_incident_outcome_external_prosecution = 'EXTERNAL_PROSECUTION'
state_incarceration_incident_outcome_financial_penalty = 'FINANCIAL_PENALTY'
state_incarceration_incident_outcome_good_time_loss = 'GOOD_TIME_LOSS'
state_incarceration_incident_outcome_miscellaneous = 'MISCELLANEOUS'
state_incarceration_incident_outcome_not_guilty = 'NOT_GUILTY'
state_incarceration_incident_outcome_privilege_loss = 'PRIVILEGE_LOSS'
state_incarceration_incident_outcome_solitary = 'SOLITARY'
state_incarceration_incident_outcome_treatment = 'TREATMENT'
state_incarceration_incident_outcome_warning = 'WARNING'

# state_incarceration_period.py
state_incarceration_period_status_in_custody = 'IN_CUSTODY'
state_incarceration_period_status_not_in_custody = 'NOT_IN_CUSTODY'

state_incarceration_facility_security_level_maximum = 'MAXIMUM'
state_incarceration_facility_security_level_medium = 'MEDIUM'
state_incarceration_facility_security_level_minimum = 'MINIMUM'

state_incarceration_period_admission_reason_admitted_in_error = 'ADMITTED_IN_ERROR'
state_incarceration_period_admission_reason_new_admission = 'NEW_ADMISSION'
state_incarceration_period_admission_reason_parole_revocation = 'PAROLE_REVOCATION'
state_incarceration_period_admission_reason_probation_revocation = 'PROBATION_REVOCATION'
state_incarceration_period_admission_reason_dual_revocation = 'DUAL_REVOCATION'
state_incarceration_period_admission_reason_return_from_court = 'RETURN_FROM_COURT'
state_incarceration_period_admission_reason_return_from_erroneous_release = 'RETURN_FROM_ERRONEOUS_RELEASE'
state_incarceration_period_admission_reason_return_from_escape = 'RETURN_FROM_ESCAPE'
state_incarceration_period_admission_reason_temporary_custody = 'TEMPORARY_CUSTODY'
state_incarceration_period_admission_reason_transfer = 'TRANSFER'
state_incarceration_period_admission_reason_transferred_from_out_of_state = 'TRANSFERRED_FROM_OUT_OF_STATE'

state_incarceration_period_release_reason_commuted = 'COMMUTED'
state_incarceration_period_release_reason_compassionate = 'COMPASSIONATE'
state_incarceration_period_release_reason_conditional_release = 'CONDITIONAL_RELEASE'
state_incarceration_period_release_reason_court_order = 'COURT_ORDER'
state_incarceration_period_release_reason_death = 'DEATH'
state_incarceration_period_release_reason_escape = 'ESCAPE'
state_incarceration_period_release_reason_execution = 'EXECUTION'
state_incarceration_period_release_reason_released_from_temporary_custody = 'RELEASED_FROM_TEMPORARY_CUSTODY'
state_incarceration_period_release_reason_released_in_error = 'RELEASED_IN_ERROR'
state_incarceration_period_release_reason_sentence_served = 'SENTENCE_SERVED'
state_incarceration_period_release_reason_transfer = 'TRANSFER'
state_incarceration_period_release_reason_transferred_out_of_state = 'TRANSFERRED_OUT_OF_STATE'

# state_parole_decision.py
state_parole_decision_parole_denied = 'PAROLE_DENIED'
state_parole_decision_parole_granted = 'PAROLE_GRANTED'

# state_person_alias.py
state_person_alias_alias_type_affiliation_name = 'AFFILIATION_NAME'
state_person_alias_alias_type_alias = 'ALIAS'
state_person_alias_alias_type_given_name = 'GIVEN_NAME'
state_person_alias_alias_type_maiden_name = 'MAIDEN_NAME'
state_person_alias_alias_type_nickname = 'NICKNAME'

# state_sentence.py
state_sentence_status_commuted = 'COMMUTED'
state_sentence_status_completed = 'COMPLETED'
state_sentence_status_serving = 'SERVING'
state_sentence_status_suspended = 'SUSPENDED'
state_sentence_status_revoked = 'REVOKED'

# state_supervision.py
state_supervision_type_civil_commitment = 'CIVIL_COMMITMENT'
state_supervision_type_halfway_house = 'HALFWAY_HOUSE'
state_supervision_type_parole = 'PAROLE'
state_supervision_type_post_confinement = 'POST_CONFINEMENT'
state_supervision_type_pre_confinement = 'PRE_CONFINEMENT'
state_supervision_type_probation = 'PROBATION'

# supervision_period.py
state_supervision_period_supervision_type_parole = 'PAROLE'
state_supervision_period_supervision_type_probation = 'PROBATION'
state_supervision_period_supervision_type_dual = 'DUAL'

state_supervision_period_admission_reason_absconsion = 'ABSCONSION'
state_supervision_period_admission_reason_conditional_release = 'CONDITIONAL_RELEASE'
state_supervision_period_admission_reason_court_sentence = 'COURT_SENTENCE'
state_supervision_period_admission_reason_transfer_out_of_state = 'TRANSFER_OUT_OF_STATE'
state_supervision_period_admission_reason_transfer_within_state = 'TRANSFER_WITHIN_STATE'
state_supervision_period_admission_reason_return_from_absconsion = 'RETURN_FROM_ABSCONSION'
state_supervision_period_admission_reason_return_from_suspension = 'RETURN_FROM_SUSPENSION'

state_supervision_period_status_terminated = 'TERMINATED'
state_supervision_period_status_under_supervision = 'UNDER_SUPERVISION'

state_supervision_period_supervision_level_minimum = 'MINIMUM'
state_supervision_period_supervision_level_medium = 'MEDIUM'
state_supervision_period_supervision_level_high = 'HIGH'
state_supervision_period_supervision_level_maximum = 'MAXIMUM'
state_supervision_period_supervision_level_incarcerated = 'INCARCERATED'
state_supervision_period_supervision_level_diversion = 'DIVERSION'
state_supervision_period_supervision_level_interstate_compact = 'INTERSTATE_COMPACT'

state_supervision_period_termination_reason_absconsion = 'ABSCONSION'
state_supervision_period_termination_reason_death = 'DEATH'
state_supervision_period_termination_reason_discharge = 'DISCHARGE'
state_supervision_period_termination_reason_expiration = 'EXPIRATION'
state_supervision_period_termination_reason_transfer_out_of_state = 'TRANSFER_OUT_OF_STATE'
state_supervision_period_termination_reason_transfer_within_state = 'TRANSFER_WITHIN_STATE'
state_supervision_period_termination_reason_return_from_absconsion = 'RETURN_FROM_ABSCONSION'
state_supervision_period_termination_reason_revocation = 'REVOCATION'
state_supervision_period_termination_reason_suspension = 'SUSPENSION'

# state_supervision_case_type_entry.py
state_supervision_case_type_domestic_violence = 'DOMESTIC_VIOLENCE'
state_supervision_case_type_general = 'GENERAL'
state_supervision_case_type_serious_mental_illness = 'SERIOUS_MENTAL_ILLNESS'
state_supervision_case_type_sex_offender = 'SEX_OFFENDER'

# state_supervision_violation.py
state_supervision_violation_type_absconded = 'ABSCONDED'
state_supervision_violation_type_escaped = 'ESCAPED'
state_supervision_violation_type_felony = 'FELONY'
state_supervision_violation_type_misdemeanor = 'MISDEMEANOR'
state_supervision_violation_type_municipal = 'MUNICIPAL'
state_supervision_violation_type_technical = 'TECHNICAL'

# state_supervision_violation_response.py
state_supervision_violation_response_type_citation = 'CITATION'
state_supervision_violation_response_type_violation_report = 'VIOLATION_REPORT'
state_supervision_violation_response_type_permanent_decision = 'PERMANENT_DECISION'

state_supervision_violation_response_decision_continuance = 'CONTINUANCE'
state_supervision_violation_response_decision_delayed_action = 'DELAYED_ACTION'
state_supervision_violation_response_decision_extension = 'EXTENSION'
state_supervision_violation_response_decision_revocation = 'REVOCATION'
state_supervision_violation_response_decision_privileges_revoked = 'PRIVILEGES_REVOKED'
state_supervision_violation_response_decision_service_termination = 'SERVICE_TERMINATION'
state_supervision_violation_response_decision_suspension = 'SUSPENSION'

state_supervision_violation_response_revocation_type_reincarceration = 'REINCARCERATION'
state_supervision_violation_response_revocation_type_return_to_supervision = 'RETURN_TO_SUPERVISION'
state_supervision_violation_response_revocation_type_shock_incarceration = 'SHOCK_INCARCERATION'
state_supervision_violation_response_revocation_type_treatment_in_prison = 'TREATMENT_IN_PRISON'

state_supervision_violation_response_deciding_body_type_court = 'COURT'
state_supervision_violation_response_deciding_body_parole_board = 'PAROLE_BOARD'
state_supervision_violation_response_deciding_body_type_supervision_officer = 'SUPERVISION_OFFICER'

# state_program_assignment.py
state_program_assignment_participation_status_denied = 'DENIED'
state_program_assignment_participation_status_discharged = 'DISCHARGED'
state_program_assignment_participation_status_in_progress = 'IN_PROGRESS'
state_program_assignment_participation_status_pending = 'PENDING'

state_program_assignment_discharge_reason_absconded = 'ABSCONDED'
state_program_assignment_discharge_reason_adverse_termination = 'ADVERSE_TERMINATION'
state_program_assignment_discharge_reason_completed = 'COMPLETED'
state_program_assignment_discharge_reason_moved = 'MOVED'
state_program_assignment_discharge_reason_opted_out = 'OPTED_OUT'
state_program_assignment_discharge_reason_program_transfer = 'PROGRAM_TRANSFER'
state_program_assignment_discharge_reason_reincarcerated = 'REINCARCERATED'

# state_specialized_purpose_for_incarceration
state_specialized_purpose_for_incarceration_shock_incarceration = 'SHOCK_INCARCERATION'
state_specialized_purpose_for_incarceration_treatment_in_prison = 'TREATMENT_IN_PRISON'
