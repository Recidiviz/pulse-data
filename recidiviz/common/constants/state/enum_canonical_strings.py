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
Strings used as underlying representation of enum values for state-specific
application and database code.

NOTE: Changing ANY STRING VALUE in this file will require a database migration.
The Python values pointing to the strings can be renamed without issue.

SQLAlchemy represents SQL enums as strings, and uses the string representation
to pass values to the database. This means any change to the string values
requires a database migration. Therefore in order to keep the code as flexible
as possible, the string values should never be used directly. Storing the
strings in this file and only referring to them by their values here allows
us to structure the application layer code any way we want, while only
requiring the database to be updated when an enum value is created or removed.
"""

# agent.py
agent_judge = 'JUDGE'
agent_parole_board_member = 'PAROLE_BOARD_MEMBER'
agent_supervision_officer = 'SUPERVISION_OFFICER'
agent_unit_supervisor = 'UNIT_SUPERVISOR'

# assessment.py
assessment_class_mental_health = 'MENTAL_HEALTH'
assessment_class_risk = 'RISK'
assessment_class_security_classification = 'SECURITY_CLASSIFICATION'
assessment_class_substance_abuse = 'SUBSTANCE_ABUSE'

assessment_type_asi = 'ASI'
assessment_type_lsir = 'LSIR'
assessment_type_oras = 'ORAS'
assessment_type_psa = 'PSA'

# charge.py
charge_classification_civil = 'CIVIL'
charge_classification_felony = 'FELONY'
charge_classification_infraction = 'INFRACTION'
charge_classification_misdemeanor = 'MISDEMEANOR'
charge_classification_other = 'OTHER'

# court_case.py
# TODO(1697): Add enum strings here

# fine.py
fine_status_paid = 'PAID'
fine_status_unpaid = 'UNPAID'

# incarceration.py
incarceration_type_county_jail = 'COUNTY_JAIL'
incarceration_type_state_prison = 'STATE_PRISON'

# incarceration_incident.py
incarceration_incident_offense_contraband = 'CONTRABAND'
incarceration_incident_offense_violent = 'VIOLENT'

incarceration_incident_outcome_privilege_loss = 'PRIVILEGE_LOSS'
incarceration_incident_outcome_solitary = 'SOLITARY'
incarceration_incident_outcome_warning = 'WARNING'
incarceration_incident_outcome_write_up = 'WRITE_UP'

# incarceration_period.py
incarceration_period_status_in_custody = 'IN_CUSTODY'
incarceration_period_status_not_in_custody = 'NOT_IN_CUSTODY'

incarceration_facility_security_level_maximum = 'MAXIMUM'
incarceration_facility_security_level_medium = 'MEDIUM'
incarceration_facility_security_level_minimum = 'MINIMUM'

incarceration_period_admission_reason_new_admission = 'NEW_ADMISSION'
incarceration_period_admission_reason_parole_revocation = 'PAROLE_REVOCATION'
incarceration_period_admission_reason_probation_revocation = \
    'PROBATION_REVOCATION'
incarceration_period_admission_reason_return_from_escape = 'RETURN_FROM_ESCAPE'
incarceration_period_admission_reason_transfer = 'TRANSFER'

incarceration_period_release_reason_conditional_release = 'CONDITIONAL_RELEASE'
incarceration_period_release_reason_death = 'DEATH'
incarceration_period_release_reason_escape = 'ESCAPE'
incarceration_period_release_reason_sentence_served = 'SENTENCE_SERVED'
incarceration_period_release_reason_transfer = 'TRANSFER'

# sentence.py
sentence_status_commuted = 'COMMUTED'
sentence_status_completed = 'COMPLETED'
sentence_status_serving = 'SERVING'
sentence_status_suspended = 'SUSPENDED'

# supervision.py
supervision_type_conditional = 'CONDITIONAL'
supervision_type_halfway_house = 'HALFWAY_HOUSE'
supervision_type_parole = 'PAROLE'
supervision_type_post_confinement = 'POST_CONFINEMENT'
supervision_type_pre_confinement = 'PRE_CONFINEMENT'
supervision_type_probation = 'PROBATION'

# supervision_period.py
supervision_period_admission_type_conditional_release = 'CONDITIONAL_RELEASE'
supervision_period_admission_type_court_sentence = 'COURT_SENTENCE'
supervision_period_admission_type_return_from_absconsion = \
    'RETURN_FROM_ABSCONSION'
supervision_period_admission_type_return_from_suspension = \
    'RETURN_FROM_SUPSENSION'

supervision_period_status_terminated = 'TERMINATED'
supervision_period_status_under_supervision = 'UNDER_SUPERVISION'

supervision_period_termination_reason_absconsion = 'ABSCONSION'
supervision_period_termination_reason_discharge = 'DISCHARGE'
supervision_period_termination_reason_revocation = 'REVOCATION'
supervision_period_termination_reason_suspension = 'SUSPENSION'

# supervision_violation.py
supervision_violation_type_absconded = 'ABSCONDED'
supervision_violation_type_felony = 'FELONY'
supervision_violation_type_misdemeanor = 'MISDEMEANOR'
supervision_violation_type_municipal = 'MUNICIPAL'
supervision_violation_type_technical = 'TECHNICAL'

# supervision_violation_response.py
supervision_violation_response_type_citation = 'CITATION'
supervision_violation_response_type_violation_report = 'VIOLATION_REPORT'
supervision_violation_response_type_permanent_decision = 'PERMANENT_DECISION'

supervision_violation_response_decision_continuance = 'CONTINUANCE'
supervision_violation_response_decision_extension = 'EXTENSION'
supervision_violation_response_decision_revocation = 'REVOCATION'
supervision_violation_response_decision_suspension = 'SUSPENSION'

supervision_violation_response_revocation_type_shock_incarceration = \
    'SHOCK_INCARCERATION'
supervision_violation_response_revocation_type_standard = 'STANDARD'
supervision_violation_response_revocation_type_treatment_in_prison = \
    'TREATMENT_IN_PRISON'

supervision_violation_response_deciding_body_type_court = 'COURT'
supervision_violation_response_deciding_body_parole_board = 'PAROLE_BOARD'
supervision_violation_response_deciding_body_type_supervision_officer = \
    'SUPERVISION_OFFICER'
