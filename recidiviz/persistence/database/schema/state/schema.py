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
# ============================================================================

"""Define the ORM schema objects that map directly to the database,
for state-level entities.

The below schema uses only generic SQLAlchemy types, and therefore should be
portable between database implementations.
"""
from typing import Any, TypeVar

from sqlalchemy import (
    Boolean,
    Column,
    Date,
    Enum,
    ForeignKey,
    Integer,
    String,
    Table,
    Text,
    UniqueConstraint,
)
from sqlalchemy.ext.declarative import declarative_base, declared_attr
from sqlalchemy.orm import DeclarativeMeta, relationship

from recidiviz.common.constants.state import (
    enum_canonical_strings as state_enum_strings,
)
from recidiviz.persistence.database.database_entity import DatabaseEntity
from recidiviz.utils.string import StrictStringFormatter

# Defines the base class for all table classes in the state schema.
StateBase: DeclarativeMeta = declarative_base(cls=DatabaseEntity, name="StateBase")

ASSOCIATON_TABLE_COMMENT_TEMPLATE = (
    "Association table that connects {first_object_name_plural} with "
    "{second_object_name_plural} by their ids."
)

EXTERNAL_ID_COMMENT_TEMPLATE = (
    "The unique identifier for the {object_name}, unique within the scope of the source "
    "data system."
)

PRIMARY_KEY_COMMENT_TEMPLATE = (
    "Unique identifier for a(n) {object_name}, generated automatically by the "
    "Recidiviz system. This identifier is not stable over time (it may change if "
    "historical data is re-ingested), but should be used within the context of a given "
    "dataset to connect this object to others."
)

FOREIGN_KEY_COMMENT_TEMPLATE = (
    "Unique identifier for a(n) {object_name}, generated automatically by the "
    "Recidiviz system. This identifier is not stable over time (it may change if "
    "historical data is re-ingested), but should be used within the context of a given "
    "dataset to connect this object to relevant {object_name} information."
)

STATE_CODE_COMMENT = "The U.S. state or region that provided the source data."

CUSTODIAL_AUTHORITY_COMMENT = (
    "The type of government entity directly responsible for the person in this period of "
    "incarceration. Not necessarily the decision making authority. For example, the "
    "supervision authority in a state might be the custodial authority for someone on "
    "probation, even though the courts are the body with the power to make decisions about "
    "that person's path through the system."
)

# SQLAlchemy enums. Created separately from the tables so they can be shared
# between tables / columns if necessary.
state_assessment_class = Enum(
    state_enum_strings.state_assessment_class_risk,
    state_enum_strings.state_assessment_class_sex_offense,
    state_enum_strings.state_assessment_class_social,
    state_enum_strings.state_assessment_class_substance_abuse,
    state_enum_strings.internal_unknown,
    state_enum_strings.external_unknown,
    name="state_assessment_class",
)

state_assessment_type = Enum(
    state_enum_strings.state_assessment_type_cssm,
    state_enum_strings.state_assessment_type_hiq,
    state_enum_strings.state_assessment_type_j_soap,
    state_enum_strings.state_assessment_type_lsir,
    state_enum_strings.state_assessment_type_odara,
    state_enum_strings.state_assessment_type_oyas,
    state_enum_strings.state_assessment_type_pa_rst,
    state_enum_strings.state_assessment_type_psa,
    state_enum_strings.state_assessment_type_sorac,
    state_enum_strings.state_assessment_type_sotips,
    state_enum_strings.state_assessment_type_spin_w,
    state_enum_strings.state_assessment_type_stable,
    state_enum_strings.state_assessment_type_static_99,
    state_enum_strings.state_assessment_type_strong_r,
    state_enum_strings.state_assessment_type_tcu_drug_screen,
    state_enum_strings.state_assessment_type_oras_community_supervision,
    state_enum_strings.state_assessment_type_oras_community_supervision_screening,
    state_enum_strings.state_assessment_type_oras_misdemeanor_assessment,
    state_enum_strings.state_assessment_type_oras_misdemeanor_screening,
    state_enum_strings.state_assessment_type_oras_pre_trial,
    state_enum_strings.state_assessment_type_oras_prison_screening,
    state_enum_strings.state_assessment_type_oras_prison_intake,
    state_enum_strings.state_assessment_type_oras_reentry,
    state_enum_strings.state_assessment_type_oras_supplemental_reentry,
    state_enum_strings.internal_unknown,
    state_enum_strings.external_unknown,
    name="state_assessment_type",
)

state_assessment_level = Enum(
    state_enum_strings.state_assessment_level_minimum,
    state_enum_strings.state_assessment_level_low,
    state_enum_strings.state_assessment_level_low_medium,
    state_enum_strings.state_assessment_level_medium,
    state_enum_strings.state_assessment_level_medium_high,
    state_enum_strings.state_assessment_level_moderate,
    state_enum_strings.state_assessment_level_high,
    state_enum_strings.state_assessment_level_very_high,
    state_enum_strings.state_assessment_level_maximum,
    state_enum_strings.internal_unknown,
    state_enum_strings.external_unknown,
    name="state_assessment_level",
)

state_sentence_status = Enum(
    state_enum_strings.state_sentence_status_commuted,
    state_enum_strings.state_sentence_status_completed,
    state_enum_strings.state_sentence_status_pardoned,
    state_enum_strings.state_sentence_status_pending,
    state_enum_strings.state_sentence_status_sanctioned,
    state_enum_strings.state_sentence_status_serving,
    state_enum_strings.state_sentence_status_suspended,
    state_enum_strings.state_sentence_status_revoked,
    state_enum_strings.state_sentence_status_vacated,
    state_enum_strings.present_without_info,
    state_enum_strings.internal_unknown,
    state_enum_strings.external_unknown,
    name="state_sentence_status",
)

state_supervision_sentence_supervision_type = Enum(
    state_enum_strings.state_supervision_sentence_supervision_type_community_corrections,
    state_enum_strings.state_supervision_sentence_supervision_type_parole,
    state_enum_strings.state_supervision_sentence_supervision_type_probation,
    state_enum_strings.internal_unknown,
    state_enum_strings.external_unknown,
    name="state_supervision_sentence_supervision_type",
)

state_supervision_case_type = Enum(
    state_enum_strings.state_supervision_case_type_domestic_violence,
    state_enum_strings.state_supervision_case_type_drug_court,
    state_enum_strings.state_supervision_case_type_family_court,
    state_enum_strings.state_supervision_case_type_general,
    state_enum_strings.state_supervision_case_type_mental_health_court,
    state_enum_strings.state_supervision_case_type_serious_mental_illness,
    state_enum_strings.state_supervision_case_type_sex_offense,
    state_enum_strings.state_supervision_case_type_veterans_court,
    state_enum_strings.internal_unknown,
    state_enum_strings.external_unknown,
    name="state_supervision_case_type",
)

state_charge_classification_type = Enum(
    state_enum_strings.state_charge_classification_type_civil,
    state_enum_strings.state_charge_classification_type_felony,
    state_enum_strings.state_charge_classification_type_infraction,
    state_enum_strings.state_charge_classification_type_misdemeanor,
    state_enum_strings.internal_unknown,
    state_enum_strings.external_unknown,
    name="state_charge_classification_type",
)

state_charge_status = Enum(
    state_enum_strings.state_charge_status_acquitted,
    state_enum_strings.state_charge_status_adjudicated,
    state_enum_strings.state_charge_status_convicted,
    state_enum_strings.state_charge_status_dropped,
    state_enum_strings.state_charge_status_pending,
    state_enum_strings.state_charge_status_sentenced,
    state_enum_strings.state_charge_status_transferred_away,
    state_enum_strings.internal_unknown,
    state_enum_strings.external_unknown,
    state_enum_strings.present_without_info,
    name="state_charge_status",
)

state_incarceration_type = Enum(
    state_enum_strings.state_incarceration_type_county_jail,
    state_enum_strings.state_incarceration_type_federal_prison,
    state_enum_strings.state_incarceration_type_out_of_state,
    state_enum_strings.state_incarceration_type_state_prison,
    state_enum_strings.internal_unknown,
    state_enum_strings.external_unknown,
    name="state_incarceration_type",
)

state_court_case_status = Enum(
    state_enum_strings.internal_unknown,
    state_enum_strings.external_unknown,
    state_enum_strings.present_without_info,
    name="state_court_case_status",
)

state_court_type = Enum(
    state_enum_strings.present_without_info,
    state_enum_strings.internal_unknown,
    state_enum_strings.external_unknown,
    name="state_court_type",
)

state_agent_type = Enum(
    state_enum_strings.present_without_info,
    state_enum_strings.state_agent_correctional_officer,
    state_enum_strings.state_agent_judge,
    state_enum_strings.state_agent_justice,
    state_enum_strings.state_agent_supervision_officer,
    state_enum_strings.internal_unknown,
    state_enum_strings.external_unknown,
    name="state_agent_type",
)

state_person_alias_type = Enum(
    state_enum_strings.state_person_alias_alias_type_affiliation_name,
    state_enum_strings.state_person_alias_alias_type_alias,
    state_enum_strings.state_person_alias_alias_type_given_name,
    state_enum_strings.state_person_alias_alias_type_maiden_name,
    state_enum_strings.state_person_alias_alias_type_nickname,
    state_enum_strings.internal_unknown,
    state_enum_strings.external_unknown,
    name="state_person_alias_type",
)

state_gender = Enum(
    state_enum_strings.state_gender_female,
    state_enum_strings.state_gender_male,
    state_enum_strings.state_gender_non_binary,
    state_enum_strings.state_gender_trans,
    state_enum_strings.state_gender_trans_female,
    state_enum_strings.state_gender_trans_male,
    state_enum_strings.internal_unknown,
    state_enum_strings.external_unknown,
    name="state_gender",
)

state_race = Enum(
    state_enum_strings.state_race_american_indian,
    state_enum_strings.state_race_asian,
    state_enum_strings.state_race_black,
    state_enum_strings.state_race_hawaiian,
    state_enum_strings.state_race_other,
    state_enum_strings.state_race_white,
    state_enum_strings.internal_unknown,
    state_enum_strings.external_unknown,
    name="state_race",
)

state_ethnicity = Enum(
    state_enum_strings.state_ethnicity_hispanic,
    state_enum_strings.state_ethnicity_not_hispanic,
    state_enum_strings.internal_unknown,
    state_enum_strings.external_unknown,
    name="state_ethnicity",
)

state_residency_status = Enum(
    state_enum_strings.state_residency_status_homeless,
    state_enum_strings.state_residency_status_permanent,
    state_enum_strings.state_residency_status_transient,
    state_enum_strings.internal_unknown,
    state_enum_strings.external_unknown,
    name="state_residency_status",
)

state_incarceration_period_admission_reason = Enum(
    state_enum_strings.state_incarceration_period_admission_reason_admitted_in_error,
    state_enum_strings.state_incarceration_period_admission_reason_admitted_from_supervision,
    state_enum_strings.state_incarceration_period_admission_reason_new_admission,
    state_enum_strings.state_incarceration_period_admission_reason_revocation,
    state_enum_strings.state_incarceration_period_admission_reason_sanction_admission,
    state_enum_strings.state_incarceration_period_admission_reason_return_from_erroneous_release,
    state_enum_strings.state_incarceration_period_admission_reason_return_from_temporary_release,
    state_enum_strings.state_incarceration_period_admission_reason_return_from_escape,
    state_enum_strings.state_incarceration_period_admission_reason_temporary_custody,
    state_enum_strings.state_incarceration_period_admission_reason_transfer,
    state_enum_strings.state_incarceration_period_admission_reason_transfer_from_other_jurisdiction,
    state_enum_strings.state_incarceration_period_admission_reason_status_change,
    state_enum_strings.internal_unknown,
    state_enum_strings.external_unknown,
    name="state_incarceration_period_admission_reason",
)

state_incarceration_period_release_reason = Enum(
    state_enum_strings.state_incarceration_period_release_reason_commuted,
    state_enum_strings.state_incarceration_period_release_reason_compassionate,
    state_enum_strings.state_incarceration_period_release_reason_conditional_release,
    state_enum_strings.state_incarceration_period_release_reason_court_order,
    state_enum_strings.state_incarceration_period_release_reason_death,
    state_enum_strings.state_incarceration_period_release_reason_escape,
    state_enum_strings.state_incarceration_period_release_reason_execution,
    state_enum_strings.state_incarceration_period_release_reason_pardoned,
    state_enum_strings.state_incarceration_period_release_reason_released_from_erroneous_admission,
    state_enum_strings.state_incarceration_period_release_reason_released_from_temporary_custody,
    state_enum_strings.state_incarceration_period_release_reason_released_in_error,
    state_enum_strings.state_incarceration_period_release_reason_released_to_supervision,
    state_enum_strings.state_incarceration_period_release_reason_sentence_served,
    state_enum_strings.state_incarceration_period_release_reason_temporary_release,
    state_enum_strings.state_incarceration_period_release_reason_transfer,
    state_enum_strings.state_incarceration_period_release_reason_transfer_to_other_jurisdiction,
    state_enum_strings.state_incarceration_period_release_reason_vacated,
    state_enum_strings.state_incarceration_period_release_reason_status_change,
    state_enum_strings.internal_unknown,
    state_enum_strings.external_unknown,
    name="state_incarceration_period_release_reason",
)

state_supervision_period_admission_reason = Enum(
    state_enum_strings.state_supervision_period_admission_reason_absconsion,
    state_enum_strings.state_supervision_period_admission_reason_conditional_release,
    state_enum_strings.state_supervision_period_admission_reason_court_sentence,
    state_enum_strings.state_supervision_period_admission_reason_investigation,
    state_enum_strings.state_supervision_period_admission_reason_transfer_from_other_jurisdiction,
    state_enum_strings.state_supervision_period_admission_reason_transfer_within_state,
    state_enum_strings.state_supervision_period_admission_reason_return_from_absconsion,
    state_enum_strings.state_supervision_period_admission_reason_return_from_suspension,
    state_enum_strings.internal_unknown,
    state_enum_strings.external_unknown,
    name="state_supervision_period_admission_reason",
)

state_supervision_level = Enum(
    state_enum_strings.state_supervision_period_supervision_level_minimum,
    state_enum_strings.state_supervision_period_supervision_level_medium,
    state_enum_strings.state_supervision_period_supervision_level_high,
    state_enum_strings.state_supervision_period_supervision_level_maximum,
    state_enum_strings.state_supervision_period_supervision_level_incarcerated,
    state_enum_strings.state_supervision_period_supervision_level_in_custody,
    state_enum_strings.state_supervision_period_supervision_level_diversion,
    state_enum_strings.state_supervision_period_supervision_level_interstate_compact,
    state_enum_strings.state_supervision_period_supervision_level_limited,
    state_enum_strings.state_supervision_period_supervision_level_electronic_monitoring_only,
    state_enum_strings.state_supervision_period_supervision_level_unsupervised,
    state_enum_strings.state_supervision_period_supervision_level_unassigned,
    state_enum_strings.internal_unknown,
    state_enum_strings.external_unknown,
    state_enum_strings.present_without_info,
    name="state_supervision_level",
)

state_supervision_period_termination_reason = Enum(
    state_enum_strings.state_supervision_period_termination_reason_absconsion,
    state_enum_strings.state_supervision_period_termination_reason_commuted,
    state_enum_strings.state_supervision_period_termination_reason_death,
    state_enum_strings.state_supervision_period_termination_reason_discharge,
    state_enum_strings.state_supervision_period_termination_reason_dismissed,
    state_enum_strings.state_supervision_period_termination_reason_expiration,
    state_enum_strings.state_supervision_period_termination_reason_investigation,
    state_enum_strings.state_supervision_period_termination_reason_pardoned,
    state_enum_strings.state_supervision_period_termination_reason_transfer_to_other_jurisdiction,
    state_enum_strings.state_supervision_period_termination_reason_transfer_within_state,
    state_enum_strings.state_supervision_period_termination_reason_return_from_absconsion,
    state_enum_strings.state_supervision_period_termination_reason_return_to_incarceration,
    state_enum_strings.state_supervision_period_termination_reason_revocation,
    state_enum_strings.state_supervision_period_termination_reason_suspension,
    state_enum_strings.state_supervision_period_termination_reason_vacated,
    state_enum_strings.internal_unknown,
    state_enum_strings.external_unknown,
    name="state_supervision_period_termination_reason",
)

state_supervision_period_supervision_type = Enum(
    state_enum_strings.state_supervision_period_supervision_type_absconsion,
    state_enum_strings.state_supervision_period_supervision_type_informal_probation,
    state_enum_strings.state_supervision_period_supervision_type_investigation,
    state_enum_strings.state_supervision_period_supervision_type_parole,
    state_enum_strings.state_supervision_period_supervision_type_probation,
    state_enum_strings.state_supervision_period_supervision_type_dual,
    state_enum_strings.state_supervision_period_supervision_type_community_confinement,
    state_enum_strings.state_supervision_period_supervision_type_bench_warrant,
    state_enum_strings.internal_unknown,
    state_enum_strings.external_unknown,
    name="state_supervision_period_supervision_type",
)

state_incarceration_incident_type = Enum(
    state_enum_strings.present_without_info,
    state_enum_strings.state_incarceration_incident_type_contraband,
    state_enum_strings.state_incarceration_incident_type_disorderly_conduct,
    state_enum_strings.state_incarceration_incident_type_escape,
    state_enum_strings.state_incarceration_incident_type_minor_offense,
    state_enum_strings.state_incarceration_incident_type_positive,
    state_enum_strings.state_incarceration_incident_type_report,
    state_enum_strings.state_incarceration_incident_type_violence,
    state_enum_strings.internal_unknown,
    state_enum_strings.external_unknown,
    name="state_incarceration_incident_type",
)

state_incarceration_incident_outcome_type = Enum(
    state_enum_strings.state_incarceration_incident_outcome_cell_confinement,
    state_enum_strings.state_incarceration_incident_outcome_disciplinary_labor,
    state_enum_strings.state_incarceration_incident_outcome_dismissed,
    state_enum_strings.state_incarceration_incident_outcome_external_prosecution,
    state_enum_strings.state_incarceration_incident_outcome_financial_penalty,
    state_enum_strings.state_incarceration_incident_outcome_good_time_loss,
    state_enum_strings.state_incarceration_incident_outcome_not_guilty,
    state_enum_strings.state_incarceration_incident_outcome_privilege_loss,
    state_enum_strings.state_incarceration_incident_outcome_restricted_confinement,
    state_enum_strings.state_incarceration_incident_outcome_solitary,
    state_enum_strings.state_incarceration_incident_outcome_treatment,
    state_enum_strings.state_incarceration_incident_outcome_warning,
    state_enum_strings.internal_unknown,
    state_enum_strings.external_unknown,
    name="state_incarceration_incident_outcome_type",
)

state_supervision_violation_type = Enum(
    state_enum_strings.state_supervision_violation_type_absconded,
    state_enum_strings.state_supervision_violation_type_escaped,
    state_enum_strings.state_supervision_violation_type_felony,
    state_enum_strings.state_supervision_violation_type_law,
    state_enum_strings.state_supervision_violation_type_misdemeanor,
    state_enum_strings.state_supervision_violation_type_municipal,
    state_enum_strings.state_supervision_violation_type_technical,
    state_enum_strings.internal_unknown,
    state_enum_strings.external_unknown,
    name="state_supervision_violation_type",
)

state_supervision_violation_response_type = Enum(
    state_enum_strings.state_supervision_violation_response_type_citation,
    state_enum_strings.state_supervision_violation_response_type_violation_report,
    state_enum_strings.state_supervision_violation_response_type_permanent_decision,
    state_enum_strings.internal_unknown,
    state_enum_strings.external_unknown,
    name="state_supervision_violation_response_type",
)

state_supervision_violation_response_decision = Enum(
    state_enum_strings.state_supervision_violation_response_decision_community_service,
    state_enum_strings.state_supervision_violation_response_decision_continuance,
    state_enum_strings.state_supervision_violation_response_decision_delayed_action,
    state_enum_strings.state_supervision_violation_response_decision_extension,
    state_enum_strings.state_supervision_violation_response_decision_new_conditions,
    state_enum_strings.state_supervision_violation_response_decision_other,
    state_enum_strings.state_supervision_violation_response_decision_revocation,
    state_enum_strings.state_supervision_violation_response_decision_privileges_revoked,
    state_enum_strings.state_supervision_violation_response_decision_service_termination,
    state_enum_strings.state_supervision_violation_response_decision_shock_incarceration,
    state_enum_strings.state_supervision_violation_response_decision_specialized_court,
    state_enum_strings.state_supervision_violation_response_decision_suspension,
    state_enum_strings.state_supervision_violation_response_decision_treatment_in_prison,
    state_enum_strings.state_supervision_violation_response_decision_treatment_in_field,
    state_enum_strings.state_supervision_violation_response_decision_violation_unfounded,
    state_enum_strings.state_supervision_violation_response_decision_warning,
    state_enum_strings.state_supervision_violation_response_decision_warrant_issued,
    state_enum_strings.internal_unknown,
    state_enum_strings.external_unknown,
    name="state_supervision_violation_response_decision",
)

state_supervision_violation_response_deciding_body_type = Enum(
    state_enum_strings.state_supervision_violation_response_deciding_body_type_court,
    state_enum_strings.state_supervision_violation_response_deciding_body_parole_board,
    state_enum_strings.state_supervision_violation_response_deciding_body_type_supervision_officer,
    state_enum_strings.internal_unknown,
    state_enum_strings.external_unknown,
    name="state_supervision_violation_response_deciding_body_type",
)

state_program_assignment_participation_status = Enum(
    state_enum_strings.present_without_info,
    state_enum_strings.state_program_assignment_participation_status_denied,
    state_enum_strings.state_program_assignment_participation_status_discharged,
    state_enum_strings.state_program_assignment_participation_status_in_progress,
    state_enum_strings.state_program_assignment_participation_status_pending,
    state_enum_strings.state_program_assignment_participation_status_refused,
    state_enum_strings.internal_unknown,
    state_enum_strings.external_unknown,
    name="state_program_assignment_participation_status",
)

state_specialized_purpose_for_incarceration = Enum(
    state_enum_strings.state_specialized_purpose_for_incarceration_general,
    state_enum_strings.state_specialized_purpose_for_incarceration_parole_board_hold,
    state_enum_strings.state_specialized_purpose_for_incarceration_shock_incarceration,
    state_enum_strings.state_specialized_purpose_for_incarceration_treatment_in_prison,
    state_enum_strings.state_specialized_purpose_for_incarceration_temporary_custody,
    state_enum_strings.state_specialized_purpose_for_incarceration_weekend_confinement,
    state_enum_strings.internal_unknown,
    state_enum_strings.external_unknown,
    name="state_specialized_purpose_for_incarceration",
)

state_early_discharge_decision = Enum(
    state_enum_strings.state_early_discharge_decision_request_denied,
    state_enum_strings.state_early_discharge_decision_sentence_termination_granted,
    state_enum_strings.state_early_discharge_decision_unsupervised_probation_granted,
    state_enum_strings.internal_unknown,
    state_enum_strings.external_unknown,
    name="state_early_discharge_decision",
)

state_early_discharge_decision_status = Enum(
    state_enum_strings.state_early_discharge_decision_status_pending,
    state_enum_strings.state_early_discharge_decision_status_decided,
    state_enum_strings.state_early_discharge_decision_status_invalid,
    state_enum_strings.internal_unknown,
    state_enum_strings.external_unknown,
    name="state_early_discharge_decision_status",
)

state_acting_body_type = Enum(
    state_enum_strings.state_acting_body_type_court,
    state_enum_strings.state_acting_body_type_parole_board,
    state_enum_strings.state_acting_body_type_sentenced_person,
    state_enum_strings.state_acting_body_type_supervision_officer,
    state_enum_strings.internal_unknown,
    state_enum_strings.external_unknown,
    name="state_acting_body_type",
)

state_custodial_authority = Enum(
    state_enum_strings.state_custodial_authority_court,
    state_enum_strings.state_custodial_authority_federal,
    state_enum_strings.state_custodial_authority_other_country,
    state_enum_strings.state_custodial_authority_other_state,
    state_enum_strings.state_custodial_authority_supervision_authority,
    state_enum_strings.state_custodial_authority_state_prison,
    state_enum_strings.internal_unknown,
    state_enum_strings.external_unknown,
    name="state_custodial_authority",
)

state_supervision_contact_location = Enum(
    state_enum_strings.state_supervision_contact_location_court,
    state_enum_strings.state_supervision_contact_location_field,
    state_enum_strings.state_supervision_contact_location_jail,
    state_enum_strings.state_supervision_contact_location_place_of_employment,
    state_enum_strings.state_supervision_contact_location_residence,
    state_enum_strings.state_supervision_contact_location_supervision_office,
    state_enum_strings.state_supervision_contact_location_treatment_provider,
    state_enum_strings.state_supervision_contact_location_law_enforcement_agency,
    state_enum_strings.state_supervision_contact_location_parole_commission,
    state_enum_strings.state_supervision_contact_location_alternative_work_site,
    state_enum_strings.internal_unknown,
    state_enum_strings.external_unknown,
    name="state_supervision_contact_location",
)

state_supervision_contact_status = Enum(
    state_enum_strings.state_supervision_contact_status_attempted,
    state_enum_strings.state_supervision_contact_status_completed,
    state_enum_strings.internal_unknown,
    state_enum_strings.external_unknown,
    name="state_supervision_contact_status",
)

state_supervision_contact_reason = Enum(
    state_enum_strings.state_supervision_contact_reason_emergency_contact,
    state_enum_strings.state_supervision_contact_reason_general_contact,
    state_enum_strings.state_supervision_contact_reason_initial_contact,
    state_enum_strings.internal_unknown,
    state_enum_strings.external_unknown,
    name="state_supervision_contact_reason",
)

state_supervision_contact_type = Enum(
    state_enum_strings.state_supervision_contact_type_collateral,
    state_enum_strings.state_supervision_contact_type_direct,
    state_enum_strings.state_supervision_contact_type_both_collateral_and_direct,
    state_enum_strings.internal_unknown,
    state_enum_strings.external_unknown,
    name="state_supervision_contact_type",
)

state_supervision_contact_method = Enum(
    state_enum_strings.state_supervision_contact_method_in_person,
    state_enum_strings.state_supervision_contact_method_telephone,
    state_enum_strings.state_supervision_contact_method_virtual,
    state_enum_strings.state_supervision_contact_method_written_message,
    state_enum_strings.internal_unknown,
    state_enum_strings.external_unknown,
    name="state_supervision_contact_method",
)


state_employment_period_employment_status = Enum(
    state_enum_strings.state_employment_period_employment_status_alternate_income_source,
    state_enum_strings.state_employment_period_employment_status_employed,
    state_enum_strings.state_employment_period_employment_status_employed_full_time,
    state_enum_strings.state_employment_period_employment_status_employed_part_time,
    state_enum_strings.state_employment_period_employment_status_student,
    state_enum_strings.state_employment_period_employment_status_unable_to_work,
    state_enum_strings.state_employment_period_employment_status_unemployed,
    state_enum_strings.internal_unknown,
    state_enum_strings.external_unknown,
    name="state_employment_period_employment_status",
)

state_employment_period_end_reason = Enum(
    state_enum_strings.state_employment_period_end_reason_employment_status_change,
    state_enum_strings.state_employment_period_end_reason_fired,
    state_enum_strings.state_employment_period_end_reason_incarcerated,
    state_enum_strings.state_employment_period_end_reason_laid_off,
    state_enum_strings.state_employment_period_end_reason_medical,
    state_enum_strings.state_employment_period_end_reason_moved,
    state_enum_strings.state_employment_period_end_reason_new_job,
    state_enum_strings.state_employment_period_end_reason_quit,
    state_enum_strings.state_employment_period_end_reason_retired,
    state_enum_strings.internal_unknown,
    state_enum_strings.external_unknown,
    name="state_employment_period_end_reason",
)

state_drug_screen_result = Enum(
    state_enum_strings.state_drug_screen_result_positive,
    state_enum_strings.state_drug_screen_result_negative,
    state_enum_strings.state_drug_screen_result_admitted_positive,
    state_enum_strings.state_drug_screen_result_medical_exemption,
    state_enum_strings.state_drug_screen_result_no_result,
    state_enum_strings.internal_unknown,
    state_enum_strings.external_unknown,
    name="state_drug_screen_result",
)

state_drug_screen_sample_type = Enum(
    state_enum_strings.state_drug_screen_sample_type_urine,
    state_enum_strings.state_drug_screen_sample_type_sweat,
    state_enum_strings.state_drug_screen_sample_type_saliva,
    state_enum_strings.state_drug_screen_sample_type_blood,
    state_enum_strings.state_drug_screen_sample_type_hair,
    state_enum_strings.state_drug_screen_sample_type_breath,
    state_enum_strings.state_drug_screen_sample_type_no_sample,
    state_enum_strings.internal_unknown,
    state_enum_strings.external_unknown,
    name="state_drug_screen_sample_type",
)

# Join tables
state_charge_incarceration_sentence_association_table = Table(
    "state_charge_incarceration_sentence_association",
    StateBase.metadata,
    Column(
        "charge_id",
        Integer,
        ForeignKey("state_charge.charge_id"),
        index=True,
        comment=StrictStringFormatter().format(
            FOREIGN_KEY_COMMENT_TEMPLATE, object_name="charge"
        ),
    ),
    Column(
        "incarceration_sentence_id",
        Integer,
        ForeignKey("state_incarceration_sentence.incarceration_sentence_id"),
        index=True,
        comment=StrictStringFormatter().format(
            FOREIGN_KEY_COMMENT_TEMPLATE, object_name="incarceration sentence"
        ),
    ),
    comment=StrictStringFormatter().format(
        ASSOCIATON_TABLE_COMMENT_TEMPLATE,
        first_object_name_plural="charges",
        second_object_name_plural="incarceration sentences",
    ),
)

state_charge_supervision_sentence_association_table = Table(
    "state_charge_supervision_sentence_association",
    StateBase.metadata,
    Column(
        "charge_id",
        Integer,
        ForeignKey("state_charge.charge_id"),
        index=True,
        comment=StrictStringFormatter().format(
            FOREIGN_KEY_COMMENT_TEMPLATE, object_name="charge"
        ),
    ),
    Column(
        "supervision_sentence_id",
        Integer,
        ForeignKey("state_supervision_sentence.supervision_sentence_id"),
        index=True,
        comment=StrictStringFormatter().format(
            FOREIGN_KEY_COMMENT_TEMPLATE, object_name="supervision sentence"
        ),
    ),
    comment=StrictStringFormatter().format(
        ASSOCIATON_TABLE_COMMENT_TEMPLATE,
        first_object_name_plural="charges",
        second_object_name_plural="supervision sentences",
    ),
)

state_supervision_violation_response_decision_agent_association_table = Table(
    "state_supervision_violation_response_decision_agent_association",
    StateBase.metadata,
    Column(
        "supervision_violation_response_id",
        Integer,
        ForeignKey(
            "state_supervision_violation_response.supervision_violation_response_id"
        ),
        index=True,
        comment=StrictStringFormatter().format(
            FOREIGN_KEY_COMMENT_TEMPLATE, object_name="violation response"
        ),
    ),
    Column(
        "agent_id",
        Integer,
        ForeignKey("state_agent.agent_id"),
        index=True,
        comment=StrictStringFormatter().format(
            FOREIGN_KEY_COMMENT_TEMPLATE, object_name="agent"
        ),
    ),
    comment=StrictStringFormatter().format(
        ASSOCIATON_TABLE_COMMENT_TEMPLATE,
        first_object_name_plural="supervision violation responses",
        second_object_name_plural="agents",
    ),
)

SchemaPeriodType = TypeVar(
    "SchemaPeriodType", "StateSupervisionPeriod", "StateIncarcerationPeriod"
)
SchemaSentenceType = TypeVar(
    "SchemaSentenceType", "StateSupervisionSentence", "StateIncarcerationSentence"
)


# Shared mixin columns
class _ReferencesStatePersonSharedColumns:
    """A mixin which defines columns for any table whose rows reference an
    individual StatePerson"""

    # Consider this class a mixin and only allow instantiating subclasses
    def __new__(cls, *_: Any, **__: Any) -> "_ReferencesStatePersonSharedColumns":
        if cls is _ReferencesStatePersonSharedColumns:
            raise Exception(f"[{cls}] cannot be instantiated")
        return super().__new__(cls)  # type: ignore

    @declared_attr
    def person_id(self) -> Column:
        return Column(
            Integer,
            ForeignKey("state_person.person_id", deferrable=True, initially="DEFERRED"),
            index=True,
            nullable=False,
            comment=StrictStringFormatter().format(
                FOREIGN_KEY_COMMENT_TEMPLATE, object_name="person"
            ),
        )


class StatePersonExternalId(StateBase, _ReferencesStatePersonSharedColumns):
    """Represents a StatePersonExternalId in the SQL schema"""

    __tablename__ = "state_person_external_id"
    __table_args__ = (
        UniqueConstraint(
            "state_code",
            "id_type",
            "external_id",
            name="person_external_ids_unique_within_type_and_region",
            deferrable=True,
            initially="DEFERRED",
        ),
        {
            "comment": "Each StatePersonExternalId holds a single external id provided by the source data system being "
            "ingested. An external id is a unique identifier for an individual, unique within the scope of "
            "the source data system. We include information denoting the source of the id to make this into "
            "a globally unique identifier."
        },
    )

    person_external_id_id = Column(
        Integer,
        primary_key=True,
        comment=StrictStringFormatter().format(
            PRIMARY_KEY_COMMENT_TEMPLATE, object_name="person external id"
        ),
    )

    external_id = Column(
        String(255),
        nullable=False,
        index=True,
        comment=StrictStringFormatter().format(
            EXTERNAL_ID_COMMENT_TEMPLATE, object_name="StatePersonExternalId"
        ),
    )
    state_code = Column(
        String(255),
        nullable=False,
        index=True,
        comment=STATE_CODE_COMMENT,
    )
    id_type = Column(
        String(255),
        nullable=False,
        comment="The type of id provided by the system. For example, in a "
        "state with multiple data systems that we ingest, this may "
        "be the name of the system from the id emanates.",
    )


class StatePersonAlias(StateBase, _ReferencesStatePersonSharedColumns):
    """Represents a StatePersonAlias in the SQL schema"""

    __tablename__ = "state_person_alias"
    __table_args__ = {
        "comment": "Each StatePersonAlias holds the naming information for an alias for a particular "
        "person. Because a given name is an alias of sorts, we copy over the name fields "
        "provided on the StatePerson object into a child StatePersonAlias object. An alias "
        "is structured similarly to a name, with various different fields, and not a "
        "raw string -- systems storing aliases are raw strings should provide those in "
        "the full_name field below."
    }
    person_alias_id = Column(
        Integer,
        primary_key=True,
        comment=StrictStringFormatter().format(
            PRIMARY_KEY_COMMENT_TEMPLATE, object_name="person"
        ),
    )

    state_code = Column(
        String(255),
        nullable=False,
        index=True,
        comment=STATE_CODE_COMMENT,
    )
    full_name = Column(String(255), comment="A person’s name.")
    alias_type = Column(state_person_alias_type, comment="The type of the name alias.")
    alias_type_raw_text = Column(
        String(255), comment="The raw text value for the alias type."
    )


class StatePersonRace(StateBase, _ReferencesStatePersonSharedColumns):
    """Represents a StatePersonRace in the SQL schema"""

    __tablename__ = "state_person_race"
    __table_args__ = {
        "comment": "Each StatePersonRace holds a single reported race for a single person. A "
        "StatePerson may have multiple StatePersonRace objects because they may be "
        "multi-racial, or because different data sources may report different races."
    }

    person_race_id = Column(
        Integer,
        primary_key=True,
        comment=StrictStringFormatter().format(
            PRIMARY_KEY_COMMENT_TEMPLATE, object_name="person race"
        ),
    )

    state_code = Column(
        String(255),
        nullable=False,
        index=True,
        comment=STATE_CODE_COMMENT,
    )
    race = Column(state_race, comment="A person’s reported race.")
    race_raw_text = Column(
        String(255), comment="The raw text value of the person's race."
    )


class StatePersonEthnicity(StateBase, _ReferencesStatePersonSharedColumns):
    """Represents a state person in the SQL schema"""

    __tablename__ = "state_person_ethnicity"
    __table_args__ = {
        "comment": "Each StatePersonEthnicity holds a single reported ethnicity for a single person. "
        "A StatePerson may have multiple StatePersonEthnicity objects, because they may be"
        " multi-ethnic, or because different data sources may report different ethnicities."
    }

    person_ethnicity_id = Column(
        Integer,
        primary_key=True,
        comment=StrictStringFormatter().format(
            PRIMARY_KEY_COMMENT_TEMPLATE, object_name="person ethnicity"
        ),
    )

    state_code = Column(
        String(255),
        nullable=False,
        index=True,
        comment=STATE_CODE_COMMENT,
    )
    ethnicity = Column(state_ethnicity, comment="A person’s reported ethnicity.")
    ethnicity_raw_text = Column(
        String(255), comment="The raw text value of the ethnicity."
    )


class StatePerson(StateBase):
    """Represents a StatePerson in the state SQL schema"""

    __tablename__ = "state_person"
    __table_args__ = {
        "comment": "Each StatePerson holds details about the individual, as well as lists of several "
        "child entities. Some of these child entities are extensions of individual details,"
        " e.g. Race is its own entity as opposed to a single field, to allow for the"
        " inclusion/tracking of multiple such entities or sources of such information."
    }
    person_id = Column(
        Integer,
        primary_key=True,
        comment=StrictStringFormatter().format(
            PRIMARY_KEY_COMMENT_TEMPLATE, object_name="person"
        ),
    )

    state_code = Column(
        String(255),
        nullable=False,
        index=True,
        comment=STATE_CODE_COMMENT,
    )

    current_address = Column(Text, comment="The current address of the person.")

    full_name = Column(
        String(255),
        index=True,
        comment="A person’s name. Only use this when names are in a "
        "single field. Use surname and given_names when they are "
        "separate.",
    )

    birthdate = Column(
        Date,
        index=True,
        comment="Date the person was born. Use this when it is known. When a "
        "person’s age but not birthdate is reported, use age instead.",
    )

    gender = Column(
        state_gender, comment="A person’s gender, as reported by the state."
    )
    gender_raw_text = Column(
        String(255), comment="The raw text value of the person's state-reported gender."
    )

    residency_status = Column(
        state_residency_status, comment="A person's reported residency status."
    )
    residency_status_raw_text = Column(
        String(255),
        comment="The raw text used to derive a person's reported residency status.",
    )

    @declared_attr
    def supervising_officer_id(self) -> Column:
        return Column(
            Integer,
            ForeignKey("state_agent.agent_id"),
            index=True,
            nullable=True,
            comment=StrictStringFormatter().format(
                FOREIGN_KEY_COMMENT_TEMPLATE, object_name="state agent"
            ),
        )

    external_ids = relationship(
        "StatePersonExternalId", backref="person", lazy="selectin"
    )
    aliases = relationship("StatePersonAlias", backref="person", lazy="selectin")
    races = relationship("StatePersonRace", backref="person", lazy="selectin")
    ethnicities = relationship(
        "StatePersonEthnicity", backref="person", lazy="selectin"
    )
    assessments = relationship("StateAssessment", backref="person", lazy="selectin")
    incarceration_sentences = relationship(
        "StateIncarcerationSentence", backref="person", lazy="selectin"
    )
    supervision_sentences = relationship(
        "StateSupervisionSentence", backref="person", lazy="selectin"
    )
    incarceration_periods = relationship(
        "StateIncarcerationPeriod", backref="person", lazy="selectin"
    )
    supervision_periods = relationship(
        "StateSupervisionPeriod", backref="person", lazy="selectin"
    )
    program_assignments = relationship(
        "StateProgramAssignment", backref="person", lazy="selectin"
    )
    incarceration_incidents = relationship(
        "StateIncarcerationIncident", backref="person", lazy="selectin"
    )
    supervision_violations = relationship(
        "StateSupervisionViolation", backref="person", lazy="selectin"
    )
    supervision_contacts = relationship(
        "StateSupervisionContact", backref="person", lazy="selectin"
    )
    employment_periods = relationship(
        "StateEmploymentPeriod", backref="person", lazy="selectin"
    )
    drug_screens = relationship("StateDrugScreen", backref="person", lazy="selectin")
    supervising_officer = relationship("StateAgent", uselist=False, lazy="selectin")


class StateCourtCase(StateBase, _ReferencesStatePersonSharedColumns):
    """Represents a StateCourtCase in the SQL schema"""

    __tablename__ = "state_court_case"
    __table_args__ = (
        UniqueConstraint(
            "state_code",
            "external_id",
            "person_id",
            name="court_case_external_ids_unique_within_state_and_person",
            deferrable=True,
            initially="DEFERRED",
        ),
        {
            "comment": "The StateCourtCase object holds information on a single court case that a person stands trial "
            "at. This represents the case itself, not the charges brought in the case, or any sentences "
            "imposed as a result of the case."
        },
    )

    court_case_id = Column(
        Integer,
        primary_key=True,
        comment=StrictStringFormatter().format(
            PRIMARY_KEY_COMMENT_TEMPLATE, object_name="court case"
        ),
    )

    external_id = Column(
        String(255),
        index=True,
        comment=StrictStringFormatter().format(
            EXTERNAL_ID_COMMENT_TEMPLATE, object_name="StateCourtCase"
        ),
    )
    status = Column(state_court_case_status, comment="The current status of the case.")
    status_raw_text = Column(
        String(255), comment="The raw text value of the current status of the case."
    )
    court_type = Column(
        state_court_type, comment="The type of court this charge will be/was heard in."
    )
    court_type_raw_text = Column(
        String(255), comment="The raw text value of the court type."
    )
    date_convicted = Column(
        Date, comment="The date the person was convicted, if applicable."
    )
    next_court_date = Column(
        Date,
        comment="Date of the next scheduled court appearance for this case, if applicable.",
    )
    state_code = Column(
        String(255),
        nullable=False,
        index=True,
        comment=STATE_CODE_COMMENT,
    )
    county_code = Column(
        String(255),
        index=True,
        comment="The code of the county under whose jurisdiction the case was tried.",
    )
    judicial_district_code = Column(
        String(255),
        comment="The code of the judicial district under whose jurisdiction "
        "the case was tried.",
    )

    @declared_attr
    def judge_id(self) -> Column:
        return Column(
            Integer,
            ForeignKey("state_agent.agent_id"),
            index=True,
            nullable=True,
            comment="The id of the judge who tried the case.<br />"
            + StrictStringFormatter().format(
                FOREIGN_KEY_COMMENT_TEMPLATE, object_name="state agent"
            ),
        )

    person = relationship("StatePerson", uselist=False)
    judge = relationship("StateAgent", uselist=False, lazy="selectin")


class StateCharge(StateBase, _ReferencesStatePersonSharedColumns):
    """Represents a StateCharge in the SQL schema"""

    __tablename__ = "state_charge"
    __table_args__ = (
        UniqueConstraint(
            "state_code",
            "external_id",
            name="charge_external_ids_unique_within_state",
            deferrable=True,
            initially="DEFERRED",
        ),
        {
            "comment": "The StateCharge object holds information on a single charge that a person has been accused of. "
            "A single StateCharge can reference multiple Incarceration/Supervision Sentences (e.g. multiple "
            "concurrent sentences served due to an overlapping set of charges) and a multiple charges can "
            "reference a single Incarceration/Supervision Sentence (e.g. one sentence resulting from multiple "
            "charges). Thus, the relationship between StateCharge and each distinct Supervision/Incarceration "
            "Sentence type is many:many. Each StateCharge is brought to trial as part of no more than a single"
            " StateCourtCase."
        },
    )

    charge_id = Column(
        Integer,
        primary_key=True,
        comment=StrictStringFormatter().format(
            PRIMARY_KEY_COMMENT_TEMPLATE, object_name="charge"
        ),
    )

    external_id = Column(
        String(255),
        index=True,
        comment=StrictStringFormatter().format(
            EXTERNAL_ID_COMMENT_TEMPLATE, object_name="StateCharge"
        ),
    )
    status = Column(
        state_charge_status, nullable=False, comment="The status of the charge."
    )
    status_raw_text = Column(
        String(255), comment="The raw text value of the status of the charge."
    )
    offense_date = Column(
        Date, comment="The date of the alleged offense that led to this charge."
    )
    date_charged = Column(
        Date, comment="The date the person was charged with the alleged offense."
    )
    state_code = Column(
        String(255),
        nullable=False,
        index=True,
        comment=STATE_CODE_COMMENT,
    )
    county_code = Column(
        String(255),
        index=True,
        comment="The code of the county under whose jurisdiction the charge was brought.",
    )
    ncic_code = Column(
        String(255),
        comment="The standardized NCIC (National Crime Information Center) code for "
        "the charged offense. NCIC codes are a set of nationally recognized "
        "codes for certain types of crimes.",
    )
    statute = Column(
        String(255),
        comment="The identifier of the charge in the state or federal code.",
    )
    description = Column(Text, comment="A text description of the charge.")
    attempted = Column(
        Boolean,
        comment="Whether this charge was an attempt or not (e.g. attempted murder).",
    )
    classification_type = Column(
        state_charge_classification_type, comment="Charge classification."
    )
    classification_type_raw_text = Column(
        String(255), comment="The raw text value of the charge classification."
    )
    classification_subtype = Column(
        String(255),
        comment="The sub-classification of the charge, such as a degree "
        "(e.g. 1st Degree, 2nd Degree, etc.) or a class (e.g. Class A,"
        " Class B, etc.).",
    )
    offense_type = Column(
        String(255), comment="The type of offense associated with the charge."
    )
    is_violent = Column(
        Boolean, comment="Whether this charge was for a violent crime or not."
    )
    is_sex_offense = Column(
        Boolean, comment="Whether or not the violation involved a sex offense."
    )
    counts = Column(
        Integer,
        comment="The number of counts of this charge which are being brought against the person.",
    )
    charge_notes = Column(
        Text, comment="Free text containing other information about a charge."
    )
    charging_entity = Column(
        String(255),
        comment="The entity that brought this charge (e.g., Boston Police"
        " Department, Southern District of New York).",
    )
    is_controlling = Column(
        Boolean,
        comment='Whether or not this is the "controlling" charge in a set of related '
        "charges. A controlling charge is the one which is responsible for the "
        "longest possible sentence duration in the set.",
    )

    @declared_attr
    def court_case_id(self) -> Column:
        return Column(
            Integer,
            ForeignKey("state_court_case.court_case_id"),
            index=True,
            comment=StrictStringFormatter().format(
                FOREIGN_KEY_COMMENT_TEMPLATE, object_name="court case"
            ),
        )

    # Cross-entity relationships
    person = relationship("StatePerson", uselist=False)
    court_case = relationship(
        "StateCourtCase", uselist=False, backref="charges", lazy="selectin"
    )


class StateAssessment(StateBase, _ReferencesStatePersonSharedColumns):
    """Represents a StateAssessment in the SQL schema"""

    __tablename__ = "state_assessment"
    __table_args__ = {
        "comment": "The StateAssessment object represents information about an "
        "assessment conducted for some person. Assessments are used in various stages "
        "of the justice system to assess a person's risk, or a person's needs, or to "
        "determine what course of action to take, such as pretrial sentencing or "
        "program reference."
    }

    assessment_id = Column(
        Integer,
        primary_key=True,
        comment=StrictStringFormatter().format(
            PRIMARY_KEY_COMMENT_TEMPLATE, object_name="assessment"
        ),
    )

    external_id = Column(
        String(255),
        index=True,
        comment=StrictStringFormatter().format(
            EXTERNAL_ID_COMMENT_TEMPLATE, object_name="StateAssessment"
        ),
    )
    assessment_class = Column(
        state_assessment_class,
        comment="The classification of assessment that was conducted.",
    )
    assessment_class_raw_text = Column(
        String(255), comment="The raw text value of the classification of assessment."
    )
    assessment_type = Column(
        state_assessment_type,
        comment="The specific type of assessment that was conducted.",
    )
    assessment_type_raw_text = Column(
        String(255), comment="The raw text value of the assessment type."
    )
    assessment_date = Column(Date, comment="The date the assessment was conducted.")
    state_code = Column(
        String(255),
        nullable=False,
        index=True,
        comment=STATE_CODE_COMMENT,
    )
    assessment_score = Column(
        Integer, comment="The final score output by the assessment, if applicable."
    )
    assessment_level = Column(
        state_assessment_level,
        comment="The final level output by the assessment, " "if applicable.",
    )
    assessment_level_raw_text = Column(
        String(255), comment="The raw text value of the assessment level"
    )
    assessment_metadata = Column(
        Text,
        comment="This includes whichever fields and values are relevant to a fine "
        "understanding of a particular assessment. It can be provided in any "
        "format, but will be transformed into JSON prior to persistence.",
    )

    @declared_attr
    def conducting_agent_id(self) -> Column:
        return Column(
            Integer,
            ForeignKey("state_agent.agent_id"),
            index=True,
            nullable=True,
            comment="The id of the agent conducting this assessment.",
        )

    conducting_agent = relationship("StateAgent", uselist=False, lazy="selectin")


class StateSupervisionSentence(StateBase, _ReferencesStatePersonSharedColumns):
    """Represents a StateSupervisionSentence in the SQL schema"""

    __tablename__ = "state_supervision_sentence"
    __table_args__ = {
        "comment": "The StateSupervisionSentence object represents information about a single sentence to a period of "
        "supervision imposed as part of a group of related sentences. Multiple distinct, related sentences "
        "to supervision should be captured as separate supervision sentence objects within the same group. "
        "These sentences may, for example, be concurrent or consecutive to one another. "
        "Like the sentence group above, the supervision sentence represents only the imposition of some "
        "sentence terms, not an actual period of supervision experienced by the person.<br /><br />"
        "A StateSupervisionSentence object can reference many charges, and each charge can reference many "
        "sentences -- the relationship is many:many.<br /><br />"
        "A StateSupervisionSentence can have multiple child StateSupervisionPeriods. It can also have child "
        "StateIncarcerationPeriods since a sentence to supervision may result in a person's parole being "
        "revoked and the person being re-incarcerated, for example. In some jurisdictions, this would be "
        "modeled as distinct sentences of supervision and incarceration, but this is not universal."
    }

    supervision_sentence_id = Column(
        Integer,
        primary_key=True,
        comment=StrictStringFormatter().format(
            PRIMARY_KEY_COMMENT_TEMPLATE, object_name="supervision sentence"
        ),
    )

    external_id = Column(
        String(255),
        index=True,
        comment=StrictStringFormatter().format(
            EXTERNAL_ID_COMMENT_TEMPLATE, object_name="StateSupervisionSentence"
        ),
    )
    status = Column(
        state_sentence_status,
        nullable=False,
        comment="The current status of this sentence.",
    )
    status_raw_text = Column(
        String(255),
        comment="The raw text value of the current status of this sentence.",
    )
    supervision_type = Column(
        state_supervision_sentence_supervision_type,
        comment="The type of supervision the person is being sentenced to.",
    )
    supervision_type_raw_text = Column(
        String(255),
        comment="The raw text value of the type of supervision the person is being sentenced to.",
    )
    date_imposed = Column(
        Date,
        comment="The date this sentence was imposed, e.g. the date of actual sentencing, but not necessarily "
        "the date the person started serving the sentence.",
    )
    start_date = Column(
        Date, comment="The date the person started serving the sentence."
    )
    projected_completion_date = Column(
        Date,
        comment="The earliest projected date the person may have completed their supervision.",
    )
    completion_date = Column(
        Date, comment="The date the person actually did complete their supervision."
    )
    state_code = Column(
        String(255),
        nullable=False,
        index=True,
        comment=STATE_CODE_COMMENT,
    )
    county_code = Column(
        String(255),
        index=True,
        comment="The code of the county under whose jurisdiction the sentence was imposed.",
    )
    min_length_days = Column(
        Integer, comment="Minimum duration of this sentence in days."
    )
    max_length_days = Column(
        Integer, comment="Maximum duration of this sentence in days."
    )
    sentence_metadata = Column(
        Text,
        comment="Arbitrary JSON-formatted metadata relevant to a fine"
        " understanding of a particular sentence. It can be provided in any "
        "format, but will be transformed into JSON prior to persistence.",
    )
    # This field can contain an arbitrarily long list of conditions, so we do not restrict the length of the string like
    # we do for most other String fields.
    conditions = Column(
        Text,
        comment="The conditions of this supervision sentence which the person must follow "
        "to avoid a disciplinary response. If this field is empty, there may still be"
        " applicable conditions that apply to someone's current term of supervision/incarceration - "
        "either inherited from another ongoing sentence or the current supervision term."
        " (See conditions on StateSupervisionPeriod).",
    )

    charges = relationship(
        "StateCharge",
        secondary=state_charge_supervision_sentence_association_table,
        backref="supervision_sentences",
        lazy="selectin",
    )
    early_discharges = relationship(
        "StateEarlyDischarge", backref="supervision_sentence", lazy="selectin"
    )


class StateIncarcerationSentence(StateBase, _ReferencesStatePersonSharedColumns):
    """Represents a StateIncarcerationSentence in the SQL schema"""

    __tablename__ = "state_incarceration_sentence"
    __table_args__ = (
        UniqueConstraint(
            "state_code",
            "external_id",
            name="incarceration_sentence_external_ids_unique_within_state",
            deferrable=True,
            initially="DEFERRED",
        ),
        {
            "comment": "The StateIncarcerationSentence object represents information about a single sentence to a "
            "period of incarceration imposed as part of a group of related sentences. Multiple distinct, related "
            "sentences to incarceration should be captured as separate incarceration sentence objects within the same "
            "group. These sentences may, for example, be concurrent or consecutive to one another. Like the sentence "
            "group, the StateIncarcerationSentence represents only the imposition of some sentence terms, "
            "not an actual period of incarceration experienced by the person.<br /><br />A StateIncarcerationSentence "
            "can reference many charges, and each charge can reference many sentences -- the relationship "
            "is many:many.<br /><br />A StateIncarcerationSentence can have multiple child StateIncarcerationPeriods. "
            "It can also have child StateSupervisionPeriods since a sentence to incarceration may result in a person "
            "being paroled, for example. In some jurisdictions, this would be modeled as distinct sentences of "
            "incarceration and supervision, but this is not universal."
        },
    )

    incarceration_sentence_id = Column(
        Integer,
        primary_key=True,
        comment=StrictStringFormatter().format(
            PRIMARY_KEY_COMMENT_TEMPLATE, object_name="incarceration sentence"
        ),
    )

    external_id = Column(
        String(255),
        index=True,
        comment=StrictStringFormatter().format(
            EXTERNAL_ID_COMMENT_TEMPLATE, object_name="StateIncarcerationSentence"
        ),
    )
    status = Column(
        state_sentence_status,
        nullable=False,
        comment="The current status of this sentence.",
    )
    status_raw_text = Column(
        String(255), comment="The raw text value of the status of the sentence."
    )
    incarceration_type = Column(
        state_incarceration_type,
        comment="The type of incarceration the person is being sentenced to.",
    )
    incarceration_type_raw_text = Column(
        String(255),
        comment="The raw text value of the type of incarceration of this sentence.",
    )
    date_imposed = Column(
        Date,
        comment="The date this sentence was imposed, e.g. the date of actual sentencing, but not necessarily the "
        "date the person started serving the sentence",
    )
    start_date = Column(Date, comment="The date this sentence started.")
    projected_min_release_date = Column(
        Date,
        comment="The earliest projected date the person may be released from incarceration due to this sentence.",
    )
    projected_max_release_date = Column(
        Date,
        comment="The latest projected date the person may be released from incarceration due to this sentence.",
    )
    completion_date = Column(Date, comment="The date this sentence has been completed.")
    parole_eligibility_date = Column(
        Date,
        comment="The first date under which the person becomes eligible for parole under the terms of this sentence.",
    )
    state_code = Column(
        String(255),
        nullable=False,
        index=True,
        comment="The code of the state under whose jurisdiction the sentence was imposed.",
    )
    county_code = Column(
        String(255),
        index=True,
        comment="The code of the county under whose jurisdiction the sentence was imposed.",
    )
    min_length_days = Column(
        Integer, comment="The minimum duration of this sentence in days."
    )
    max_length_days = Column(
        Integer, comment="The maximum duration of this sentence in days."
    )
    is_life = Column(Boolean, comment="Whether or not this is a life sentence.")
    is_capital_punishment = Column(
        Boolean, comment="Whether or not this is a sentence for the death penalty."
    )
    parole_possible = Column(
        Boolean,
        comment="Whether or not the person may be released to parole under the terms of this sentence.",
    )
    initial_time_served_days = Column(
        Integer,
        comment="The amount of any time already served (in days), to possible be credited against "
        "the overall sentence duration.",
    )
    good_time_days = Column(
        Integer,
        comment="Any good time (in days) the person has credited against this sentence due to good conduct, a.k.a. "
        "time off for good behavior, if applicable.",
    )
    earned_time_days = Column(
        Integer,
        comment="Any earned time (in days) the person has credited against this sentence due to participation in "
        "programming designed to reduce the likelihood of re-offense, if applicable.",
    )
    sentence_metadata = Column(
        Text,
        comment="Arbitrary JSON-formatted metadata relevant to a fine"
        " understanding of a particular sentence. It can be provided in any "
        "format, but will be transformed into JSON prior to persistence.",
    )
    # This field can contain an arbitrarily long list of conditions, so we do not restrict the length of the string like
    # we do for most other String fields.
    conditions = Column(
        Text,
        comment="The conditions of this incarceration sentence which the person must follow "
        "to avoid a disciplinary response. If this field is empty, there may still be"
        " applicable conditions that apply to someone's current term of supervision/incarceration - "
        "either inherited from another ongoing sentence or the current supervision term."
        " (See conditions on StateSupervisionPeriod).",
    )

    charges = relationship(
        "StateCharge",
        secondary=state_charge_incarceration_sentence_association_table,
        backref="incarceration_sentences",
        lazy="selectin",
    )
    early_discharges = relationship(
        "StateEarlyDischarge", backref="incarceration_sentence", lazy="selectin"
    )


class StateIncarcerationPeriod(StateBase, _ReferencesStatePersonSharedColumns):
    """Represents a StateIncarcerationPeriod in the SQL schema"""

    __tablename__ = "state_incarceration_period"
    __table_args__ = (
        UniqueConstraint(
            "state_code",
            "external_id",
            name="incarceration_period_external_ids_unique_within_state",
            deferrable=True,
            initially="DEFERRED",
        ),
        {
            "comment": "The StateIncarcerationPeriod object represents information "
            "about a single period of incarceration, defined as a contiguous stay by a "
            "particular person in a particular facility. As a person transfers from "
            "facility to facility, these are modeled as multiple abutting "
            "incarceration periods. This also extends to temporary transfers to, say, "
            "hospitals or court appearances. The sequence of incarceration periods can "
            "be squashed into longer conceptual periods (e.g. from the first admission "
            "to the final release for a particular sentence) for analytical purposes, "
            "such as measuring recidivism and revocation -- this is done with a "
            "fine-grained examination of the admission dates, admission reasons, "
            "release dates, and release reasons of consecutive incarceration periods."
            "<br /><br />Handling of incarceration periods is a crucial aspect of our "
            "platform and involves work in jurisdictional ingest mappings, entity "
            "matching, and calculation. Fortunately, this means that we have practice "
            "working with varied representations of this information."
        },
    )
    incarceration_period_id = Column(
        Integer,
        primary_key=True,
        comment=StrictStringFormatter().format(
            PRIMARY_KEY_COMMENT_TEMPLATE, object_name="incarceration period"
        ),
    )

    external_id = Column(
        String(255),
        index=True,
        comment=StrictStringFormatter().format(
            EXTERNAL_ID_COMMENT_TEMPLATE, object_name="StateIncarcerationPeriod"
        ),
    )
    incarceration_type = Column(
        state_incarceration_type,
        comment="The type of incarceration the person is serving.",
    )
    incarceration_type_raw_text = Column(
        String(255), comment="The raw text value of the incarceration period type."
    )
    admission_date = Column(
        Date,
        comment="The date the person was admitted to this particular period of incarceration.",
    )
    release_date = Column(
        Date,
        comment="The date the person was released from this particular period of incarceration.",
    )
    state_code = Column(
        String(255),
        nullable=False,
        index=True,
        comment=STATE_CODE_COMMENT,
    )
    county_code = Column(
        String(255),
        index=True,
        comment="he code of the county where the person is currently incarcerated.",
    )
    facility = Column(
        String(255),
        comment="The facility in which the person is currently incarcerated.",
    )
    housing_unit = Column(
        String(255),
        comment="The housing unit within the facility in which the person currently resides.",
    )
    admission_reason = Column(
        state_incarceration_period_admission_reason,
        comment="The reason the person was admitted to this particular period of incarceration.",
    )
    admission_reason_raw_text = Column(
        String(255),
        comment="The raw text value of the incarceration period admission reason.",
    )
    release_reason = Column(
        state_incarceration_period_release_reason,
        comment="The reason the person was released from this particular period of incarceration.",
    )
    release_reason_raw_text = Column(
        String(255),
        comment="The raw text value of the incarceration period's release reason.",
    )
    specialized_purpose_for_incarceration = Column(
        state_specialized_purpose_for_incarceration,
        comment="The specialized purpose for incarceration for this "
        "particular incarceration period.",
    )
    specialized_purpose_for_incarceration_raw_text = Column(
        String(255),
        comment="The raw text value of the specialized purpose " "for incarceration.",
    )
    custodial_authority = Column(
        state_custodial_authority,
        comment=CUSTODIAL_AUTHORITY_COMMENT,
    )
    custodial_authority_raw_text = Column(
        String(255),
        comment="The raw text value of the incarceration period's "
        "custodial authority.",
    )


class StateSupervisionPeriod(StateBase, _ReferencesStatePersonSharedColumns):
    """Represents a StateSupervisionPeriod in the SQL schema"""

    __tablename__ = "state_supervision_period"
    __table_args__ = {
        "comment": "The StateSupervisionPeriod object represents information about a "
        "single period of supervision, defined as a contiguous period of custody for a "
        "particular person under a particular jurisdiction. As a person transfers "
        "between supervising locations, these are modeled as multiple abutting "
        "supervision periods. Multiple periods of supervision for a particular person "
        "may be overlapping, due to extended periods of supervision that are "
        "temporarily interrupted by, say, periods of incarceration, or periods of "
        "supervision stemming from different charges."
    }

    supervision_period_id = Column(
        Integer,
        primary_key=True,
        comment=StrictStringFormatter().format(
            PRIMARY_KEY_COMMENT_TEMPLATE, object_name="supervision period"
        ),
    )

    external_id = Column(
        String(255),
        index=True,
        comment=StrictStringFormatter().format(
            EXTERNAL_ID_COMMENT_TEMPLATE, object_name="StateSupervisionPeriod"
        ),
    )
    supervision_type = Column(
        state_supervision_period_supervision_type,
        comment="The type of supervision the person is serving during "
        "this time period.",
    )
    supervision_type_raw_text = Column(
        String(255),
        comment="The raw text value of the supervision period" " supervision type.",
    )
    start_date = Column(
        Date, comment="The date the person began this period of supervision."
    )
    termination_date = Column(
        Date,
        comment="The date the period of supervision was terminated, either positively"
        " or negatively.",
    )
    state_code = Column(
        String(255),
        nullable=False,
        index=True,
        comment=STATE_CODE_COMMENT,
    )
    county_code = Column(
        String(255),
        index=True,
        comment="The code of the county where the person is currently supervised.",
    )
    supervision_site = Column(
        String(255),
        comment="A single string encoding the location (i.e. office/region/district) this person is being supervised"
        " out of. This field may eventually be split into multiple to better encode supervision org structure. "
        "See #3829.",
    )
    admission_reason = Column(
        state_supervision_period_admission_reason,
        comment="The reason the person was admitted to this particular period of supervision.",
    )
    admission_reason_raw_text = Column(
        String(255),
        comment="The raw text value of the supervision period's admission reason.",
    )
    termination_reason = Column(
        state_supervision_period_termination_reason,
        comment="The reason the period of supervision was terminated.",
    )
    termination_reason_raw_text = Column(
        String(255),
        comment="The raw text value of the supervision period's termination reason.",
    )
    supervision_level = Column(
        state_supervision_level,
        comment="The level of supervision the person is receiving, "
        "i.e. an analog to the security level of "
        "incarceration, indicating frequency of contact, "
        "strictness of constraints, etc.",
    )
    supervision_level_raw_text = Column(
        String(255),
        comment="The raw text value of the supervision period's " "supervision level.",
    )

    # This field can contain an arbitrarily long list of conditions, so we do not restrict the length of the string like
    # we do for most other String fields.
    conditions = Column(
        Text,
        comment="The conditions of this period of supervision which the person must follow"
        "to avoid a disciplinary response. If this is empty, there may still be applicable "
        "conditions that apply to the whole term of the sentence. "
        "(See conditions on StateSupervisionSentence/StateIncarcerationSentence)",
    )
    custodial_authority = Column(
        state_custodial_authority,
        comment=CUSTODIAL_AUTHORITY_COMMENT,
    )
    custodial_authority_raw_text = Column(
        String(255),
        comment="The raw text value of the supervision period's custodial authority.",
    )

    @declared_attr
    def supervising_officer_id(self) -> Column:
        return Column(
            Integer,
            ForeignKey("state_agent.agent_id"),
            index=True,
            nullable=True,
            comment=StrictStringFormatter().format(
                FOREIGN_KEY_COMMENT_TEMPLATE, object_name="state agent"
            ),
        )

    supervising_officer = relationship("StateAgent", uselist=False, lazy="selectin")
    case_type_entries = relationship(
        "StateSupervisionCaseTypeEntry", backref="supervision_period", lazy="selectin"
    )


class StateSupervisionCaseTypeEntry(StateBase, _ReferencesStatePersonSharedColumns):
    """Represents a StateSupervisionCaseTypeEntry in the SQL schema"""

    __tablename__ = "state_supervision_case_type_entry"
    __table_args__ = (
        UniqueConstraint(
            "state_code",
            "external_id",
            name="supervision_case_type_entry_external_ids_unique_within_state",
            deferrable=True,
            initially="DEFERRED",
        ),
        {
            "comment": "The StateSupervisionCaseTypeEntry object represents a particular case type that applies to this "
            "period of supervision. A case type implies certain conditions of supervision that may apply, or "
            "certain levels or intensity of supervision, or certain kinds of specialized courts that "
            "generated the sentence to supervision, or even that the person being supervised may be "
            "supervised by particular kinds of officers with particular types of caseloads they are "
            "responsible for. A StateSupervisionPeriod may have zero to many distinct case types."
        },
    )

    supervision_case_type_entry_id = Column(
        Integer,
        primary_key=True,
        comment=StrictStringFormatter().format(
            PRIMARY_KEY_COMMENT_TEMPLATE, object_name="case type entry"
        ),
    )

    case_type = Column(
        state_supervision_case_type,
        comment="The type of case that describes the associated period of supervision.",
    )
    case_type_raw_text = Column(
        String(255), comment="The raw text value of the case type."
    )
    state_code = Column(
        String(255),
        nullable=False,
        index=True,
        comment=STATE_CODE_COMMENT,
    )

    @declared_attr
    def supervision_period_id(self) -> Column:
        return Column(
            Integer,
            ForeignKey("state_supervision_period.supervision_period_id"),
            index=True,
            nullable=True,
            comment=StrictStringFormatter().format(
                FOREIGN_KEY_COMMENT_TEMPLATE, object_name="state supervision period"
            ),
        )

    person = relationship("StatePerson", uselist=False)

    external_id = Column(
        String(255),
        index=True,
        comment=StrictStringFormatter().format(
            EXTERNAL_ID_COMMENT_TEMPLATE, object_name="StateSupervisionCaseTypeEntry"
        ),
    )


class StateIncarcerationIncident(StateBase, _ReferencesStatePersonSharedColumns):
    """Represents a StateIncarcerationIncident in the SQL schema"""

    __tablename__ = "state_incarceration_incident"
    __table_args__ = (
        UniqueConstraint(
            "state_code",
            "external_id",
            name="incarceration_incident_external_ids_unique_within_state",
            deferrable=True,
            initially="DEFERRED",
        ),
        {
            "comment": "The StateIncarcerationIncident object represents any behavioral incidents recorded against a "
            "person during a period of incarceration, such as a fight with another incarcerated individual "
            "or the possession of contraband. A StateIncarcerationIncident has zero to many "
            "StateIncarcerationIncidentOutcome children, indicating any official outcomes "
            "(e.g. disciplinary responses) due to the incident."
        },
    )

    incarceration_incident_id = Column(
        Integer,
        primary_key=True,
        comment=StrictStringFormatter().format(
            PRIMARY_KEY_COMMENT_TEMPLATE, object_name="incarceartion incident"
        ),
    )

    external_id = Column(
        String(255),
        index=True,
        comment=StrictStringFormatter().format(
            EXTERNAL_ID_COMMENT_TEMPLATE, object_name="StateIncarcerationIncident"
        ),
    )
    incident_type = Column(
        state_incarceration_incident_type, comment="The type of incident."
    )
    incident_type_raw_text = Column(
        String(255), comment="The raw text value of the incident type."
    )
    incident_date = Column(Date, comment="The date on which the incident took place.")
    state_code = Column(
        String(255),
        nullable=False,
        index=True,
        comment=STATE_CODE_COMMENT,
    )
    facility = Column(
        String(255), comment="The facility in which the incident took place."
    )
    location_within_facility = Column(
        String(255), comment="The more specific location where the incident took place."
    )
    incident_details = Column(
        Text, comment="Descriptive notes describing the incident."
    )

    @declared_attr
    def responding_officer_id(self) -> Column:
        return Column(
            Integer,
            ForeignKey("state_agent.agent_id"),
            index=True,
            nullable=True,
            comment=StrictStringFormatter().format(
                FOREIGN_KEY_COMMENT_TEMPLATE, object_name="state agent"
            ),
        )

    responding_officer = relationship("StateAgent", uselist=False, lazy="selectin")

    incarceration_incident_outcomes = relationship(
        "StateIncarcerationIncidentOutcome",
        backref="incarceration_incident",
        lazy="selectin",
    )


class StateIncarcerationIncidentOutcome(StateBase, _ReferencesStatePersonSharedColumns):
    """Represents a StateIncarcerationIncidentOutcome in the SQL schema"""

    __tablename__ = "state_incarceration_incident_outcome"
    __table_args__ = (
        UniqueConstraint(
            "state_code",
            "external_id",
            name="incarceration_incident_outcome_external_ids_unique_within_state",
            deferrable=True,
            initially="DEFERRED",
        ),
        {
            "comment": "The StateIncarcerationIncidentOutcome object represents the outcomes in response to a particular "
            "StateIncarcerationIncident. These can be positive, neutral, or negative, but they should never "
            "be empty or null -- an incident that has no outcomes should simply have no "
            "StateIncarcerationIncidentOutcome children objects."
        },
    )

    incarceration_incident_outcome_id = Column(
        Integer,
        primary_key=True,
        comment=StrictStringFormatter().format(
            PRIMARY_KEY_COMMENT_TEMPLATE, object_name="incarceration incident outcome"
        ),
    )

    external_id = Column(
        String(255),
        index=True,
        comment=StrictStringFormatter().format(
            EXTERNAL_ID_COMMENT_TEMPLATE,
            object_name="StateIncarcerationIncidentOutcome",
        ),
    )
    outcome_type = Column(
        state_incarceration_incident_outcome_type, comment="The type of outcome."
    )
    outcome_type_raw_text = Column(
        String(255), comment="The raw text value of the outcome type."
    )
    state_code = Column(
        String(255),
        nullable=False,
        index=True,
        comment=STATE_CODE_COMMENT,
    )
    date_effective = Column(Date, comment="The date on which the outcome takes effect.")
    hearing_date = Column(
        Date, comment="The date on which the hearing for the incident is taking place."
    )
    report_date = Column(Date, comment="The date on which the incident was reported.")
    outcome_description = Column(
        String(255), comment="Descriptive notes describing the outcome."
    )
    punishment_length_days = Column(
        Integer, comment="The length of any durational, punishment-focused outcome."
    )

    @declared_attr
    def incarceration_incident_id(self) -> Column:
        return Column(
            Integer,
            ForeignKey("state_incarceration_incident.incarceration_incident_id"),
            index=True,
            nullable=True,
            comment=StrictStringFormatter().format(
                FOREIGN_KEY_COMMENT_TEMPLATE, object_name="incarceration incident"
            ),
        )

    person = relationship("StatePerson", uselist=False)


class StateSupervisionViolationTypeEntry(
    StateBase, _ReferencesStatePersonSharedColumns
):
    """Represents a StateSupervisionViolationTypeEntry in the SQL schema."""

    __tablename__ = "state_supervision_violation_type_entry"
    __table_args__ = {
        "comment": "The StateSupervisionViolationTypeEntry object represents each specific violation "
        "type that was composed within a single violation. Each supervision violation has "
        "zero to many such violation types. For example, a single violation may have been "
        "reported for both absconsion and a technical violation. However, it may also be "
        "the case that separate violations were recorded for both an absconsion and a "
        "technical violation which were related in the real world. The drawing line is "
        "how the violation is itself reported in the source data: if a single violation"
        " report filed by an agency staff member includes multiple types of violations, "
        "then it will be ingested into our schema as a single supervision violation with"
        " multiple supervision violation type entries."
    }

    supervision_violation_type_entry_id = Column(
        Integer,
        primary_key=True,
        comment=StrictStringFormatter().format(
            PRIMARY_KEY_COMMENT_TEMPLATE, object_name="supervision violation type entry"
        ),
    )

    state_code = Column(
        String(255), nullable=False, index=True, comment=STATE_CODE_COMMENT
    )
    violation_type = Column(
        state_supervision_violation_type, comment="The type of violation."
    )
    violation_type_raw_text = Column(
        String(255), comment="The raw text value of the violation type."
    )

    @declared_attr
    def supervision_violation_id(self) -> Column:
        return Column(
            Integer,
            ForeignKey("state_supervision_violation.supervision_violation_id"),
            index=True,
            nullable=True,
            comment=StrictStringFormatter().format(
                FOREIGN_KEY_COMMENT_TEMPLATE, object_name="supervision violation"
            ),
        )

    person = relationship("StatePerson", uselist=False)


class StateSupervisionViolatedConditionEntry(
    StateBase, _ReferencesStatePersonSharedColumns
):
    """Represents a StateSupervisionViolatedConditionEntry in the SQL schema."""

    __tablename__ = "state_supervision_violated_condition_entry"
    __table_args__ = {
        "comment": "The StateSupervisionViolatedConditionEntry object represents a particular condition of supervision "
        "which was violated by a particular supervision violation. Each supervision violation has zero "
        "to many violated conditions. For example, a violation may be recorded because a brand new charge "
        "has been brought against the supervised person."
    }

    supervision_violated_condition_entry_id = Column(
        Integer,
        primary_key=True,
        comment=StrictStringFormatter().format(
            PRIMARY_KEY_COMMENT_TEMPLATE,
            object_name="supervision violated condition entry",
        ),
    )

    state_code = Column(
        String(255),
        nullable=False,
        index=True,
        comment=STATE_CODE_COMMENT,
    )
    condition = Column(
        String(255),
        nullable=False,
        comment="The specific condition of supervision which was violated.",
    )

    @declared_attr
    def supervision_violation_id(self) -> Column:
        return Column(
            Integer,
            ForeignKey("state_supervision_violation.supervision_violation_id"),
            index=True,
            nullable=True,
            comment=StrictStringFormatter().format(
                FOREIGN_KEY_COMMENT_TEMPLATE, object_name="supervision violation"
            ),
        )

    person = relationship("StatePerson", uselist=False)


class StateSupervisionViolation(StateBase, _ReferencesStatePersonSharedColumns):
    """Represents a StateSupervisionViolation in the SQL schema"""

    __tablename__ = "state_supervision_violation"
    __table_args__ = (
        UniqueConstraint(
            "state_code",
            "external_id",
            name="supervision_violation_external_ids_unique_within_state",
            deferrable=True,
            initially="DEFERRED",
        ),
        {
            "comment": "The StateSupervisionViolation object represents any violations recorded against a person"
            " during a period of supervision, such as technical violation or a new offense. A "
            "StateSupervisionViolation has zero to many StateSupervisionViolationResponse children, "
            "indicating any official response to the violation, e.g. a disciplinary response such as a "
            "revocation back to prison or extension of supervision."
        },
    )

    supervision_violation_id = Column(
        Integer,
        primary_key=True,
        comment=StrictStringFormatter().format(
            PRIMARY_KEY_COMMENT_TEMPLATE, object_name="supervision violation"
        ),
    )

    external_id = Column(
        String(255),
        index=True,
        comment=StrictStringFormatter().format(
            EXTERNAL_ID_COMMENT_TEMPLATE, object_name="StateSupervisionViolation"
        ),
    )

    violation_date = Column(Date, comment="The date on which the violation took place.")
    state_code = Column(
        String(255), nullable=False, index=True, comment=STATE_CODE_COMMENT
    )
    is_violent = Column(
        Boolean, comment="Whether or not the violation was violent in nature."
    )
    is_sex_offense = Column(
        Boolean, comment="Whether or not the violation involved a sex offense."
    )

    supervision_violation_types = relationship(
        "StateSupervisionViolationTypeEntry",
        backref="supervision_violation",
        lazy="selectin",
    )
    supervision_violated_conditions = relationship(
        "StateSupervisionViolatedConditionEntry",
        backref="supervision_violation",
        lazy="selectin",
    )
    supervision_violation_responses = relationship(
        "StateSupervisionViolationResponse",
        backref="supervision_violation",
        lazy="selectin",
    )


class StateSupervisionViolationResponseDecisionEntry(
    StateBase, _ReferencesStatePersonSharedColumns
):
    """Represents a StateSupervisionViolationResponseDecisionEntry in the
    SQL schema.
    """

    __tablename__ = "state_supervision_violation_response_decision_entry"
    __table_args__ = {
        "comment": "The StateSupervisionViolationResponseDecisionEntry object represents each "
        "specific decision made in response to a particular supervision violation. Each "
        "supervision violation response has zero to many such decisions. Decisions are "
        "essentially the final consequences of a violation, actions such as continuance, "
        "privileges revoked, or revocation."
    }

    supervision_violation_response_decision_entry_id = Column(
        Integer,
        primary_key=True,
        comment=StrictStringFormatter().format(
            PRIMARY_KEY_COMMENT_TEMPLATE,
            object_name="supervision violation response decision entry",
        ),
    )

    state_code = Column(
        String(255), nullable=False, index=True, comment=STATE_CODE_COMMENT
    )
    decision = Column(
        state_supervision_violation_response_decision,
        comment="A specific decision that was made in response, if applicable.",
    )
    decision_raw_text = Column(
        String(255),
        comment="The raw text value of the supervision violation response decision.",
    )

    @declared_attr
    def supervision_violation_response_id(self) -> Column:
        return Column(
            Integer,
            ForeignKey(
                "state_supervision_violation_response."
                "supervision_violation_response_id"
            ),
            index=True,
            nullable=True,
            comment=StrictStringFormatter().format(
                FOREIGN_KEY_COMMENT_TEMPLATE,
                object_name="supervision violation response",
            ),
        )

    person = relationship("StatePerson", uselist=False)


class StateSupervisionViolationResponse(StateBase, _ReferencesStatePersonSharedColumns):
    """Represents a StateSupervisionViolationResponse in the SQL schema"""

    __tablename__ = "state_supervision_violation_response"
    __table_args__ = (
        UniqueConstraint(
            "state_code",
            "external_id",
            name="supervision_violation_response_external_ids_unique_within_state",
            deferrable=True,
            initially="DEFERRED",
        ),
        {
            "comment": "The StateSupervisionViolationResponse object represents the official responses to a"
            " particular StateSupervisionViolation. These can be positive, neutral, or negative, but they "
            "should never be empty or null -- a violation that has no responses should simply have no "
            "StateSupervisionViolationResponse children objects.<br /><br />As described under "
            "StateIncarcerationPeriod, any StateSupervisionViolationResponse which leads to a revocation "
            "back to prison should be linked to the subsequent period of incarceration. This can be done "
            "implicitly in entity matching, or can be marked explicitly in incoming data, either here or "
            "on the incarceration period as the case may be."
        },
    )

    supervision_violation_response_id = Column(
        Integer,
        primary_key=True,
        comment=StrictStringFormatter().format(
            PRIMARY_KEY_COMMENT_TEMPLATE, object_name="supervision violation response"
        ),
    )

    external_id = Column(
        String(255),
        index=True,
        comment=StrictStringFormatter().format(
            EXTERNAL_ID_COMMENT_TEMPLATE,
            object_name="StateSupervisionViolationResponse",
        ),
    )
    response_type = Column(
        state_supervision_violation_response_type,
        comment="The type of response to the violation.",
    )
    response_type_raw_text = Column(
        String(255), comment="The raw text value of the response type."
    )
    response_subtype = Column(
        String(255), comment="The type of response subtype to the violation."
    )
    response_date = Column(
        Date, comment="The date on which the response was made official."
    )
    state_code = Column(
        String(255), nullable=False, index=True, comment=STATE_CODE_COMMENT
    )
    deciding_body_type = Column(
        state_supervision_violation_response_deciding_body_type,
        comment="The type of decision-making body who made the decision, such as a supervising "
        "officer or a parole board or a judge.",
    )
    deciding_body_type_raw_text = Column(
        String(255),
        comment="The raw text value of the supervision violation "
        "deciding body type.",
    )
    is_draft = Column(
        Boolean,
        comment="Whether or not this is response is still a draft, i.e. is not yet "
        "finalized by the deciding body.",
    )

    @declared_attr
    def supervision_violation_id(self) -> Column:
        return Column(
            Integer,
            ForeignKey("state_supervision_violation.supervision_violation_id"),
            index=True,
            nullable=True,
            comment=StrictStringFormatter().format(
                FOREIGN_KEY_COMMENT_TEMPLATE, object_name="supervision violation"
            ),
        )

    person = relationship("StatePerson", uselist=False)
    supervision_violation_response_decisions = relationship(
        "StateSupervisionViolationResponseDecisionEntry",
        backref="supervision_violation_response",
        lazy="selectin",
    )
    decision_agents = relationship(
        "StateAgent",
        secondary=state_supervision_violation_response_decision_agent_association_table,
        lazy="selectin",
    )


class StateAgent(StateBase):
    """Represents a StateAgent in the SQL schema"""

    __tablename__ = "state_agent"
    __table_args__ = {
        "comment": "The StateAgent object represents some agent operating on behalf of the criminal "
        "justice system, usually referenced in the context of taking some action related "
        "to a person moving through that system. This includes references such as the judges "
        "trying cases, the officers supervising people on parole, the individuals who make a "
        "decision at a parole hearing, and so on. We entity match across StateAgents where "
        "possible so that we can see the full scope of actions taken by a particular agent "
        "to understand patterns in their behavior."
    }

    agent_id = Column(
        Integer,
        primary_key=True,
        comment=StrictStringFormatter().format(
            PRIMARY_KEY_COMMENT_TEMPLATE, object_name="agent"
        ),
    )

    external_id = Column(
        String(255),
        index=True,
        comment=StrictStringFormatter().format(
            EXTERNAL_ID_COMMENT_TEMPLATE, object_name="StateAgent"
        ),
    )
    agent_type = Column(state_agent_type, nullable=False, comment="The type of agent.")
    agent_type_raw_text = Column(
        String(255), comment="The raw text value of the agent type."
    )
    state_code = Column(
        String(255),
        nullable=False,
        index=True,
        comment=STATE_CODE_COMMENT,
    )
    full_name = Column(String(255), comment="The state agent's full name.")


class StateProgramAssignment(StateBase, _ReferencesStatePersonSharedColumns):
    """Represents a StateProgramAssignment in the SQL schema."""

    __tablename__ = "state_program_assignment"
    __table_args__ = (
        UniqueConstraint(
            "state_code",
            "external_id",
            name="program_assignment_external_ids_unique_within_state",
            deferrable=True,
            initially="DEFERRED",
        ),
        {
            "comment": "The StateProgramAssignment object represents information about "
            "the assignment of a person to some form of rehabilitative programming -- "
            "and their participation in the program -- intended to address specific "
            "needs of the person. People can be assigned to programs while under "
            "various forms of custody, principally while incarcerated or under "
            "supervision. These programs can be administered by the "
            "agency/government, by a quasi-governmental organization, by a private "
            "third party, or any other number of service providers. The "
            "programming-related portion of our schema is still being constructed and "
            "will be added to in the near future."
        },
    )

    program_assignment_id = Column(
        Integer,
        primary_key=True,
        comment=StrictStringFormatter().format(
            PRIMARY_KEY_COMMENT_TEMPLATE, object_name="program assignment"
        ),
    )

    external_id = Column(
        String(255),
        index=True,
        comment=StrictStringFormatter().format(
            EXTERNAL_ID_COMMENT_TEMPLATE, object_name="StateProgramAssignment"
        ),
    )
    state_code = Column(
        String(255),
        nullable=False,
        index=True,
        comment=STATE_CODE_COMMENT,
    )
    # TODO(#2450): Switch program_id/location_id for a program foreign key once
    # we've ingested program information into our schema.
    program_id = Column(
        String(255), comment="Unique identifier for a program being assigned to."
    )
    program_location_id = Column(
        String(255), comment="The id of where the program takes place."
    )

    participation_status = Column(
        state_program_assignment_participation_status,
        nullable=False,
        comment="The status of the person's participation in the program.",
    )
    participation_status_raw_text = Column(
        String(255), comment="The raw text value of the participation status."
    )
    referral_date = Column(
        Date, comment="The date the person was referred to the program, if applicable."
    )
    start_date = Column(
        Date, comment="The date the person started the program, if applicable."
    )
    discharge_date = Column(
        Date,
        comment="The date the person was discharged from the program, if applicable.",
    )
    referral_metadata = Column(
        Text,
        comment="This includes whichever fields and values are relevant to a fine"
        " understanding of a particular referral. It can be provided in any "
        "format, but will be transformed into JSON prior to persistence.",
    )

    @declared_attr
    def referring_agent_id(self) -> Column:
        return Column(
            Integer,
            ForeignKey("state_agent.agent_id"),
            index=True,
            nullable=True,
            comment=StrictStringFormatter().format(
                FOREIGN_KEY_COMMENT_TEMPLATE, object_name="state agent"
            ),
        )

    referring_agent = relationship("StateAgent", uselist=False, lazy="selectin")


class StateEarlyDischarge(StateBase, _ReferencesStatePersonSharedColumns):
    """Represents a StateEarlyDischarge in the SQL schema."""

    __tablename__ = "state_early_discharge"
    __table_args__ = (
        UniqueConstraint(
            "state_code",
            "external_id",
            name="early_discharge_external_ids_unique_within_state",
            deferrable=True,
            initially="DEFERRED",
        ),
        {
            "comment": "The StateEarlyDischarge object represents a request and its associated decision to discharge "
            "a sentence before its expected end date. This includes various metadata surrounding the "
            "actual event of the early discharge request as well as who requested and approved the "
            "decision for early discharge. It is possible for a sentence to be discharged early without "
            "ending someone's supervision / incarceration term if that person is serving multiple sentences."
        },
    )

    early_discharge_id = Column(
        Integer,
        primary_key=True,
        comment=StrictStringFormatter().format(
            PRIMARY_KEY_COMMENT_TEMPLATE, object_name="early discharge"
        ),
    )

    external_id = Column(
        String(255),
        index=True,
        comment=StrictStringFormatter().format(
            EXTERNAL_ID_COMMENT_TEMPLATE, object_name="StateEarlyDischarge"
        ),
    )
    state_code = Column(
        String(255),
        nullable=False,
        index=True,
        comment=STATE_CODE_COMMENT,
    )
    county_code = Column(
        String(255),
        comment="The code of the county under whose jurisdiction the early discharge took place.",
    )
    decision_date = Column(
        Date,
        comment="The date on which the result of this early decision request was decided.",
    )
    decision = Column(
        state_early_discharge_decision,
        comment="The decided result of this early decision request.",
    )
    decision_raw_text = Column(
        String(255), comment="The raw text value of the early discharge decision."
    )
    decision_status = Column(
        state_early_discharge_decision_status,
        comment="The current status of the early discharge decision.",
    )
    decision_status_raw_text = Column(
        String(255),
        comment="The raw text value of the early discharge decision status.",
    )
    deciding_body_type = Column(
        state_acting_body_type,
        comment="The type of body that made or will make the early discharge decision.",
    )
    deciding_body_type_raw_text = Column(
        String(255), comment="The raw text value of the deciding body type."
    )
    request_date = Column(
        Date, comment="The date on which the early discharge request took place."
    )
    requesting_body_type = Column(
        state_acting_body_type,
        comment="The type of body that requested the early discharge for this person.",
    )
    requesting_body_type_raw_text = Column(
        String(255), comment="The raw text value of the requesting body type."
    )

    @declared_attr
    def supervision_sentence_id(self) -> Column:
        return Column(
            Integer,
            ForeignKey(
                "state_supervision_sentence.supervision_sentence_id",
                deferrable=True,
                initially="DEFERRED",
            ),
            index=True,
            nullable=True,
            comment=StrictStringFormatter().format(
                FOREIGN_KEY_COMMENT_TEMPLATE, object_name="supervision sentence"
            ),
        )

    @declared_attr
    def incarceration_sentence_id(self) -> Column:
        return Column(
            Integer,
            ForeignKey(
                "state_incarceration_sentence.incarceration_sentence_id",
                deferrable=True,
                initially="DEFERRED",
            ),
            index=True,
            nullable=True,
            comment=StrictStringFormatter().format(
                FOREIGN_KEY_COMMENT_TEMPLATE, object_name="incarceration sentence"
            ),
        )

    person = relationship("StatePerson", uselist=False)


class StateSupervisionContact(StateBase, _ReferencesStatePersonSharedColumns):
    """Represents a StateSupervisionContact in the SQL schema."""

    __tablename__ = "state_supervision_contact"
    __table_args__ = (
        UniqueConstraint(
            "state_code",
            "external_id",
            name="supervision_contact_external_ids_unique_within_state",
            deferrable=True,
            initially="DEFERRED",
        ),
        {
            "comment": "The StateSupervisionContact object represents information about a point of contact between a "
            "person under supervision and some agent representing the department, typically a "
            "supervising officer. These may include face-to-face meetings, phone calls, emails, or other "
            "such media. At these contacts, specific things may occur, such as referral to programming or "
            "written warnings or even arrest, but any and all events that happen as part of a single contact "
            "are modeled as one supervision contact. StateSupervisionPeriods have zero to many "
            "StateSupervisionContacts as children, and each StateSupervisionContact has one to many "
            "StateSupervisionPeriods as parents. This is because a given person may be serving multiple "
            "periods of supervision simultaneously in rare cases, and a given point of contact may apply "
            "to both."
        },
    )

    supervision_contact_id = Column(
        Integer,
        primary_key=True,
        comment=StrictStringFormatter().format(
            PRIMARY_KEY_COMMENT_TEMPLATE, object_name="supervision contact"
        ),
    )

    external_id = Column(
        String(255),
        index=True,
        comment=StrictStringFormatter().format(
            EXTERNAL_ID_COMMENT_TEMPLATE, object_name="StateSupervisionContact"
        ),
    )
    state_code = Column(
        String(255),
        nullable=False,
        index=True,
        comment=STATE_CODE_COMMENT,
    )

    contact_date = Column(Date, comment="The date when this contact happened.")
    contact_reason = Column(
        state_supervision_contact_reason,
        comment="The reason why this contact took place.",
    )
    contact_reason_raw_text = Column(
        String(255), comment="The raw text value of the contact reason."
    )
    contact_type = Column(
        state_supervision_contact_type, comment="The type of contact which took place."
    )
    contact_type_raw_text = Column(
        String(255), comment="The raw text value of the contact type."
    )
    contact_method = Column(
        state_supervision_contact_method,
        comment="The method used to perform the contact.",
    )
    contact_method_raw_text = Column(
        String(255), comment="The raw text value of the contact method."
    )
    location = Column(
        state_supervision_contact_location, comment="Where this contact took place."
    )
    location_raw_text = Column(
        String(255), comment="The raw text value of the contact location."
    )
    resulted_in_arrest = Column(
        Boolean, comment="Whether or not this contact resulted in the person's arrest."
    )
    status = Column(
        state_supervision_contact_status, comment="The current status of this contact."
    )
    status_raw_text = Column(
        String(255), comment="The raw text value of the contact status."
    )
    verified_employment = Column(
        Boolean,
        comment="Whether or not the person's current employment status was "
        "verified at this contact.",
    )

    @declared_attr
    def contacted_agent_id(self) -> Column:
        return Column(
            Integer,
            ForeignKey("state_agent.agent_id"),
            index=True,
            nullable=True,
            comment=StrictStringFormatter().format(
                FOREIGN_KEY_COMMENT_TEMPLATE, object_name="state agent"
            ),
        )

    contacted_agent = relationship("StateAgent", uselist=False, lazy="selectin")


class StateEmploymentPeriod(StateBase, _ReferencesStatePersonSharedColumns):
    """Represents a StateEmploymentPeriod in the SQL schema."""

    __tablename__ = "state_employment_period"
    __table_args__ = (
        UniqueConstraint(
            "state_code",
            "external_id",
            name="employment_period_external_ids_unique_within_state",
            deferrable=True,
            initially="DEFERRED",
        ),
        {
            "comment": (
                "The StateEmploymentPeriod object represents information about a "
                "person's employment status during a particular period of time. This "
                "object can be used to track employer information, or to track periods "
                "of unemployment if we have positive confirmation from the state that "
                "a person was unemployed at a given period."
            )
        },
    )

    employment_period_id = Column(
        Integer,
        primary_key=True,
        comment=StrictStringFormatter().format(
            PRIMARY_KEY_COMMENT_TEMPLATE, object_name="employment period"
        ),
    )

    external_id = Column(
        String(255),
        index=True,
        comment=StrictStringFormatter().format(
            EXTERNAL_ID_COMMENT_TEMPLATE, object_name="StateEmploymentPeriod"
        ),
    )
    state_code = Column(
        String(255),
        nullable=False,
        index=True,
        comment=STATE_CODE_COMMENT,
    )

    employment_status = Column(
        state_employment_period_employment_status,
        comment=(
            "Indicates the type of the person's employment or unemployment during the "
            "given period of time."
        ),
    )
    employment_status_raw_text = Column(
        String(255), comment="The raw text value of the employment status."
    )

    start_date = Column(
        Date,
        nullable=False,
        comment=(
            "Date on which a person’s employment with the given employer and job "
            "title started."
        ),
    )
    end_date = Column(
        Date,
        comment=(
            "Date on which a person’s employment with the given employer and job "
            "title terminated."
        ),
    )
    last_verified_date = Column(
        Date,
        comment=(
            "Most recent date on which person’s employment with a given employer and "
            "job title was verified. Note that this field is only meaningful for open "
            "employment periods."
        ),
    )

    employer_name = Column(String(255), comment="The name of the person's employer.")
    job_title = Column(String(255), comment="The name of the person's job position.")

    end_reason = Column(
        state_employment_period_end_reason,
        comment=(
            "The reason why this period of employment or unemployment was terminated. Should "
            "only be set if the `end_date` is nonnull."
        ),
    )
    end_reason_raw_text = Column(
        String(255), comment="The raw text value of the end reason."
    )


class StateDrugScreen(StateBase, _ReferencesStatePersonSharedColumns):
    """Represents a StateDrugScreen in the SQL schema."""

    __tablename__ = "state_drug_screen"
    __table_args__ = (
        UniqueConstraint(
            "state_code",
            "external_id",
            name="state_drug_screen_external_ids_unique_within_state",
            deferrable=True,
            initially="DEFERRED",
        ),
        {
            "comment": (
                "The StateDrugScreen object represents information about the results of a particular drug screen."
            )
        },
    )

    drug_screen_id = Column(
        Integer,
        primary_key=True,
        comment=StrictStringFormatter().format(
            PRIMARY_KEY_COMMENT_TEMPLATE, object_name="drug screen"
        ),
    )
    external_id = Column(
        String(255),
        index=True,
        comment=StrictStringFormatter().format(
            EXTERNAL_ID_COMMENT_TEMPLATE, object_name="StateDrugScreen"
        ),
    )
    state_code = Column(
        String(255),
        nullable=False,
        index=True,
        comment=STATE_CODE_COMMENT,
    )
    drug_screen_date = Column(
        Date,
        nullable=False,
        comment=(
            "Date the drug screen was administered. This is the date the sample was collected or a positive admission was recorded."
        ),
    )
    drug_screen_result = Column(
        state_drug_screen_result,
        comment=(
            "Enum indicating whether the test result was positive, negative or other."
        ),
    )
    drug_screen_result_raw_text = Column(
        String(255), comment="Raw text for the result field."
    )
    sample_type = Column(
        state_drug_screen_sample_type,
        comment="Type of sample collected for the drug screen.",
    )
    sample_type_raw_text = Column(
        String(255), comment="Raw text for the sample_type field."
    )
