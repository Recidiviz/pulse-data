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

NOTE: Many of the tables in the below schema are historical tables. The primary
key of a historical table exists only due to the requirements of SQLAlchemy,
and should not be referenced by any other table. The key which should be used
to reference a historical table is the key shared with the master table. For
the historical table, this key is non-unique. This is necessary to allow the
desired temporal table behavior. Because of this, any foreign key column on a
historical table must point to the *master* table (which has a unique key), not
the historical table (which does not). Because the key is shared between the
master and historical tables, this allows an indirect guarantee of referential
integrity to the historical tables as well.
"""
from typing import TypeVar, Any

from sqlalchemy import (
    Boolean,
    Column,
    Date,
    Enum,
    ForeignKey,
    Integer,
    String,
    Text,
    Table,
    UniqueConstraint,
)
from sqlalchemy.ext.declarative import declared_attr
from sqlalchemy.orm import relationship

from recidiviz.common.constants.state import (
    enum_canonical_strings as state_enum_strings,
)
import recidiviz.common.constants.enum_canonical_strings as enum_strings
from recidiviz.persistence.database.schema.history_table_shared_columns_mixin import (
    HistoryTableSharedColumns,
)

from recidiviz.persistence.database.schema.shared_enums import (
    gender,
    race,
    ethnicity,
    residency_status,
    bond_status,
    bond_type,
    charge_status,
)

# SQLAlchemy enums. Created separately from the tables so they can be shared
# between the master and historical tables for each entity.
from recidiviz.persistence.database.base_schema import StateBase

state_assessment_class = Enum(
    state_enum_strings.state_assessment_class_mental_health,
    state_enum_strings.state_assessment_class_risk,
    state_enum_strings.state_assessment_class_security_classification,
    state_enum_strings.state_assessment_class_sex_offense,
    state_enum_strings.state_assessment_class_social,
    state_enum_strings.state_assessment_class_substance_abuse,
    name="state_assessment_class",
)

state_assessment_type = Enum(
    state_enum_strings.state_assessment_type_asi,
    state_enum_strings.state_assessment_type_cssm,
    state_enum_strings.state_assessment_type_hiq,
    state_enum_strings.state_assessment_type_lsir,
    state_enum_strings.state_assessment_type_oras,
    state_enum_strings.state_assessment_type_pa_rst,
    state_enum_strings.state_assessment_type_psa,
    state_enum_strings.state_assessment_type_sorac,
    state_enum_strings.state_assessment_type_static_99,
    state_enum_strings.state_assessment_type_tcu_drug_screen,
    state_enum_strings.state_assessment_type_oras_community_supervision,
    state_enum_strings.state_assessment_type_oras_community_supervision_screening,
    state_enum_strings.state_assessment_type_oras_misdemeanor_assessment,
    state_enum_strings.state_assessment_type_oras_misdemeanor_screening,
    state_enum_strings.state_assessment_type_oras_pre_trial,
    state_enum_strings.state_assessment_type_oras_prison_screening,
    state_enum_strings.state_assessment_type_oras_prison_intake,
    state_enum_strings.state_assessment_type_oras_reentry,
    state_enum_strings.state_assessment_type_oras_static,
    state_enum_strings.state_assessment_type_oras_supplemental_reentry,
    enum_strings.internal_unknown,
    name="state_assessment_type",
)

state_assessment_level = Enum(
    enum_strings.external_unknown,
    state_enum_strings.state_assessment_level_low,
    state_enum_strings.state_assessment_level_low_medium,
    state_enum_strings.state_assessment_level_medium,
    state_enum_strings.state_assessment_level_medium_high,
    state_enum_strings.state_assessment_level_moderate,
    state_enum_strings.state_assessment_level_high,
    state_enum_strings.state_assessment_level_very_high,
    name="state_assessment_level",
)

state_sentence_status = Enum(
    state_enum_strings.state_sentence_status_commuted,
    state_enum_strings.state_sentence_status_completed,
    enum_strings.external_unknown,
    state_enum_strings.state_sentence_status_pardoned,
    enum_strings.present_without_info,
    state_enum_strings.state_sentence_status_serving,
    state_enum_strings.state_sentence_status_suspended,
    state_enum_strings.state_sentence_status_revoked,
    state_enum_strings.state_sentence_status_vacated,
    name="state_sentence_status",
)

state_supervision_type = Enum(
    state_enum_strings.state_supervision_type_civil_commitment,
    enum_strings.internal_unknown,
    enum_strings.external_unknown,
    state_enum_strings.state_supervision_type_halfway_house,
    state_enum_strings.state_supervision_type_parole,
    state_enum_strings.state_supervision_type_post_confinement,
    state_enum_strings.state_supervision_type_pre_confinement,
    state_enum_strings.state_supervision_type_probation,
    name="state_supervision_type",
)

state_supervision_case_type = Enum(
    state_enum_strings.state_supervision_case_type_alcohol_drug,
    state_enum_strings.state_supervision_case_type_domestic_violence,
    state_enum_strings.state_supervision_case_type_drug_court,
    state_enum_strings.state_supervision_case_type_family_court,
    state_enum_strings.state_supervision_case_type_general,
    state_enum_strings.state_supervision_case_type_mental_health_court,
    state_enum_strings.state_supervision_case_type_serious_mental_illness,
    state_enum_strings.state_supervision_case_type_sex_offense,
    state_enum_strings.state_supervision_case_type_veterans_court,
    name="state_supervision_case_type",
)

state_charge_classification_type = Enum(
    state_enum_strings.state_charge_classification_type_civil,
    enum_strings.external_unknown,
    state_enum_strings.state_charge_classification_type_felony,
    state_enum_strings.state_charge_classification_type_infraction,
    state_enum_strings.state_charge_classification_type_misdemeanor,
    state_enum_strings.state_charge_classification_type_other,
    name="state_charge_classification_type",
)

state_fine_status = Enum(
    enum_strings.external_unknown,
    state_enum_strings.state_fine_status_paid,
    enum_strings.present_without_info,
    state_enum_strings.state_fine_status_unpaid,
    name="state_fine_status",
)

state_incarceration_type = Enum(
    enum_strings.external_unknown,
    state_enum_strings.state_incarceration_type_county_jail,
    state_enum_strings.state_incarceration_type_federal_prison,
    state_enum_strings.state_incarceration_type_out_of_state,
    state_enum_strings.state_incarceration_type_state_prison,
    name="state_incarceration_type",
)

state_court_case_status = Enum(
    enum_strings.external_unknown,
    enum_strings.present_without_info,
    # TODO(#1697): Add values here
    name="state_court_case_status",
)

state_court_type = Enum(
    enum_strings.present_without_info,
    # TODO(#1697): Add values here,
    name="state_court_type",
)

state_agent_type = Enum(
    enum_strings.present_without_info,
    state_enum_strings.state_agent_correctional_officer,
    state_enum_strings.state_agent_judge,
    state_enum_strings.state_agent_parole_board_member,
    state_enum_strings.state_agent_supervision_officer,
    state_enum_strings.state_agent_unit_supervisor,
    enum_strings.internal_unknown,
    name="state_agent_type",
)

state_person_alias_type = Enum(
    state_enum_strings.state_person_alias_alias_type_affiliation_name,
    state_enum_strings.state_person_alias_alias_type_alias,
    state_enum_strings.state_person_alias_alias_type_given_name,
    state_enum_strings.state_person_alias_alias_type_maiden_name,
    state_enum_strings.state_person_alias_alias_type_nickname,
    name="state_person_alias_type",
)

state_incarceration_period_status = Enum(
    enum_strings.external_unknown,
    state_enum_strings.state_incarceration_period_status_in_custody,
    state_enum_strings.state_incarceration_period_status_not_in_custody,
    enum_strings.present_without_info,
    name="state_incarceration_period_status",
)

state_incarceration_facility_security_level = Enum(
    state_enum_strings.state_incarceration_facility_security_level_maximum,
    state_enum_strings.state_incarceration_facility_security_level_medium,
    state_enum_strings.state_incarceration_facility_security_level_minimum,
    name="state_incarceration_facility_security_level",
)

state_incarceration_period_admission_reason = Enum(
    state_enum_strings.state_incarceration_period_admission_reason_admitted_in_error,
    enum_strings.external_unknown,
    enum_strings.internal_unknown,
    state_enum_strings.state_incarceration_period_admission_reason_new_admission,
    state_enum_strings.state_incarceration_period_admission_reason_parole_revocation,
    state_enum_strings.state_incarceration_period_admission_reason_probation_revocation,
    state_enum_strings.state_incarceration_period_admission_reason_dual_revocation,
    state_enum_strings.state_incarceration_period_admission_reason_return_from_erroneous_release,
    state_enum_strings.state_incarceration_period_admission_reason_return_from_escape,
    state_enum_strings.state_incarceration_period_admission_reason_return_from_supervision,
    state_enum_strings.state_incarceration_period_admission_reason_temporary_custody,
    state_enum_strings.state_incarceration_period_admission_reason_transfer,
    state_enum_strings.state_incarceration_period_admission_reason_transferred_from_out_of_state,
    state_enum_strings.state_incarceration_period_admission_reason_status_change,
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
    enum_strings.external_unknown,
    enum_strings.internal_unknown,
    state_enum_strings.state_incarceration_period_release_reason_pardoned,
    state_enum_strings.state_incarceration_period_release_reason_released_from_erroneous_admission,
    state_enum_strings.state_incarceration_period_release_reason_released_from_temporary_custody,
    state_enum_strings.state_incarceration_period_release_reason_released_in_error,
    state_enum_strings.state_incarceration_period_release_reason_sentence_served,
    state_enum_strings.state_incarceration_period_release_reason_transfer,
    state_enum_strings.state_incarceration_period_release_reason_transfer_out_of_state,
    state_enum_strings.state_incarceration_period_release_reason_vacated,
    state_enum_strings.state_incarceration_period_release_reason_status_change,
    name="state_incarceration_period_release_reason",
)

state_supervision_period_status = Enum(
    enum_strings.external_unknown,
    state_enum_strings.state_supervision_period_status_terminated,
    state_enum_strings.state_supervision_period_status_under_supervision,
    enum_strings.present_without_info,
    name="state_supervision_period_status",
)

state_supervision_period_admission_reason = Enum(
    enum_strings.external_unknown,
    enum_strings.internal_unknown,
    state_enum_strings.state_supervision_period_admission_reason_absconsion,
    state_enum_strings.state_supervision_period_admission_reason_conditional_release,
    state_enum_strings.state_supervision_period_admission_reason_court_sentence,
    state_enum_strings.state_supervision_period_admission_reason_investigation,
    state_enum_strings.state_supervision_period_admission_reason_transfer_out_of_state,
    state_enum_strings.state_supervision_period_admission_reason_transfer_within_state,
    state_enum_strings.state_supervision_period_admission_reason_return_from_absconsion,
    state_enum_strings.state_supervision_period_admission_reason_return_from_suspension,
    name="state_supervision_period_admission_reason",
)

state_supervision_level = Enum(
    enum_strings.external_unknown,
    enum_strings.internal_unknown,
    enum_strings.present_without_info,
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
    name="state_supervision_level",
)

state_supervision_period_termination_reason = Enum(
    enum_strings.external_unknown,
    enum_strings.internal_unknown,
    state_enum_strings.state_supervision_period_termination_reason_absconsion,
    state_enum_strings.state_supervision_period_termination_reason_commuted,
    state_enum_strings.state_supervision_period_termination_reason_death,
    state_enum_strings.state_supervision_period_termination_reason_discharge,
    state_enum_strings.state_supervision_period_termination_reason_dismissed,
    state_enum_strings.state_supervision_period_termination_reason_expiration,
    state_enum_strings.state_supervision_period_termination_reason_investigation,
    state_enum_strings.state_supervision_period_termination_reason_pardoned,
    state_enum_strings.state_supervision_period_termination_reason_transfer_out_of_state,
    state_enum_strings.state_supervision_period_termination_reason_transfer_within_state,
    state_enum_strings.state_supervision_period_termination_reason_return_from_absconsion,
    state_enum_strings.state_supervision_period_termination_reason_return_to_incarceration,
    state_enum_strings.state_supervision_period_termination_reason_revocation,
    state_enum_strings.state_supervision_period_termination_reason_suspension,
    name="state_supervision_period_termination_reason",
)

state_supervision_period_supervision_type = Enum(
    enum_strings.external_unknown,
    enum_strings.internal_unknown,
    state_enum_strings.state_supervision_period_supervision_type_informal_probation,
    state_enum_strings.state_supervision_period_supervision_type_investigation,
    state_enum_strings.state_supervision_period_supervision_type_parole,
    state_enum_strings.state_supervision_period_supervision_type_probation,
    state_enum_strings.state_supervision_period_supervision_type_dual,
    name="state_supervision_period_supervision_type",
)

state_incarceration_incident_type = Enum(
    enum_strings.present_without_info,
    state_enum_strings.state_incarceration_incident_type_contraband,
    state_enum_strings.state_incarceration_incident_type_disorderly_conduct,
    state_enum_strings.state_incarceration_incident_type_escape,
    state_enum_strings.state_incarceration_incident_type_minor_offense,
    state_enum_strings.state_incarceration_incident_type_positive,
    state_enum_strings.state_incarceration_incident_type_report,
    state_enum_strings.state_incarceration_incident_type_violence,
    name="state_incarceration_incident_type",
)

state_incarceration_incident_outcome_type = Enum(
    state_enum_strings.state_incarceration_incident_outcome_cell_confinement,
    state_enum_strings.state_incarceration_incident_outcome_disciplinary_labor,
    state_enum_strings.state_incarceration_incident_outcome_dismissed,
    state_enum_strings.state_incarceration_incident_outcome_external_prosecution,
    state_enum_strings.state_incarceration_incident_outcome_financial_penalty,
    state_enum_strings.state_incarceration_incident_outcome_good_time_loss,
    state_enum_strings.state_incarceration_incident_outcome_miscellaneous,
    state_enum_strings.state_incarceration_incident_outcome_not_guilty,
    state_enum_strings.state_incarceration_incident_outcome_privilege_loss,
    state_enum_strings.state_incarceration_incident_outcome_restricted_confinement,
    state_enum_strings.state_incarceration_incident_outcome_solitary,
    state_enum_strings.state_incarceration_incident_outcome_treatment,
    state_enum_strings.state_incarceration_incident_outcome_warning,
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
    name="state_supervision_violation_type",
)

state_supervision_violation_response_type = Enum(
    state_enum_strings.state_supervision_violation_response_type_citation,
    state_enum_strings.state_supervision_violation_response_type_violation_report,
    state_enum_strings.state_supervision_violation_response_type_permanent_decision,
    name="state_supervision_violation_response_type",
)

state_supervision_violation_response_decision = Enum(
    state_enum_strings.state_supervision_violation_response_decision_community_service,
    state_enum_strings.state_supervision_violation_response_decision_continuance,
    state_enum_strings.state_supervision_violation_response_decision_delayed_action,
    state_enum_strings.state_supervision_violation_response_decision_extension,
    enum_strings.internal_unknown,
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
    state_enum_strings.state_supervision_violation_response_decision_warning,
    state_enum_strings.state_supervision_violation_response_decision_warrant_issued,
    name="state_supervision_violation_response_decision",
)

state_supervision_violation_response_revocation_type = Enum(
    state_enum_strings.state_supervision_violation_response_revocation_type_reincarceration,
    state_enum_strings.state_supervision_violation_response_revocation_type_return_to_supervision,
    state_enum_strings.state_supervision_violation_response_revocation_type_shock_incarceration,
    state_enum_strings.state_supervision_violation_response_revocation_type_treatment_in_prison,
    name="state_supervision_violation_response_revocation_type",
)

state_supervision_violation_response_deciding_body_type = Enum(
    state_enum_strings.state_supervision_violation_response_deciding_body_type_court,
    state_enum_strings.state_supervision_violation_response_deciding_body_parole_board,
    state_enum_strings.state_supervision_violation_response_deciding_body_type_supervision_officer,
    name="state_supervision_violation_response_deciding_body_type",
)

state_parole_decision_outcome = Enum(
    enum_strings.external_unknown,
    state_enum_strings.state_parole_decision_parole_denied,
    state_enum_strings.state_parole_decision_parole_granted,
    name="state_parole_decision_outcome",
)
state_program_assignment_participation_status = Enum(
    enum_strings.external_unknown,
    enum_strings.present_without_info,
    state_enum_strings.state_program_assignment_participation_status_denied,
    state_enum_strings.state_program_assignment_participation_status_discharged,
    state_enum_strings.state_program_assignment_participation_status_in_progress,
    state_enum_strings.state_program_assignment_participation_status_pending,
    state_enum_strings.state_program_assignment_participation_status_refused,
    name="state_program_assignment_participation_status",
)

state_program_assignment_discharge_reason = Enum(
    enum_strings.external_unknown,
    state_enum_strings.state_program_assignment_discharge_reason_absconded,
    state_enum_strings.state_program_assignment_discharge_reason_adverse_termination,
    state_enum_strings.state_program_assignment_discharge_reason_completed,
    state_enum_strings.state_program_assignment_discharge_reason_moved,
    state_enum_strings.state_program_assignment_discharge_reason_opted_out,
    state_enum_strings.state_program_assignment_discharge_reason_program_transfer,
    state_enum_strings.state_program_assignment_discharge_reason_reincarcerated,
    name="state_program_assignment_discharge_reason",
)

state_specialized_purpose_for_incarceration = Enum(
    enum_strings.external_unknown,
    enum_strings.internal_unknown,
    state_enum_strings.state_specialized_purpose_for_incarceration_general,
    state_enum_strings.state_specialized_purpose_for_incarceration_parole_board_hold,
    state_enum_strings.state_specialized_purpose_for_incarceration_shock_incarceration,
    state_enum_strings.state_specialized_purpose_for_incarceration_treatment_in_prison,
    state_enum_strings.state_specialized_purpose_for_incarceration_temporary_custody,
    name="state_specialized_purpose_for_incarceration",
)

state_early_discharge_decision = Enum(
    state_enum_strings.state_early_discharge_decision_request_denied,
    state_enum_strings.state_early_discharge_decision_sentence_termination_granted,
    state_enum_strings.state_early_discharge_decision_unsupervised_probation_granted,
    name="state_early_discharge_decision",
)

state_early_discharge_decision_status = Enum(
    state_enum_strings.state_early_discharge_decision_status_pending,
    state_enum_strings.state_early_discharge_decision_status_decided,
    state_enum_strings.state_early_discharge_decision_status_invalid,
    name="state_early_discharge_decision_status",
)

state_acting_body_type = Enum(
    state_enum_strings.state_acting_body_type_court,
    state_enum_strings.state_acting_body_type_parole_board,
    state_enum_strings.state_acting_body_type_sentenced_person,
    state_enum_strings.state_acting_body_type_supervision_officer,
    name="state_acting_body_type",
)

state_custodial_authority = Enum(
    state_enum_strings.state_custodial_authority_federal,
    state_enum_strings.state_custodial_authority_other_country,
    state_enum_strings.state_custodial_authority_other_state,
    state_enum_strings.state_custodial_authority_supervision_authority,
    state_enum_strings.state_custodial_authority_state_prison,
    name="state_custodial_authority",
)

state_supervision_contact_location = Enum(
    enum_strings.internal_unknown,
    enum_strings.external_unknown,
    state_enum_strings.state_supervision_contact_location_court,
    state_enum_strings.state_supervision_contact_location_field,
    state_enum_strings.state_supervision_contact_location_jail,
    state_enum_strings.state_supervision_contact_location_place_of_employment,
    state_enum_strings.state_supervision_contact_location_residence,
    state_enum_strings.state_supervision_contact_location_supervision_office,
    state_enum_strings.state_supervision_contact_location_treatment_provider,
    name="state_supervision_contact_location",
)

state_supervision_contact_status = Enum(
    enum_strings.internal_unknown,
    enum_strings.external_unknown,
    state_enum_strings.state_supervision_contact_status_attempted,
    state_enum_strings.state_supervision_contact_status_completed,
    name="state_supervision_contact_status",
)

state_supervision_contact_reason = Enum(
    enum_strings.internal_unknown,
    enum_strings.external_unknown,
    state_enum_strings.state_supervision_contact_reason_emergency_contact,
    state_enum_strings.state_supervision_contact_reason_general_contact,
    state_enum_strings.state_supervision_contact_reason_initial_contact,
    name="state_supervision_contact_reason",
)

state_supervision_contact_type = Enum(
    enum_strings.internal_unknown,
    enum_strings.external_unknown,
    state_enum_strings.state_supervision_contact_type_face_to_face,
    state_enum_strings.state_supervision_contact_type_telephone,
    state_enum_strings.state_supervision_contact_type_written_message,
    state_enum_strings.state_supervision_contact_type_virtual,
    name="state_supervision_contact_type",
)

# Join tables

state_supervision_sentence_incarceration_period_association_table = Table(
    "state_supervision_sentence_incarceration_period_association",
    StateBase.metadata,
    Column(
        "supervision_sentence_id",
        Integer,
        ForeignKey("state_supervision_sentence.supervision_sentence_id"),
        index=True,
    ),
    Column(
        "incarceration_period_id",
        Integer,
        ForeignKey("state_incarceration_period.incarceration_period_id"),
        index=True,
    ),
)

state_supervision_sentence_supervision_period_association_table = Table(
    "state_supervision_sentence_supervision_period_association",
    StateBase.metadata,
    Column(
        "supervision_sentence_id",
        Integer,
        ForeignKey("state_supervision_sentence.supervision_sentence_id"),
        index=True,
    ),
    Column(
        "supervision_period_id",
        Integer,
        ForeignKey("state_supervision_period.supervision_period_id"),
        index=True,
    ),
)

state_incarceration_sentence_incarceration_period_association_table = Table(
    "state_incarceration_sentence_incarceration_period_association",
    StateBase.metadata,
    Column(
        "incarceration_sentence_id",
        Integer,
        ForeignKey("state_incarceration_sentence.incarceration_sentence_id"),
        index=True,
    ),
    Column(
        "incarceration_period_id",
        Integer,
        ForeignKey("state_incarceration_period.incarceration_period_id"),
        index=True,
    ),
)

state_incarceration_sentence_supervision_period_association_table = Table(
    "state_incarceration_sentence_supervision_period_association",
    StateBase.metadata,
    Column(
        "incarceration_sentence_id",
        Integer,
        ForeignKey("state_incarceration_sentence.incarceration_sentence_id"),
        index=True,
    ),
    Column(
        "supervision_period_id",
        Integer,
        ForeignKey("state_supervision_period.supervision_period_id"),
        index=True,
    ),
)

state_supervision_period_supervision_violation_association_table = Table(
    "state_supervision_period_supervision_violation_association",
    StateBase.metadata,
    Column(
        "supervision_period_id",
        Integer,
        ForeignKey("state_supervision_period.supervision_period_id"),
        index=True,
    ),
    Column(
        "supervision_violation_id",
        Integer,
        ForeignKey("state_supervision_violation.supervision_violation_id"),
        index=True,
    ),
)

state_supervision_period_program_assignment_association_table = Table(
    "state_supervision_period_program_assignment_association",
    StateBase.metadata,
    Column(
        "supervision_period_id",
        Integer,
        ForeignKey("state_supervision_period.supervision_period_id"),
        index=True,
    ),
    Column(
        "program_assignment_id",
        Integer,
        ForeignKey("state_program_assignment.program_assignment_id"),
        index=True,
    ),
)

state_supervision_period_supervision_contact_association_table = Table(
    "state_supervision_period_supervision_contact_association",
    StateBase.metadata,
    Column(
        "supervision_period_id",
        Integer,
        ForeignKey("state_supervision_period.supervision_period_id"),
        index=True,
    ),
    Column(
        "supervision_contact_id",
        Integer,
        ForeignKey("state_supervision_contact.supervision_contact_id"),
        index=True,
    ),
)

state_incarceration_period_program_assignment_association_table = Table(
    "state_incarceration_period_program_assignment_association",
    StateBase.metadata,
    Column(
        "incarceration_period_id",
        Integer,
        ForeignKey("state_incarceration_period.incarceration_period_id"),
        index=True,
    ),
    Column(
        "program_assignment_id",
        Integer,
        ForeignKey("state_program_assignment.program_assignment_id"),
        index=True,
    ),
)

state_charge_incarceration_sentence_association_table = Table(
    "state_charge_incarceration_sentence_association",
    StateBase.metadata,
    Column("charge_id", Integer, ForeignKey("state_charge.charge_id"), index=True),
    Column(
        "incarceration_sentence_id",
        Integer,
        ForeignKey("state_incarceration_sentence.incarceration_sentence_id"),
        index=True,
    ),
)

state_charge_supervision_sentence_association_table = Table(
    "state_charge_supervision_sentence_association",
    StateBase.metadata,
    Column("charge_id", Integer, ForeignKey("state_charge.charge_id"), index=True),
    Column(
        "supervision_sentence_id",
        Integer,
        ForeignKey("state_supervision_sentence.supervision_sentence_id"),
        index=True,
    ),
)

state_charge_fine_association_table = Table(
    "state_charge_fine_association",
    StateBase.metadata,
    Column("charge_id", Integer, ForeignKey("state_charge.charge_id"), index=True),
    Column("fine_id", Integer, ForeignKey("state_fine.fine_id"), index=True),
)

state_parole_decision_decision_agent_association_table = Table(
    "state_parole_decision_decision_agent_association",
    StateBase.metadata,
    Column(
        "parole_decision_id",
        Integer,
        ForeignKey("state_parole_decision.parole_decision_id"),
        index=True,
    ),
    Column("agent_id", Integer, ForeignKey("state_agent.agent_id"), index=True),
)

state_supervision_violation_response_decision_agent_association_table = Table(
    "state_supervision_violation_response_decision_agent_association",
    StateBase.metadata,
    Column(
        "supervision_violation_response_id",
        Integer,
        ForeignKey(
            "state_supervision_violation_response." "supervision_violation_response_id"
        ),
        index=True,
    ),
    Column("agent_id", Integer, ForeignKey("state_agent.agent_id"), index=True),
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
        )


class _ReferencesStateSentenceGroupSharedColumns:
    """A mixin which defines columns for any table whose rows reference an
    individual StateSentenceGroup"""

    # Consider this class a mixin and only allow instantiating subclasses
    def __new__(
        cls, *_: Any, **__: Any
    ) -> "_ReferencesStateSentenceGroupSharedColumns":
        if cls is _ReferencesStateSentenceGroupSharedColumns:
            raise Exception(f"[{cls}] cannot be instantiated")
        return super().__new__(cls)  # type: ignore

    @declared_attr
    def sentence_group_id(self) -> Column:
        return Column(
            Integer,
            ForeignKey(
                "state_sentence_group.sentence_group_id",
                deferrable=True,
                initially="DEFERRED",
            ),
            index=True,
            nullable=False,
        )


# StatePersonExternalId


class _StatePersonExternalIdSharedColumns(_ReferencesStatePersonSharedColumns):
    """A mixin which defines all columns common to StatePersonExternalId and
    StatePersonExternalIdHistory"""

    # Consider this class a mixin and only allow instantiating subclasses
    def __new__(cls, *_: Any, **__: Any) -> "_StatePersonExternalIdSharedColumns":
        if cls is _StatePersonExternalIdSharedColumns:
            raise Exception(f"[{cls}] cannot be instantiated")
        return super().__new__(cls)  # type: ignore

    external_id = Column(String(255), nullable=False, index=True)
    state_code = Column(String(255), nullable=False, index=True)
    id_type = Column(String(255), nullable=False)


class StatePersonExternalId(StateBase, _StatePersonExternalIdSharedColumns):
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
    )

    person_external_id_id = Column(Integer, primary_key=True)


class StatePersonExternalIdHistory(
    StateBase, _StatePersonExternalIdSharedColumns, HistoryTableSharedColumns
):
    """Represents the historical state of a StatePersonExternalId"""

    __tablename__ = "state_person_external_id_history"

    # This primary key should NOT be used. It only exists because SQLAlchemy
    # requires every table to have a unique primary key.
    person_external_id_history_id = Column(Integer, primary_key=True)

    person_external_id_id = Column(
        Integer,
        ForeignKey("state_person_external_id.person_external_id_id"),
        nullable=False,
        index=True,
    )


# StatePersonAlias


class _StatePersonAliasSharedColumns(_ReferencesStatePersonSharedColumns):
    """A mixin which defines all columns common to StatePersonAlias and
    StatePersonAliasHistory
    """

    # Consider this class a mixin and only allow instantiating subclasses
    def __new__(cls, *_: Any, **__: Any) -> "_StatePersonAliasSharedColumns":
        if cls is _StatePersonAliasSharedColumns:
            raise Exception(f"[{cls}] cannot be instantiated")
        return super().__new__(cls)  # type: ignore

    state_code = Column(String(255), nullable=False, index=True)
    full_name = Column(String(255))
    alias_type = Column(state_person_alias_type)
    alias_type_raw_text = Column(String(255))


class StatePersonAlias(StateBase, _StatePersonAliasSharedColumns):
    """Represents a StatePersonAlias in the SQL schema"""

    __tablename__ = "state_person_alias"

    person_alias_id = Column(Integer, primary_key=True)


class StatePersonAliasHistory(
    StateBase, _StatePersonAliasSharedColumns, HistoryTableSharedColumns
):
    """Represents the historical state of a StatePersonAlias"""

    __tablename__ = "state_person_alias_history"

    # This primary key should NOT be used. It only exists because SQLAlchemy
    # requires every table to have a unique primary key.
    person_alias_history_id = Column(Integer, primary_key=True)

    person_alias_id = Column(
        Integer,
        ForeignKey("state_person_alias.person_alias_id"),
        nullable=False,
        index=True,
    )


# StatePersonRace


class _StatePersonRaceSharedColumns(_ReferencesStatePersonSharedColumns):
    """A mixin which defines all columns common to StatePersonRace and
    StatePersonRaceHistory"""

    # Consider this class a mixin and only allow instantiating subclasses
    def __new__(cls, *_: Any, **__: Any) -> "_StatePersonRaceSharedColumns":
        if cls is _StatePersonRaceSharedColumns:
            raise Exception(f"[{cls}] cannot be instantiated")
        return super().__new__(cls)  # type: ignore

    state_code = Column(String(255), nullable=False, index=True)
    race = Column(race)
    race_raw_text = Column(String(255))


class StatePersonRace(StateBase, _StatePersonRaceSharedColumns):
    """Represents a StatePersonRace in the SQL schema"""

    __tablename__ = "state_person_race"

    person_race_id = Column(Integer, primary_key=True)


class StatePersonRaceHistory(
    StateBase, _StatePersonRaceSharedColumns, HistoryTableSharedColumns
):
    """Represents the historical state of a StatePersonRace"""

    __tablename__ = "state_person_race_history"

    # This primary key should NOT be used. It only exists because SQLAlchemy
    # requires every table to have a unique primary key.
    person_race_history_id = Column(Integer, primary_key=True)

    person_race_id = Column(
        Integer,
        ForeignKey("state_person_race.person_race_id"),
        nullable=False,
        index=True,
    )


# StatePersonEthnicity


class _StatePersonEthnicitySharedColumns(_ReferencesStatePersonSharedColumns):
    """A mixin which defines all columns common to StatePersonEthnicity and
    StatePersonEthnicityHistory"""

    # Consider this class a mixin and only allow instantiating subclasses
    def __new__(cls, *_: Any, **__: Any) -> "_StatePersonEthnicitySharedColumns":
        if cls is _StatePersonEthnicitySharedColumns:
            raise Exception(f"[{cls}] cannot be instantiated")
        return super().__new__(cls)  # type: ignore

    state_code = Column(String(255), nullable=False, index=True)
    ethnicity = Column(ethnicity)
    ethnicity_raw_text = Column(String(255))


class StatePersonEthnicity(StateBase, _StatePersonEthnicitySharedColumns):
    """Represents a state person in the SQL schema"""

    __tablename__ = "state_person_ethnicity"

    person_ethnicity_id = Column(Integer, primary_key=True)


class StatePersonEthnicityHistory(
    StateBase, _StatePersonEthnicitySharedColumns, HistoryTableSharedColumns
):
    """Represents the historical state of a state person ethnicity"""

    __tablename__ = "state_person_ethnicity_history"

    # This primary key should NOT be used. It only exists because SQLAlchemy
    # requires every table to have a unique primary key.
    person_ethnicity_history_id = Column(Integer, primary_key=True)

    person_ethnicity_id = Column(
        Integer,
        ForeignKey("state_person_ethnicity.person_ethnicity_id"),
        nullable=False,
        index=True,
    )


# StatePerson


class _StatePersonSharedColumns:
    """A mixin which defines all columns common to StatePerson and
    StatePersonHistory"""

    # Consider this class a mixin and only allow instantiating subclasses
    def __new__(cls, *_: Any, **__: Any) -> "_StatePersonSharedColumns":
        if cls is _StatePersonSharedColumns:
            raise Exception(f"[{cls}] cannot be instantiated")
        return super().__new__(cls)  # type: ignore

    state_code = Column(String(255), nullable=False, index=True)

    current_address = Column(Text)

    full_name = Column(String(255), index=True)

    birthdate = Column(Date, index=True)
    birthdate_inferred_from_age = Column(Boolean)

    gender = Column(gender)
    gender_raw_text = Column(String(255))

    residency_status = Column(residency_status)

    @declared_attr
    def supervising_officer_id(self) -> Column:
        return Column(
            Integer, ForeignKey("state_agent.agent_id"), index=True, nullable=True
        )


class StatePerson(StateBase, _StatePersonSharedColumns):
    """Represents a StatePerson in the state SQL schema"""

    __tablename__ = "state_person"

    person_id = Column(Integer, primary_key=True)

    external_ids = relationship(
        "StatePersonExternalId", backref="person", lazy="selectin"
    )
    aliases = relationship("StatePersonAlias", backref="person", lazy="selectin")
    races = relationship("StatePersonRace", backref="person", lazy="selectin")
    ethnicities = relationship(
        "StatePersonEthnicity", backref="person", lazy="selectin"
    )
    assessments = relationship("StateAssessment", backref="person", lazy="selectin")
    program_assignments = relationship(
        "StateProgramAssignment", backref="person", lazy="selectin"
    )
    sentence_groups = relationship(
        "StateSentenceGroup", backref="person", lazy="selectin"
    )
    supervising_officer = relationship("StateAgent", uselist=False, lazy="selectin")


class StatePersonHistory(
    StateBase, _StatePersonSharedColumns, HistoryTableSharedColumns
):
    """Represents the historical state of a StatePerson"""

    __tablename__ = "state_person_history"

    # This primary key should NOT be used. It only exists because SQLAlchemy
    # requires every table to have a unique primary key.
    person_history_id = Column(Integer, primary_key=True)

    person_id = Column(
        Integer, ForeignKey("state_person.person_id"), nullable=False, index=True
    )


# StateBond


class _StateBondSharedColumns(_ReferencesStatePersonSharedColumns):
    """A mixin which defines all columns common to StateBond and
    StateBond"""

    # Consider this class a mixin and only allow instantiating subclasses
    def __new__(cls, *_: Any, **__: Any) -> "_StateBondSharedColumns":
        if cls is _StateBondSharedColumns:
            raise Exception(f"[{cls}] cannot be instantiated")
        return super().__new__(cls)  # type: ignore

    external_id = Column(String(255), index=True)
    status = Column(bond_status, nullable=False)
    status_raw_text = Column(String(255))
    bond_type = Column(bond_type, nullable=False)
    bond_type_raw_text = Column(String(255))
    date_paid = Column(Date)
    state_code = Column(String(255), nullable=False, index=True)
    county_code = Column(String(255), index=True)
    amount_dollars = Column(Integer)
    bond_agent = Column(String(255))


class StateBond(StateBase, _StateBondSharedColumns):
    """Represents a StateBond in the SQL schema"""

    __tablename__ = "state_bond"
    __table_args__ = (
        UniqueConstraint(
            "state_code",
            "external_id",
            name="bond_external_ids_unique_within_state",
            deferrable=True,
            initially="DEFERRED",
        ),
    )
    bond_id = Column(Integer, primary_key=True)

    person = relationship("StatePerson", uselist=False)


class StateBondHistory(StateBase, _StateBondSharedColumns, HistoryTableSharedColumns):
    """Represents the historical state of a StateBond"""

    __tablename__ = "state_bond_history"

    # This primary key should NOT be used. It only exists because SQLAlchemy
    # requires every table to have a unique primary key.
    bond_history_id = Column(Integer, primary_key=True)

    bond_id = Column(
        Integer, ForeignKey("state_bond.bond_id"), nullable=False, index=True
    )


# StateCourtCase


class _StateCourtCaseSharedColumns(_ReferencesStatePersonSharedColumns):
    """A mixin which defines all columns common to StateCourtCase and
    StateCourtCaseHistory
    """

    # Consider this class a mixin and only allow instantiating subclasses
    def __new__(cls, *_: Any, **__: Any) -> "_StateCourtCaseSharedColumns":
        if cls is _StateCourtCaseSharedColumns:
            raise Exception(f"[{cls}] cannot be instantiated")
        return super().__new__(cls)  # type: ignore

    external_id = Column(String(255), index=True)
    status = Column(state_court_case_status)
    status_raw_text = Column(String(255))
    court_type = Column(state_court_type)
    court_type_raw_text = Column(String(255))
    date_convicted = Column(Date)
    next_court_date = Column(Date)
    state_code = Column(String(255), nullable=False, index=True)
    county_code = Column(String(255), index=True)
    judicial_district_code = Column(String(255))
    court_fee_dollars = Column(Integer)

    @declared_attr
    def judge_id(self) -> Column:
        return Column(
            Integer, ForeignKey("state_agent.agent_id"), index=True, nullable=True
        )


class StateCourtCase(StateBase, _StateCourtCaseSharedColumns):
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
    )

    court_case_id = Column(Integer, primary_key=True)
    person = relationship("StatePerson", uselist=False)
    judge = relationship("StateAgent", uselist=False, lazy="selectin")


class StateCourtCaseHistory(
    StateBase, _StateCourtCaseSharedColumns, HistoryTableSharedColumns
):
    """Represents the historical state of a StateCourtCase"""

    __tablename__ = "state_court_case_history"

    # This primary key should NOT be used. It only exists because SQLAlchemy
    # requires every table to have a unique primary key.
    court_case_history_id = Column(Integer, primary_key=True)

    court_case_id = Column(
        Integer,
        ForeignKey("state_court_case.court_case_id"),
        nullable=False,
        index=True,
    )


# StateCharge


class _StateChargeSharedColumns(_ReferencesStatePersonSharedColumns):
    """A mixin which defines all columns common to StateCharge and
    StateChargeHistory"""

    # Consider this class a mixin and only allow instantiating subclasses
    def __new__(cls, *_: Any, **__: Any) -> "_StateChargeSharedColumns":
        if cls is _StateChargeSharedColumns:
            raise Exception(f"[{cls}] cannot be instantiated")
        return super().__new__(cls)  # type: ignore

    external_id = Column(String(255), index=True)
    status = Column(charge_status, nullable=False)
    status_raw_text = Column(String(255))
    offense_date = Column(Date)
    date_charged = Column(Date)
    state_code = Column(String(255), nullable=False, index=True)
    county_code = Column(String(255), index=True)
    ncic_code = Column(String(255))
    statute = Column(String(255))
    description = Column(Text)
    attempted = Column(Boolean)
    classification_type = Column(state_charge_classification_type)
    classification_type_raw_text = Column(String(255))
    classification_subtype = Column(String(255))
    offense_type = Column(String(255))
    is_violent = Column(Boolean)
    counts = Column(Integer)
    charge_notes = Column(Text)
    charging_entity = Column(String(255))
    is_controlling = Column(Boolean)

    @declared_attr
    def court_case_id(self) -> Column:
        return Column(Integer, ForeignKey("state_court_case.court_case_id"), index=True)

    @declared_attr
    def bond_id(self) -> Column:
        return Column(Integer, ForeignKey("state_bond.bond_id"), index=True)


class StateCharge(StateBase, _StateChargeSharedColumns):
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
    )

    charge_id = Column(Integer, primary_key=True)

    # Cross-entity relationships
    person = relationship("StatePerson", uselist=False)
    court_case = relationship(
        "StateCourtCase", uselist=False, backref="charges", lazy="selectin"
    )
    bond = relationship("StateBond", uselist=False, backref="charges", lazy="selectin")


class StateChargeHistory(
    StateBase, _StateChargeSharedColumns, HistoryTableSharedColumns
):
    """Represents the historical state of a StateCharge"""

    __tablename__ = "state_charge_history"

    # This primary key should NOT be used. It only exists because SQLAlchemy
    # requires every table to have a unique primary key.
    charge_history_id = Column(Integer, primary_key=True)

    charge_id = Column(
        Integer, ForeignKey("state_charge.charge_id"), nullable=False, index=True
    )


# StateAssessment


class _StateAssessmentSharedColumns(_ReferencesStatePersonSharedColumns):
    """A mixin which defines all columns common to StateAssessment and
    StateAssessmentHistory"""

    # Consider this class a mixin and only allow instantiating subclasses
    def __new__(cls, *_: Any, **__: Any) -> "_StateAssessmentSharedColumns":
        if cls is _StateAssessmentSharedColumns:
            raise Exception(f"[{cls}] cannot be instantiated")
        return super().__new__(cls)  # type: ignore

    external_id = Column(String(255), index=True)
    assessment_class = Column(state_assessment_class)
    assessment_class_raw_text = Column(String(255))
    assessment_type = Column(state_assessment_type)
    assessment_type_raw_text = Column(String(255))
    assessment_date = Column(Date)
    state_code = Column(String(255), nullable=False, index=True)
    assessment_score = Column(Integer)
    assessment_level = Column(state_assessment_level)
    assessment_level_raw_text = Column(String(255))
    assessment_metadata = Column(Text)

    @declared_attr
    def incarceration_period_id(self) -> Column:
        return Column(
            Integer,
            ForeignKey("state_incarceration_period.incarceration_period_id"),
            index=True,
            nullable=True,
        )

    @declared_attr
    def supervision_period_id(self) -> Column:
        return Column(
            Integer,
            ForeignKey("state_supervision_period.supervision_period_id"),
            index=True,
            nullable=True,
        )

    @declared_attr
    def conducting_agent_id(self) -> Column:
        return Column(
            Integer, ForeignKey("state_agent.agent_id"), index=True, nullable=True
        )


class StateAssessment(StateBase, _StateAssessmentSharedColumns):
    """Represents a StateAssessment in the SQL schema"""

    __tablename__ = "state_assessment"

    assessment_id = Column(Integer, primary_key=True)

    conducting_agent = relationship("StateAgent", uselist=False, lazy="selectin")


class StateAssessmentHistory(
    StateBase, _StateAssessmentSharedColumns, HistoryTableSharedColumns
):
    """Represents the historical state of a StateAssessment"""

    __tablename__ = "state_assessment_history"

    # This primary key should NOT be used. It only exists because SQLAlchemy
    # requires every table to have a unique primary key.
    assessment_history_id = Column(Integer, primary_key=True)

    assessment_id = Column(
        Integer,
        ForeignKey("state_assessment.assessment_id"),
        nullable=False,
        index=True,
    )


# StateSentenceGroup


class _StateSentenceGroupSharedColumns(_ReferencesStatePersonSharedColumns):
    """A mixin which defines all columns common to StateSentenceGroup and
    StateSentenceGroupHistory"""

    # Consider this class a mixin and only allow instantiating subclasses
    def __new__(cls, *_: Any, **__: Any) -> "_StateSentenceGroupSharedColumns":
        if cls is _StateSentenceGroupSharedColumns:
            raise Exception(f"[{cls}] cannot be instantiated")
        return super().__new__(cls)  # type: ignore

    external_id = Column(String(255), index=True)
    status = Column(state_sentence_status, nullable=False)
    status_raw_text = Column(String(255))
    date_imposed = Column(Date)
    state_code = Column(String(255), nullable=False, index=True)
    county_code = Column(String(255), index=True)
    min_length_days = Column(Integer)
    max_length_days = Column(Integer)
    is_life = Column(Boolean)


class StateSentenceGroup(StateBase, _StateSentenceGroupSharedColumns):
    """Represents a StateSentenceGroup in the SQL schema"""

    __tablename__ = "state_sentence_group"
    __table_args__ = (
        UniqueConstraint(
            "state_code",
            "external_id",
            name="sentence_group_external_ids_unique_within_state",
            deferrable=True,
            initially="DEFERRED",
        ),
    )

    sentence_group_id = Column(Integer, primary_key=True)

    supervision_sentences = relationship(
        "StateSupervisionSentence", backref="sentence_group", lazy="selectin"
    )
    incarceration_sentences = relationship(
        "StateIncarcerationSentence", backref="sentence_group", lazy="selectin"
    )
    fines = relationship("StateFine", backref="sentence_group", lazy="selectin")


class StateSentenceGroupHistory(
    StateBase, _StateSentenceGroupSharedColumns, HistoryTableSharedColumns
):
    """Represents the historical state of a StateSentenceGroup"""

    __tablename__ = "state_sentence_group_history"

    # This primary key should NOT be used. It only exists because SQLAlchemy
    # requires every table to have a unique primary key.
    sentence_group_history_id = Column(Integer, primary_key=True)

    sentence_group_id = Column(
        Integer,
        ForeignKey("state_sentence_group.sentence_group_id"),
        nullable=False,
        index=True,
    )


# StateSupervisionSentence


class _StateSupervisionSentenceSharedColumns(
    _ReferencesStatePersonSharedColumns, _ReferencesStateSentenceGroupSharedColumns
):
    """A mixin which defines all columns common to StateSupervisionSentence and
    StateSupervisionSentenceHistory"""

    # Consider this class a mixin and only allow instantiating subclasses
    def __new__(cls, *_: Any, **__: Any) -> "_StateSupervisionSentenceSharedColumns":
        if cls is _StateSupervisionSentenceSharedColumns:
            raise Exception(f"[{cls}] cannot be instantiated")
        return super().__new__(cls)  # type: ignore

    external_id = Column(String(255), index=True)
    status = Column(state_sentence_status, nullable=False)
    status_raw_text = Column(String(255))
    supervision_type = Column(state_supervision_type)
    supervision_type_raw_text = Column(String(255))
    date_imposed = Column(Date)
    start_date = Column(Date)
    projected_completion_date = Column(Date)
    completion_date = Column(Date)
    state_code = Column(String(255), nullable=False, index=True)
    county_code = Column(String(255), index=True)
    min_length_days = Column(Integer)
    max_length_days = Column(Integer)


class StateSupervisionSentence(StateBase, _StateSupervisionSentenceSharedColumns):
    """Represents a StateSupervisionSentence in the SQL schema"""

    __tablename__ = "state_supervision_sentence"

    supervision_sentence_id = Column(Integer, primary_key=True)

    person = relationship("StatePerson", uselist=False)
    charges = relationship(
        "StateCharge",
        secondary=state_charge_supervision_sentence_association_table,
        backref="supervision_sentences",
        lazy="selectin",
    )
    incarceration_periods = relationship(
        "StateIncarcerationPeriod",
        secondary=state_supervision_sentence_incarceration_period_association_table,
        backref="supervision_sentences",
        lazy="selectin",
    )
    supervision_periods = relationship(
        "StateSupervisionPeriod",
        secondary=state_supervision_sentence_supervision_period_association_table,
        backref="supervision_sentences",
        lazy="selectin",
    )
    early_discharges = relationship(
        "StateEarlyDischarge", backref="supervision_sentence", lazy="selectin"
    )


class StateSupervisionSentenceHistory(
    StateBase, _StateSupervisionSentenceSharedColumns, HistoryTableSharedColumns
):
    """Represents the historical state of a StateSupervisionSentence"""

    __tablename__ = "state_supervision_sentence_history"

    # This primary key should NOT be used. It only exists because SQLAlchemy
    # requires every table to have a unique primary key.
    supervision_sentence_history_id = Column(Integer, primary_key=True)

    supervision_sentence_id = Column(
        Integer,
        ForeignKey("state_supervision_sentence.supervision_sentence_id"),
        nullable=False,
        index=True,
    )


# StateIncarcerationSentence


class _StateIncarcerationSentenceSharedColumns(
    _ReferencesStatePersonSharedColumns, _ReferencesStateSentenceGroupSharedColumns
):
    """A mixin which defines all columns common to StateIncarcerationSentence
    and StateIncarcerationSentenceHistory
    """

    # Consider this class a mixin and only allow instantiating subclasses
    def __new__(cls, *_: Any, **__: Any) -> "_StateIncarcerationSentenceSharedColumns":
        if cls is _StateIncarcerationSentenceSharedColumns:
            raise Exception(f"[{cls}] cannot be instantiated")
        return super().__new__(cls)  # type: ignore

    external_id = Column(String(255), index=True)
    status = Column(state_sentence_status, nullable=False)
    status_raw_text = Column(String(255))
    incarceration_type = Column(state_incarceration_type)
    incarceration_type_raw_text = Column(String(255))
    date_imposed = Column(Date)
    start_date = Column(Date)
    projected_min_release_date = Column(Date)
    projected_max_release_date = Column(Date)
    completion_date = Column(Date)
    parole_eligibility_date = Column(Date)
    state_code = Column(String(255), nullable=False, index=True)
    county_code = Column(String(255), index=True)
    min_length_days = Column(Integer)
    max_length_days = Column(Integer)
    is_life = Column(Boolean)
    is_capital_punishment = Column(Boolean)
    parole_possible = Column(Boolean)
    initial_time_served_days = Column(Integer)
    good_time_days = Column(Integer)
    earned_time_days = Column(Integer)


class StateIncarcerationSentence(StateBase, _StateIncarcerationSentenceSharedColumns):
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
    )

    incarceration_sentence_id = Column(Integer, primary_key=True)

    person = relationship("StatePerson", uselist=False)
    charges = relationship(
        "StateCharge",
        secondary=state_charge_incarceration_sentence_association_table,
        backref="incarceration_sentences",
        lazy="selectin",
    )
    incarceration_periods = relationship(
        "StateIncarcerationPeriod",
        secondary=state_incarceration_sentence_incarceration_period_association_table,
        backref="incarceration_sentences",
        lazy="selectin",
    )

    supervision_periods = relationship(
        "StateSupervisionPeriod",
        secondary=state_incarceration_sentence_supervision_period_association_table,
        backref="incarceration_sentences",
        lazy="selectin",
    )

    early_discharges = relationship(
        "StateEarlyDischarge", backref="incarceration_sentence", lazy="selectin"
    )


class StateIncarcerationSentenceHistory(
    StateBase, _StateIncarcerationSentenceSharedColumns, HistoryTableSharedColumns
):
    """Represents the historical state of a StateIncarcerationSentence"""

    __tablename__ = "state_incarceration_sentence_history"

    # This primary key should NOT be used. It only exists because SQLAlchemy
    # requires every table to have a unique primary key.
    incarceration_sentence_history_id = Column(Integer, primary_key=True)

    incarceration_sentence_id = Column(
        Integer,
        ForeignKey("state_incarceration_sentence.incarceration_sentence_id"),
        nullable=False,
        index=True,
    )


# StateFine


class _StateFineSharedColumns(
    _ReferencesStatePersonSharedColumns, _ReferencesStateSentenceGroupSharedColumns
):
    """A mixin which defines all columns common to StateFine and
    StateFineHistory
    """

    # Consider this class a mixin and only allow instantiating subclasses
    def __new__(cls, *_: Any, **__: Any) -> "_StateFineSharedColumns":
        if cls is _StateFineSharedColumns:
            raise Exception(f"[{cls}] cannot be instantiated")
        return super().__new__(cls)  # type: ignore

    external_id = Column(String(255), index=True)
    status = Column(state_fine_status, nullable=False)
    status_raw_text = Column(String(255))
    date_paid = Column(Date)
    state_code = Column(String(255), nullable=False, index=True)
    county_code = Column(String(255), index=True)
    fine_dollars = Column(Integer)


class StateFine(StateBase, _StateFineSharedColumns):
    """Represents a StateFine in the SQL schema"""

    __tablename__ = "state_fine"
    __table_args__ = (
        UniqueConstraint(
            "state_code",
            "external_id",
            name="fine_external_ids_unique_within_state",
            deferrable=True,
            initially="DEFERRED",
        ),
    )
    fine_id = Column(Integer, primary_key=True)

    person = relationship("StatePerson", uselist=False)
    charges = relationship(
        "StateCharge",
        secondary=state_charge_fine_association_table,
        backref="fines",
        lazy="selectin",
    )


class StateFineHistory(StateBase, _StateFineSharedColumns, HistoryTableSharedColumns):
    """Represents the historical state of a StateFine"""

    __tablename__ = "state_fine_history"

    # This primary key should NOT be used. It only exists because SQLAlchemy
    # requires every table to have a unique primary key.
    fine_history_id = Column(Integer, primary_key=True)

    fine_id = Column(
        Integer, ForeignKey("state_fine.fine_id"), nullable=False, index=True
    )


# StateIncarcerationPeriod


class _StateIncarcerationPeriodSharedColumns(_ReferencesStatePersonSharedColumns):
    """A mixin which defines all columns common to StateIncarcerationPeriod and
    StateIncarcerationPeriodHistory
    """

    # Consider this class a mixin and only allow instantiating subclasses
    def __new__(cls, *_: Any, **__: Any) -> "_StateIncarcerationPeriodSharedColumns":
        if cls is _StateIncarcerationPeriodSharedColumns:
            raise Exception(f"[{cls}] cannot be instantiated")
        return super().__new__(cls)  # type: ignore

    external_id = Column(String(255), index=True)
    status = Column(state_incarceration_period_status, nullable=False)
    status_raw_text = Column(String(255))
    incarceration_type = Column(state_incarceration_type)
    incarceration_type_raw_text = Column(String(255))
    admission_date = Column(Date)
    release_date = Column(Date)
    state_code = Column(String(255), nullable=False, index=True)
    county_code = Column(String(255), index=True)
    facility = Column(String(255))
    housing_unit = Column(String(255))
    facility_security_level = Column(state_incarceration_facility_security_level)
    facility_security_level_raw_text = Column(String(255))
    admission_reason = Column(state_incarceration_period_admission_reason)
    admission_reason_raw_text = Column(String(255))
    projected_release_reason = Column(state_incarceration_period_release_reason)
    projected_release_reason_raw_text = Column(String(255))
    release_reason = Column(state_incarceration_period_release_reason)
    release_reason_raw_text = Column(String(255))
    specialized_purpose_for_incarceration = Column(
        state_specialized_purpose_for_incarceration
    )
    specialized_purpose_for_incarceration_raw_text = Column(String(255))
    custodial_authority = Column(state_custodial_authority)
    custodial_authority_raw_text = Column(String(255))

    @declared_attr
    def source_supervision_violation_response_id(self) -> Column:
        return Column(
            Integer,
            ForeignKey(
                "state_supervision_violation_response."
                "supervision_violation_response_id"
            ),
            index=True,
            nullable=True,
        )


class StateIncarcerationPeriod(StateBase, _StateIncarcerationPeriodSharedColumns):
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
    )
    incarceration_period_id = Column(Integer, primary_key=True)

    person = relationship("StatePerson", uselist=False)
    incarceration_incidents = relationship(
        "StateIncarcerationIncident", backref="incarceration_period", lazy="selectin"
    )
    parole_decisions = relationship(
        "StateParoleDecision", backref="incarceration_period", lazy="selectin"
    )
    assessments = relationship(
        "StateAssessment", backref="incarceration_period", lazy="selectin"
    )
    program_assignments = relationship(
        "StateProgramAssignment",
        secondary=state_incarceration_period_program_assignment_association_table,
        backref="incarceration_periods",
        lazy="selectin",
    )

    source_supervision_violation_response = relationship(
        "StateSupervisionViolationResponse", uselist=False, lazy="selectin"
    )


class StateIncarcerationPeriodHistory(
    StateBase, _StateIncarcerationPeriodSharedColumns, HistoryTableSharedColumns
):
    """Represents the historical state of a StateIncarcerationPeriod"""

    __tablename__ = "state_incarceration_period_history"

    # This primary key should NOT be used. It only exists because SQLAlchemy
    # requires every table to have a unique primary key.
    incarceration_period_history_id = Column(Integer, primary_key=True)

    incarceration_period_id = Column(
        Integer,
        ForeignKey("state_incarceration_period.incarceration_period_id"),
        nullable=False,
        index=True,
    )


# StateSupervisionPeriod


class _StateSupervisionPeriodSharedColumns(_ReferencesStatePersonSharedColumns):
    """A mixin which defines all columns common to StateSupervisionPeriod and
    StateSupervisionPeriodHistory
    """

    # Consider this class a mixin and only allow instantiating subclasses
    def __new__(cls, *_: Any, **__: Any) -> "_StateSupervisionPeriodSharedColumns":
        if cls is _StateSupervisionPeriodSharedColumns:
            raise Exception(f"[{cls}] cannot be instantiated")
        return super().__new__(cls)  # type: ignore

    external_id = Column(String(255), index=True)
    status = Column(state_supervision_period_status, nullable=False)
    status_raw_text = Column(String(255))
    supervision_type = Column(state_supervision_type)
    supervision_type_raw_text = Column(String(255))
    supervision_period_supervision_type = Column(
        state_supervision_period_supervision_type
    )
    supervision_period_supervision_type_raw_text = Column(String(255))
    start_date = Column(Date)
    termination_date = Column(Date)
    state_code = Column(String(255), nullable=False, index=True)
    county_code = Column(String(255), index=True)
    supervision_site = Column(String(255))
    admission_reason = Column(state_supervision_period_admission_reason)
    admission_reason_raw_text = Column(String(255))
    termination_reason = Column(state_supervision_period_termination_reason)
    termination_reason_raw_text = Column(String(255))
    supervision_level = Column(state_supervision_level)
    supervision_level_raw_text = Column(String(255))

    # This field can contain an arbitrarily long list of conditions, so we do not restrict the length of the length like
    # we do for most other String fields.
    conditions = Column(Text)
    custodial_authority = Column(state_custodial_authority)
    custodial_authority_raw_text = Column(String(255))

    @declared_attr
    def supervising_officer_id(self) -> Column:
        return Column(
            Integer, ForeignKey("state_agent.agent_id"), index=True, nullable=True
        )


class StateSupervisionPeriod(StateBase, _StateSupervisionPeriodSharedColumns):
    """Represents a StateSupervisionPeriod in the SQL schema"""

    __tablename__ = "state_supervision_period"

    supervision_period_id = Column(Integer, primary_key=True)

    person = relationship("StatePerson", uselist=False)
    supervising_officer = relationship("StateAgent", uselist=False, lazy="selectin")
    # TODO(#2668): Deprecated - Delete this column from our schema.
    supervision_violations = relationship(
        "StateSupervisionViolation", backref="supervision_period", lazy="selectin"
    )
    # TODO(#2697): Rename `supervision_violation_entries` to
    # `supervision_violations` once the 1:many relationship
    # `supervision_violations` above has been removed from our db/schema object.
    supervision_violation_entries = relationship(
        "StateSupervisionViolation",
        secondary=state_supervision_period_supervision_violation_association_table,
        backref="supervision_periods",
        lazy="selectin",
    )
    assessments = relationship(
        "StateAssessment", backref="supervision_period", lazy="selectin"
    )
    program_assignments = relationship(
        "StateProgramAssignment",
        secondary=state_supervision_period_program_assignment_association_table,
        backref="supervision_periods",
        lazy="selectin",
    )
    case_type_entries = relationship(
        "StateSupervisionCaseTypeEntry", backref="supervision_period", lazy="selectin"
    )
    supervision_contacts = relationship(
        "StateSupervisionContact",
        secondary=state_supervision_period_supervision_contact_association_table,
        backref="supervision_periods",
        lazy="selectin",
    )


class StateSupervisionPeriodHistory(
    StateBase, _StateSupervisionPeriodSharedColumns, HistoryTableSharedColumns
):
    """Represents the historical state of a StateSupervisionPeriod"""

    __tablename__ = "state_supervision_period_history"

    # This primary key should NOT be used. It only exists because SQLAlchemy
    # requires every table to have a unique primary key.
    supervision_period_history_id = Column(Integer, primary_key=True)

    supervision_period_id = Column(
        Integer,
        ForeignKey("state_supervision_period.supervision_period_id"),
        nullable=False,
        index=True,
    )


# StateSupervisionCaseTypeEntry


class _StateSupervisionCaseTypeEntrySharedColumns(_ReferencesStatePersonSharedColumns):
    """A mixin which defines all columns common to
    StateSupervisionCaseTypeEntry and StateSupervisionCaseTypeEntryHistory
    """

    # Consider this class a mixin and only allow instantiating subclasses
    def __new__(
        cls, *_: Any, **__: Any
    ) -> "_StateSupervisionCaseTypeEntrySharedColumns":
        if cls is _StateSupervisionCaseTypeEntrySharedColumns:
            raise Exception(f"[{cls}] cannot be instantiated")
        return super().__new__(cls)  # type: ignore

    case_type = Column(state_supervision_case_type)
    case_type_raw_text = Column(String(255))
    state_code = Column(String(255), nullable=False, index=True)

    @declared_attr
    def supervision_period_id(self) -> Column:
        return Column(
            Integer,
            ForeignKey("state_supervision_period.supervision_period_id"),
            index=True,
            nullable=True,
        )


class StateSupervisionCaseTypeEntry(
    StateBase, _StateSupervisionCaseTypeEntrySharedColumns
):
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
    )

    supervision_case_type_entry_id = Column(Integer, primary_key=True)

    person = relationship("StatePerson", uselist=False)

    external_id = Column(String(255), index=True)


# TODO(#4136): Update historical column names here -- or downgrade and upgrade?
class StateSupervisionCaseTypeEntryHistory(
    StateBase, _StateSupervisionCaseTypeEntrySharedColumns, HistoryTableSharedColumns
):
    """Represents the historical state of a StateSupervisionCaseTypeEntry"""

    __tablename__ = "state_supervision_case_type_entry_history"

    # This primary key should NOT be used. It only exists because SQLAlchemy
    # requires every table to have a unique primary key.
    supervision_case_type_entry_history_id = Column(Integer, primary_key=True)

    supervision_case_type_entry_id = Column(
        Integer,
        ForeignKey(
            "state_supervision_case_type_entry" ".supervision_case_type_entry_id"
        ),
        nullable=False,
        index=True,
    )


# StateIncarcerationIncident


class _StateIncarcerationIncidentSharedColumns(_ReferencesStatePersonSharedColumns):
    """A mixin which defines all columns common to StateIncarcerationIncident
    and StateIncarcerationIncidentHistory
    """

    # Consider this class a mixin and only allow instantiating subclasses
    def __new__(cls, *_: Any, **__: Any) -> "_StateIncarcerationIncidentSharedColumns":
        if cls is _StateIncarcerationIncidentSharedColumns:
            raise Exception(f"[{cls}] cannot be instantiated")
        return super().__new__(cls)  # type: ignore

    external_id = Column(String(255), index=True)
    incident_type = Column(state_incarceration_incident_type)
    incident_type_raw_text = Column(String(255))
    incident_date = Column(Date)
    state_code = Column(String(255), nullable=False, index=True)
    facility = Column(String(255))
    location_within_facility = Column(String(255))
    incident_details = Column(Text)

    @declared_attr
    def incarceration_period_id(self) -> Column:
        return Column(
            Integer,
            ForeignKey("state_incarceration_period.incarceration_period_id"),
            index=True,
            nullable=True,
        )

    @declared_attr
    def responding_officer_id(self) -> Column:
        return Column(
            Integer, ForeignKey("state_agent.agent_id"), index=True, nullable=True
        )


class StateIncarcerationIncident(StateBase, _StateIncarcerationIncidentSharedColumns):
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
    )

    incarceration_incident_id = Column(Integer, primary_key=True)

    person = relationship("StatePerson", uselist=False)
    responding_officer = relationship("StateAgent", uselist=False, lazy="selectin")

    incarceration_incident_outcomes = relationship(
        "StateIncarcerationIncidentOutcome",
        backref="incarceration_incident",
        lazy="selectin",
    )


class StateIncarcerationIncidentHistory(
    StateBase, _StateIncarcerationIncidentSharedColumns, HistoryTableSharedColumns
):
    """Represents the historical state of a StateIncarcerationIncident"""

    __tablename__ = "state_incarceration_incident_history"

    # This primary key should NOT be used. It only exists because SQLAlchemy
    # requires every table to have a unique primary key.
    incarceration_incident_history_id = Column(Integer, primary_key=True)

    incarceration_incident_id = Column(
        Integer,
        ForeignKey("state_incarceration_incident.incarceration_incident_id"),
        nullable=False,
        index=True,
    )


# StateIncarcerationIncidentOutcome


class _StateIncarcerationIncidentOutcomeSharedColumns(
    _ReferencesStatePersonSharedColumns
):
    """A mixin which defines all columns common to
    StateIncarcerationIncidentOutcome and
    StateIncarcerationIncidentOutcomeHistory
    """

    # Consider this class a mixin and only allow instantiating subclasses
    def __new__(
        cls, *_: Any, **__: Any
    ) -> "_StateIncarcerationIncidentOutcomeSharedColumns":
        if cls is _StateIncarcerationIncidentOutcomeSharedColumns:
            raise Exception(f"[{cls}] cannot be instantiated")
        return super().__new__(cls)  # type: ignore

    external_id = Column(String(255), index=True)
    outcome_type = Column(state_incarceration_incident_outcome_type)
    outcome_type_raw_text = Column(String(255))
    state_code = Column(String(255), nullable=False, index=True)
    date_effective = Column(Date)
    hearing_date = Column(Date)
    report_date = Column(Date)
    outcome_description = Column(String(255))
    punishment_length_days = Column(Integer)

    @declared_attr
    def incarceration_incident_id(self) -> Column:
        return Column(
            Integer,
            ForeignKey("state_incarceration_incident.incarceration_incident_id"),
            index=True,
            nullable=True,
        )


class StateIncarcerationIncidentOutcome(
    StateBase, _StateIncarcerationIncidentOutcomeSharedColumns
):
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
    )

    incarceration_incident_outcome_id = Column(Integer, primary_key=True)

    person = relationship("StatePerson", uselist=False)


class StateIncarcerationIncidentOutcomeHistory(
    StateBase,
    _StateIncarcerationIncidentOutcomeSharedColumns,
    HistoryTableSharedColumns,
):
    """Represents the historical state of a StateIncarcerationIncidentOutcome"""

    __tablename__ = "state_incarceration_incident_outcome_history"

    # This primary key should NOT be used. It only exists because SQLAlchemy
    # requires every table to have a unique primary key.
    incarceration_incident_outcome_history_id = Column(Integer, primary_key=True)

    incarceration_incident_outcome_id = Column(
        Integer,
        ForeignKey(
            "state_incarceration_incident_outcome." "incarceration_incident_outcome_id"
        ),
        nullable=False,
        index=True,
    )


# StateParoleDecision


class _StateParoleDecisionSharedColumns(_ReferencesStatePersonSharedColumns):
    """A mixin which defines all columns common to StateParoleDecision and
    StateParoleDecisionHistory
    """

    # Consider this class a mixin and only allow instantiating subclasses
    def __new__(cls, *_: Any, **__: Any) -> "_StateParoleDecisionSharedColumns":
        if cls is _StateParoleDecisionSharedColumns:
            raise Exception(f"[{cls}] cannot be instantiated")
        return super().__new__(cls)  # type: ignore

    external_id = Column(String(255), index=True)

    decision_date = Column(Date)
    corrective_action_deadline = Column(Date)
    state_code = Column(String(255), nullable=False, index=True)
    county_code = Column(String(255), index=True)
    decision_outcome = Column(state_parole_decision_outcome)
    decision_outcome_raw_text = Column(String(255))
    decision_reasoning = Column(String(255))
    corrective_action = Column(String(255))

    @declared_attr
    def incarceration_period_id(self) -> Column:
        return Column(
            Integer,
            ForeignKey("state_incarceration_period.incarceration_period_id"),
            index=True,
            nullable=True,
        )


class StateParoleDecision(StateBase, _StateParoleDecisionSharedColumns):
    """Represents a StateParoleDecision in the SQL schema"""

    __tablename__ = "state_parole_decision"
    __table_args__ = (
        UniqueConstraint(
            "state_code",
            "external_id",
            name="parole_decision_external_ids_unique_within_state",
            deferrable=True,
            initially="DEFERRED",
        ),
    )
    parole_decision_id = Column(Integer, primary_key=True)

    person = relationship("StatePerson", uselist=False)
    decision_agents = relationship(
        "StateAgent",
        secondary=state_parole_decision_decision_agent_association_table,
        lazy="selectin",
    )


class StateParoleDecisionHistory(
    StateBase, _StateParoleDecisionSharedColumns, HistoryTableSharedColumns
):
    """Represents the historical state of a StateParoleDecision"""

    __tablename__ = "state_parole_decision_history"

    # This primary key should NOT be used. It only exists because SQLAlchemy
    # requires every table to have a unique primary key.
    parole_decision_history_id = Column(Integer, primary_key=True)

    parole_decision_id = Column(
        Integer,
        ForeignKey("state_parole_decision.parole_decision_id"),
        nullable=False,
        index=True,
    )


# StateSupervisionViolationTypeEntry


class _StateSupervisionViolationTypeEntrySharedColumns(
    _ReferencesStatePersonSharedColumns
):
    """A mixin which defines all columns common to
    StateSupervisionViolationTypeEntry and
    StateSupervisionViolationTypeEntryHistory
    """

    # Consider this class a mixin and only allow instantiating subclasses
    def __new__(
        cls, *_: Any, **__: Any
    ) -> "_StateSupervisionViolationTypeEntrySharedColumns":
        if cls is _StateSupervisionViolationTypeEntrySharedColumns:
            raise Exception(f"[{cls}] cannot be instantiated")
        return super().__new__(cls)  # type: ignore

    state_code = Column(String(255), nullable=False, index=True)
    violation_type = Column(state_supervision_violation_type)
    violation_type_raw_text = Column(String(255))

    @declared_attr
    def supervision_violation_id(self) -> Column:
        return Column(
            Integer,
            ForeignKey("state_supervision_violation." "supervision_violation_id"),
            index=True,
            nullable=True,
        )


class StateSupervisionViolationTypeEntry(
    StateBase, _StateSupervisionViolationTypeEntrySharedColumns
):
    """Represents a StateSupervisionViolationTypeEntry in the SQL schema."""

    __tablename__ = "state_supervision_violation_type_entry"

    supervision_violation_type_entry_id = Column(Integer, primary_key=True)

    person = relationship("StatePerson", uselist=False)


class StateSupervisionViolationTypeEntryHistory(
    StateBase,
    _StateSupervisionViolationTypeEntrySharedColumns,
    HistoryTableSharedColumns,
):
    """Represents the historical state of a
    StateSupervisionViolationTypeEntry.
    """

    __tablename__ = "state_supervision_violation_type_entry_history"

    # This primary key should NOT be used. It only exists because SQLAlchemy
    # requires every table to have a unique primary key.
    supervision_violation_type_history_id = Column(Integer, primary_key=True)

    supervision_violation_type_entry_id = Column(
        Integer,
        ForeignKey(
            "state_supervision_violation_type_entry."
            "supervision_violation_type_entry_id"
        ),
        nullable=False,
        index=True,
    )


# StateSupervisionViolatedConditionEntry


class _StateSupervisionViolatedConditionEntrySharedColumns(
    _ReferencesStatePersonSharedColumns
):
    """A mixin which defines all columns common to
    StateSupervisionViolatedConditionEntry and
    StateSupervisionViolatedConditionEntryHistory
    """

    # Consider this class a mixin and only allow instantiating subclasses
    def __new__(
        cls, *_: Any, **__: Any
    ) -> "_StateSupervisionViolatedConditionEntrySharedColumns":
        if cls is _StateSupervisionViolatedConditionEntrySharedColumns:
            raise Exception(f"[{cls}] cannot be instantiated")
        return super().__new__(cls)  # type: ignore

    state_code = Column(String(255), nullable=False, index=True)
    condition = Column(String(255), nullable=False)

    @declared_attr
    def supervision_violation_id(self) -> Column:
        return Column(
            Integer,
            ForeignKey("state_supervision_violation." "supervision_violation_id"),
            index=True,
            nullable=True,
        )


class StateSupervisionViolatedConditionEntry(
    StateBase, _StateSupervisionViolatedConditionEntrySharedColumns
):
    """Represents a StateSupervisionViolatedConditionEntry in the SQL schema."""

    __tablename__ = "state_supervision_violated_condition_entry"

    supervision_violated_condition_entry_id = Column(Integer, primary_key=True)

    person = relationship("StatePerson", uselist=False)


class StateSupervisionViolatedConditionEntryHistory(
    StateBase,
    _StateSupervisionViolatedConditionEntrySharedColumns,
    HistoryTableSharedColumns,
):
    """Represents the historical state of a
    StateSupervisionViolatedConditionEntry
    """

    __tablename__ = "state_supervision_violated_condition_entry_history"

    # This primary key should NOT be used. It only exists because SQLAlchemy
    # requires every table to have a unique primary key.
    supervision_violated_condition_entry_history_id = Column(Integer, primary_key=True)

    supervision_violated_condition_entry_id = Column(
        Integer,
        ForeignKey(
            "state_supervision_violated_condition_entry."
            "supervision_violated_condition_entry_id"
        ),
        nullable=False,
        index=True,
    )


# StateSupervisionViolation


class _StateSupervisionViolationSharedColumns(_ReferencesStatePersonSharedColumns):
    """A mixin which defines all columns common to StateSupervisionViolation and
    StateSupervisionViolationHistory
    """

    # Consider this class a mixin and only allow instantiating subclasses
    def __new__(cls, *_: Any, **__: Any) -> "_StateSupervisionViolationSharedColumns":
        if cls is _StateSupervisionViolationSharedColumns:
            raise Exception(f"[{cls}] cannot be instantiated")
        return super().__new__(cls)  # type: ignore

    external_id = Column(String(255), index=True)
    violation_type = Column(state_supervision_violation_type)
    violation_type_raw_text = Column(String(255))
    violation_date = Column(Date)
    state_code = Column(String(255), nullable=False, index=True)
    is_violent = Column(Boolean)
    is_sex_offense = Column(Boolean)
    violated_conditions = Column(String(255))

    # TODO(#2668): Deprecated - remove this column from our schema.
    @declared_attr
    def supervision_period_id(self) -> Column:
        return Column(
            Integer,
            ForeignKey("state_supervision_period.supervision_period_id"),
            index=True,
            nullable=True,
        )


class StateSupervisionViolation(StateBase, _StateSupervisionViolationSharedColumns):
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
    )

    supervision_violation_id = Column(Integer, primary_key=True)

    person = relationship("StatePerson", uselist=False)

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


class StateSupervisionViolationHistory(
    StateBase, _StateSupervisionViolationSharedColumns, HistoryTableSharedColumns
):
    """Represents the historical state of a StateSupervisionViolation"""

    __tablename__ = "state_supervision_violation_history"

    # This primary key should NOT be used. It only exists because SQLAlchemy
    # requires every table to have a unique primary key.
    supervision_violation_history_id = Column(Integer, primary_key=True)

    supervision_violation_id = Column(
        Integer,
        ForeignKey("state_supervision_violation.supervision_violation_id"),
        nullable=False,
        index=True,
    )


# StateSupervisionViolationResponseDecisionEntry


class _StateSupervisionViolationResponseDecisionEntrySharedColumns(
    _ReferencesStatePersonSharedColumns
):
    """A mixin which defines all columns common to
    StateSupervisionViolationResponseDecisionEntry and
    StateSupervisionViolationResponseDecisionEntryHistory
    """

    # Consider this class a mixin and only allow instantiating subclasses
    def __new__(
        cls, *_: Any, **__: Any
    ) -> "_StateSupervisionViolationResponseDecisionEntrySharedColumns":
        if cls is _StateSupervisionViolationResponseDecisionEntrySharedColumns:
            raise Exception(f"[{cls}] cannot be instantiated")
        return super().__new__(cls)  # type: ignore

    state_code = Column(String(255), nullable=False, index=True)
    decision = Column(state_supervision_violation_response_decision)
    decision_raw_text = Column(String(255))
    revocation_type = Column(state_supervision_violation_response_revocation_type)
    revocation_type_raw_text = Column(String(255))

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
        )


class StateSupervisionViolationResponseDecisionEntry(
    StateBase, _StateSupervisionViolationResponseDecisionEntrySharedColumns
):
    """Represents a StateSupervisionViolationResponseDecisionEntry in the
    SQL schema.
    """

    __tablename__ = "state_supervision_violation_response_decision_entry"

    supervision_violation_response_decision_entry_id = Column(Integer, primary_key=True)

    person = relationship("StatePerson", uselist=False)


class StateSupervisionViolationResponseDecisionEntryHistory(
    StateBase,
    _StateSupervisionViolationResponseDecisionEntrySharedColumns,
    HistoryTableSharedColumns,
):
    """Represents the historical state of a
    StateSupervisionViolationResponseDecisionEntry.
    """

    __tablename__ = "state_supervision_violation_response_decision_entry_history"

    # This primary key should NOT be used. It only exists because SQLAlchemy
    # requires every table to have a unique primary key.
    supervision_violation_response_decision_entry_history_id = Column(
        Integer, primary_key=True
    )

    supervision_violation_response_decision_entry_id = Column(
        Integer,
        ForeignKey(
            "state_supervision_violation_response_decision_entry."
            "supervision_violation_response_decision_entry_id"
        ),
        nullable=False,
        index=True,
    )


# StateSupervisionViolationResponse


class _StateSupervisionViolationResponseSharedColumns(
    _ReferencesStatePersonSharedColumns
):
    """A mixin which defines all columns common to
    StateSupervisionViolationResponse and
    StateSupervisionViolationResponseHistory
    """

    # Consider this class a mixin and only allow instantiating subclasses
    def __new__(
        cls, *_: Any, **__: Any
    ) -> "_StateSupervisionViolationResponseSharedColumns":
        if cls is _StateSupervisionViolationResponseSharedColumns:
            raise Exception(f"[{cls}] cannot be instantiated")
        return super().__new__(cls)  # type: ignore

    external_id = Column(String(255), index=True)
    response_type = Column(state_supervision_violation_response_type)
    response_type_raw_text = Column(String(255))
    response_subtype = Column(String(255))
    response_date = Column(Date)
    state_code = Column(String(255), nullable=False, index=True)
    # TODO(#2668): DEPRECATED - DELETE IN FOLLOW-UP PR
    decision = Column(state_supervision_violation_response_decision)
    decision_raw_text = Column(String(255))
    revocation_type = Column(state_supervision_violation_response_revocation_type)
    revocation_type_raw_text = Column(String(255))
    deciding_body_type = Column(state_supervision_violation_response_deciding_body_type)
    deciding_body_type_raw_text = Column(String(255))
    is_draft = Column(Boolean)

    @declared_attr
    def supervision_violation_id(self) -> Column:
        return Column(
            Integer,
            ForeignKey("state_supervision_violation.supervision_violation_id"),
            index=True,
            nullable=True,
        )


class StateSupervisionViolationResponse(
    StateBase, _StateSupervisionViolationResponseSharedColumns
):
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
    )

    supervision_violation_response_id = Column(Integer, primary_key=True)

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


class StateSupervisionViolationResponseHistory(
    StateBase,
    _StateSupervisionViolationResponseSharedColumns,
    HistoryTableSharedColumns,
):
    """Represents the historical state of a StateSupervisionViolationResponse"""

    __tablename__ = "state_supervision_violation_response_history"

    # This primary key should NOT be used. It only exists because SQLAlchemy
    # requires every table to have a unique primary key.
    supervision_violation_response_history_id = Column(Integer, primary_key=True)

    supervision_violation_response_id = Column(
        Integer,
        ForeignKey(
            "state_supervision_violation_response." "supervision_violation_response_id"
        ),
        nullable=False,
        index=True,
    )


# StateAgent


class _StateAgentSharedColumns:
    """A mixin which defines all columns common to StateAgent and
    StateAgentHistory
    """

    # Consider this class a mixin and only allow instantiating subclasses
    def __new__(cls, *_: Any, **__: Any) -> "_StateAgentSharedColumns":
        if cls is _StateAgentSharedColumns:
            raise Exception(f"[{cls}] cannot be instantiated")
        return super().__new__(cls)  # type: ignore

    external_id = Column(String(255), index=True)
    agent_type = Column(state_agent_type, nullable=False)
    agent_type_raw_text = Column(String(255))
    state_code = Column(String(255), nullable=False, index=True)
    full_name = Column(String(255))


class StateAgent(StateBase, _StateAgentSharedColumns):
    """Represents a StateAgent in the SQL schema"""

    __tablename__ = "state_agent"

    agent_id = Column(Integer, primary_key=True)


class StateAgentHistory(StateBase, _StateAgentSharedColumns, HistoryTableSharedColumns):
    """Represents the historical state of a StateAgent"""

    __tablename__ = "state_agent_history"

    # This primary key should NOT be used. It only exists because SQLAlchemy
    # requires every table to have a unique primary key.
    agent_history_id = Column(Integer, primary_key=True)

    agent_id = Column(
        Integer, ForeignKey("state_agent.agent_id"), nullable=False, index=True
    )


# StateProgramAssignment


class _StateProgramAssignmentSharedColumns(_ReferencesStatePersonSharedColumns):
    """A mixin which defines all columns common to StateProgramAssignment and
    StateProgramAssignmentHistory.
    """

    # Consider this class a mixin and only allow instantiating subclasses
    def __new__(cls, *_: Any, **__: Any) -> "_StateProgramAssignmentSharedColumns":
        if cls is _StateProgramAssignmentSharedColumns:
            raise Exception(f"[{cls}] cannot be instantiated")
        return super().__new__(cls)  # type: ignore

    external_id = Column(String(255), index=True)
    state_code = Column(String(255), nullable=False, index=True)
    # TODO(#2450): Switch program_id/location_id for a program foreign key once
    # we've ingested program information into our schema.
    program_id = Column(String(255))
    program_location_id = Column(String(255))

    participation_status = Column(
        state_program_assignment_participation_status, nullable=False
    )
    participation_status_raw_text = Column(String(255))
    discharge_reason = Column(state_program_assignment_discharge_reason)
    discharge_reason_raw_text = Column(String(255))
    referral_date = Column(Date)
    start_date = Column(Date)
    discharge_date = Column(Date)
    referral_metadata = Column(Text)

    @declared_attr
    def referring_agent_id(self) -> Column:
        return Column(
            Integer, ForeignKey("state_agent.agent_id"), index=True, nullable=True
        )


class StateProgramAssignment(StateBase, _StateProgramAssignmentSharedColumns):
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
    )

    program_assignment_id = Column(Integer, primary_key=True)
    referring_agent = relationship("StateAgent", uselist=False, lazy="selectin")


class StateProgramAssignmentHistory(
    StateBase, _StateProgramAssignmentSharedColumns, HistoryTableSharedColumns
):
    """Represents the historical state of a StateProgramAssignment"""

    __tablename__ = "state_program_assignment_history"

    # This primary key should NOT be used. It only exists because SQLAlchemy
    # requires every table to have a unique primary key.
    program_assignment_history_id = Column(Integer, primary_key=True)

    program_assignment_id = Column(
        Integer,
        ForeignKey("state_program_assignment.program_assignment_id"),
        nullable=False,
        index=True,
    )


# StateEarlyDischarge


class _StateEarlyDischargeSharedColumns(_ReferencesStatePersonSharedColumns):
    """A mixin which defines all columns common to StateEarlyDischarge and
    StateEarlyDischargeHistory.
    """

    # Consider this class a mixin and only allow instantiating subclasses
    def __new__(cls, *_: Any, **__: Any) -> "_StateEarlyDischargeSharedColumns":
        if cls is _StateEarlyDischargeSharedColumns:
            raise Exception(f"[{cls}] cannot be instantiated")
        return super().__new__(cls)  # type: ignore

    external_id = Column(String(255), index=True)
    state_code = Column(String(255), nullable=False, index=True)
    county_code = Column(String(255))

    decision_date = Column(Date)
    decision = Column(state_early_discharge_decision)
    decision_raw_text = Column(String(255))
    decision_status = Column(state_early_discharge_decision_status)
    decision_status_raw_text = Column(String(255))
    deciding_body_type = Column(state_acting_body_type)
    deciding_body_type_raw_text = Column(String(255))
    request_date = Column(Date)
    requesting_body_type = Column(state_acting_body_type)
    requesting_body_type_raw_text = Column(String(255))

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
        )


class StateEarlyDischarge(StateBase, _StateEarlyDischargeSharedColumns):
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
    )

    early_discharge_id = Column(Integer, primary_key=True)

    person = relationship("StatePerson", uselist=False)


class StateEarlyDischargeHistory(
    StateBase, _StateEarlyDischargeSharedColumns, HistoryTableSharedColumns
):
    """Represents the historical state of a StateEarlyDischarge"""

    __tablename__ = "state_early_discharge_history"

    # This primary key should NOT be used. It only exists because SQLAlchemy
    # requires every table to have a unique primary key.
    early_discharge_history_id = Column(Integer, primary_key=True)

    early_discharge_id = Column(
        Integer,
        ForeignKey("state_early_discharge.early_discharge_id"),
        nullable=False,
        index=True,
    )


# StateSupervisionContact


class _StateSupervisionContactSharedColumns(_ReferencesStatePersonSharedColumns):
    """A mixin which defines all columns common to StateSupervisionContact and
    StateSupervisionContactHistory.
    """

    # Consider this class a mixin and only allow instantiating subclasses
    def __new__(cls, *_: Any, **__: Any) -> "_StateSupervisionContactSharedColumns":
        if cls is _StateSupervisionContactSharedColumns:
            raise Exception(f"[{cls}] cannot be instantiated")
        return super().__new__(cls)  # type: ignore

    external_id = Column(String(255), index=True)
    state_code = Column(String(255), nullable=False, index=True)

    contact_date = Column(Date)
    contact_reason = Column(state_supervision_contact_reason)
    contact_reason_raw_text = Column(String(255))
    contact_type = Column(state_supervision_contact_type)
    contact_type_raw_text = Column(String(255))
    location = Column(state_supervision_contact_location)
    location_raw_text = Column(String(255))
    resulted_in_arrest = Column(Boolean)
    status = Column(state_supervision_contact_status)
    status_raw_text = Column(String(255))
    verified_employment = Column(Boolean)

    @declared_attr
    def contacted_agent_id(self) -> Column:
        return Column(
            Integer, ForeignKey("state_agent.agent_id"), index=True, nullable=True
        )


class StateSupervisionContact(StateBase, _StateSupervisionContactSharedColumns):
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
    )

    supervision_contact_id = Column(Integer, primary_key=True)

    person = relationship("StatePerson", uselist=False)
    contacted_agent = relationship("StateAgent", uselist=False, lazy="selectin")


class StateSupervisionContactHistory(
    StateBase, _StateSupervisionContactSharedColumns, HistoryTableSharedColumns
):
    """Represents the historical state of a StateSupervisionContact"""

    __tablename__ = "state_supervision_contact_history"

    # This primary key should NOT be used. It only exists because SQLAlchemy
    # requires every table to have a unique primary key.
    supervision_contact_history_id = Column(Integer, primary_key=True)

    supervision_contact_id = Column(
        Integer,
        ForeignKey("state_supervision_contact.supervision_contact_id"),
        nullable=False,
        index=True,
    )
