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
    CheckConstraint,
    Column,
    Date,
    DateTime,
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

# Defines the base class for all table classes in the state schema.
StateBase: DeclarativeMeta = declarative_base(cls=DatabaseEntity, name="StateBase")

# SQLAlchemy enums. Created separately from the tables so they can be shared
# between tables / columns if necessary.
state_charge_v2_classification_type = Enum(
    # TODO(#26240): Replace state_charge usage with this
    state_enum_strings.state_charge_classification_type_civil,
    state_enum_strings.state_charge_classification_type_felony,
    state_enum_strings.state_charge_classification_type_misdemeanor,
    state_enum_strings.internal_unknown,
    state_enum_strings.external_unknown,
    name="state_charge_v2_classification_type",
)
state_charge_v2_status = Enum(
    # TODO(#26240): Replace state_charge usage with this
    state_enum_strings.state_charge_status_acquitted,
    state_enum_strings.state_charge_status_adjudicated,
    state_enum_strings.state_charge_status_convicted,
    state_enum_strings.state_charge_status_dropped,
    state_enum_strings.state_charge_status_pending,
    state_enum_strings.state_charge_status_transferred_away,
    state_enum_strings.internal_unknown,
    state_enum_strings.external_unknown,
    state_enum_strings.present_without_info,
    name="state_charge_v2_status",
)
state_sentence_type = Enum(
    state_enum_strings.state_sentence_type_county_jail,
    state_enum_strings.state_sentence_type_federal_prison,
    state_enum_strings.state_sentence_type_state_prison,
    state_enum_strings.state_sentence_type_parole,
    state_enum_strings.state_sentence_type_probation,
    state_enum_strings.state_sentence_type_community_corrections,
    state_enum_strings.state_sentence_type_community_service,
    state_enum_strings.state_sentence_type_fines_restitution,
    state_enum_strings.state_sentence_type_split,
    state_enum_strings.state_sentence_type_treatment,
    state_enum_strings.internal_unknown,
    state_enum_strings.external_unknown,
    name="state_sentence_type",
)

state_sentencing_authority = Enum(
    state_enum_strings.state_sentencing_authority_county,
    state_enum_strings.state_sentencing_authority_state,
    state_enum_strings.state_sentencing_authority_other_state,
    state_enum_strings.state_sentencing_authority_federal,
    state_enum_strings.present_without_info,
    state_enum_strings.internal_unknown,
    name="state_sentencing_authority",
)

state_assessment_class = Enum(
    state_enum_strings.state_assessment_class_education,
    state_enum_strings.state_assessment_class_mental_health,
    state_enum_strings.state_assessment_class_risk,
    state_enum_strings.state_assessment_class_sex_offense,
    state_enum_strings.state_assessment_class_social,
    state_enum_strings.state_assessment_class_substance_abuse,
    state_enum_strings.state_assessment_class_medical,
    state_enum_strings.state_assessment_class_work,
    state_enum_strings.internal_unknown,
    state_enum_strings.external_unknown,
    name="state_assessment_class",
)

state_assessment_type = Enum(
    state_enum_strings.state_assessment_type_caf,
    state_enum_strings.state_assessment_type_csra,
    state_enum_strings.state_assessment_type_cssm,
    state_enum_strings.state_assessment_type_cmhs,
    state_enum_strings.state_assessment_type_compas,
    state_enum_strings.state_assessment_type_hiq,
    state_enum_strings.state_assessment_type_icasa,
    state_enum_strings.state_assessment_type_j_soap,
    state_enum_strings.state_assessment_type_lsir,
    state_enum_strings.state_assessment_type_ls_rnr,
    state_enum_strings.state_assessment_type_odara,
    state_enum_strings.state_assessment_type_oyas,
    state_enum_strings.state_assessment_type_pa_rst,
    state_enum_strings.state_assessment_type_psa,
    state_enum_strings.state_assessment_type_saca,
    state_enum_strings.state_assessment_type_sorac,
    state_enum_strings.state_assessment_type_sotips,
    state_enum_strings.state_assessment_type_spin_w,
    state_enum_strings.state_assessment_type_stable,
    state_enum_strings.state_assessment_type_static_99,
    state_enum_strings.state_assessment_type_strong_r,
    state_enum_strings.state_assessment_type_strong_r2,
    state_enum_strings.state_assessment_type_tabe,
    state_enum_strings.state_assessment_type_tcu_drug_screen,
    state_enum_strings.state_assessment_type_oras_community_supervision,
    state_enum_strings.state_assessment_type_oras_community_supervision_screening,
    state_enum_strings.state_assessment_type_oras_misdemeanor_assessment,
    state_enum_strings.state_assessment_type_oras_misdemeanor_screening,
    state_enum_strings.state_assessment_type_oras_pre_trial,
    state_enum_strings.state_assessment_type_oras_prison_screening,
    state_enum_strings.state_assessment_type_oras_prison_intake,
    state_enum_strings.state_assessment_type_oras_supplemental_reentry,
    state_enum_strings.state_assessment_type_oras_reentry,
    state_enum_strings.state_assessment_type_tx_csst,
    state_enum_strings.state_assessment_type_tx_cst,
    state_enum_strings.state_assessment_type_tx_srt,
    state_enum_strings.state_assessment_type_tx_rt,
    state_enum_strings.state_assessment_type_accat,
    state_enum_strings.state_assessment_type_acute,
    state_enum_strings.state_assessment_type_rsls,
    state_enum_strings.state_assessment_type_ccrra,
    state_enum_strings.state_assessment_type_mi_csj_353,
    state_enum_strings.state_assessment_type_az_gen_risk_lvl,
    state_enum_strings.state_assessment_type_az_vlnc_risk_lvl,
    state_enum_strings.state_assessment_type_mo_classification_e,
    state_enum_strings.state_assessment_type_mo_classification_mh,
    state_enum_strings.state_assessment_type_mo_classification_m,
    state_enum_strings.state_assessment_type_mo_classification_w,
    state_enum_strings.state_assessment_type_mo_classification_v,
    state_enum_strings.state_assessment_type_mo_classification_i,
    state_enum_strings.state_assessment_type_mo_classification_p,
    state_enum_strings.state_assessment_type_mo_1270,
    state_enum_strings.state_assessment_type_mi_security_class,
    state_enum_strings.state_assessment_type_ut_security_assess,
    state_enum_strings.state_assessment_type_ia_custody_class,
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
    state_enum_strings.state_assessment_level_low_moderate,
    state_enum_strings.state_assessment_level_intense,
    state_enum_strings.internal_unknown,
    state_enum_strings.external_unknown,
    name="state_assessment_level",
)

state_sentence_status = Enum(
    state_enum_strings.state_sentence_status_amended,
    state_enum_strings.state_sentence_status_commuted,
    state_enum_strings.state_sentence_status_completed,
    state_enum_strings.state_sentence_status_death,
    state_enum_strings.state_sentence_status_execution,
    state_enum_strings.state_sentence_status_pardoned,
    state_enum_strings.state_sentence_status_pending,
    state_enum_strings.state_sentence_status_sanctioned,
    state_enum_strings.state_sentence_status_serving,
    state_enum_strings.state_sentence_status_imposed_pending_serving,
    state_enum_strings.state_sentence_status_suspended,
    state_enum_strings.state_sentence_status_revoked,
    state_enum_strings.state_sentence_status_vacated,
    state_enum_strings.state_sentence_status_non_credit_serving,
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
    state_enum_strings.state_supervision_case_type_lifetime_supervision,
    state_enum_strings.state_supervision_case_type_mental_health_court,
    state_enum_strings.state_supervision_case_type_serious_mental_illness_or_disability,
    state_enum_strings.state_supervision_case_type_sex_offense,
    state_enum_strings.state_supervision_case_type_veterans_court,
    state_enum_strings.state_supervision_case_type_day_reporting,
    state_enum_strings.state_supervision_case_type_physical_illness_or_disability,
    state_enum_strings.state_supervision_case_type_electronic_monitoring,
    state_enum_strings.state_supervision_case_type_intense_supervision,
    state_enum_strings.internal_unknown,
    state_enum_strings.external_unknown,
    name="state_supervision_case_type",
)

state_charge_classification_type = Enum(
    state_enum_strings.state_charge_classification_type_civil,
    state_enum_strings.state_charge_classification_type_felony,
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


state_person_address_type = Enum(
    state_enum_strings.state_person_address_type_physical_residence,
    state_enum_strings.state_person_address_type_physical_other,
    state_enum_strings.state_person_address_type_mailing_only,
    state_enum_strings.internal_unknown,
    state_enum_strings.external_unknown,
    name="state_person_address_type",
)

state_person_housing_status_type = Enum(
    state_enum_strings.state_person_housing_status_type_unhoused,
    state_enum_strings.state_person_housing_status_type_temporary_or_supportive_housing,
    state_enum_strings.state_person_housing_status_type_permanent_residence,
    state_enum_strings.state_person_housing_status_type_facility,
    state_enum_strings.internal_unknown,
    state_enum_strings.external_unknown,
    name="state_person_housing_status_type",
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
    state_enum_strings.internal_unknown,
    state_enum_strings.external_unknown,
    name="state_gender",
)

state_sex = Enum(
    state_enum_strings.state_sex_female,
    state_enum_strings.state_sex_male,
    state_enum_strings.state_sex_other,
    state_enum_strings.internal_unknown,
    state_enum_strings.external_unknown,
    name="state_sex",
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
    state_enum_strings.state_incarceration_period_admission_reason_escape,
    state_enum_strings.state_incarceration_period_admission_reason_new_admission,
    state_enum_strings.state_incarceration_period_admission_reason_revocation,
    state_enum_strings.state_incarceration_period_admission_reason_sanction_admission,
    state_enum_strings.state_incarceration_period_admission_reason_return_from_erroneous_release,
    state_enum_strings.state_incarceration_period_admission_reason_return_from_temporary_release,
    state_enum_strings.state_incarceration_period_admission_reason_return_from_escape,
    state_enum_strings.state_incarceration_period_admission_reason_temporary_custody,
    state_enum_strings.state_incarceration_period_admission_reason_temporary_release,
    state_enum_strings.state_incarceration_period_admission_reason_transfer,
    state_enum_strings.state_incarceration_period_admission_reason_transfer_from_other_jurisdiction,
    state_enum_strings.state_incarceration_period_admission_reason_status_change,
    state_enum_strings.state_incarceration_period_admission_reason_weekend_confinement,
    state_enum_strings.internal_unknown,
    state_enum_strings.external_unknown,
    name="state_incarceration_period_admission_reason",
)

state_incarceration_period_custody_level = Enum(
    state_enum_strings.state_incarceration_period_custody_level_close,
    state_enum_strings.state_incarceration_period_custody_level_intake,
    state_enum_strings.state_incarceration_period_custody_level_maximum,
    state_enum_strings.state_incarceration_period_custody_level_medium,
    state_enum_strings.state_incarceration_period_custody_level_minimum,
    state_enum_strings.state_incarceration_period_custody_level_restrictive_minimum,
    state_enum_strings.state_incarceration_period_custody_level_solitary_confinement,
    state_enum_strings.internal_unknown,
    state_enum_strings.external_unknown,
    name="state_incarceration_period_custody_level",
)

state_incarceration_period_housing_unit_category = Enum(
    state_enum_strings.state_incarceration_period_housing_unit_category_solitary_confinement,
    state_enum_strings.state_incarceration_period_housing_unit_category_general,
    state_enum_strings.internal_unknown,
    state_enum_strings.external_unknown,
    name="state_incarceration_period_housing_unit_category",
)

state_incarceration_period_housing_unit_type = Enum(
    state_enum_strings.state_incarceration_period_housing_unit_type_temporary_solitary_confinement,
    state_enum_strings.state_incarceration_period_housing_unit_type_disciplinary_solitary_confinement,
    state_enum_strings.state_incarceration_period_housing_unit_type_administrative_solitary_confinement,
    state_enum_strings.state_incarceration_period_housing_unit_type_protective_custody,
    state_enum_strings.state_incarceration_period_housing_unit_type_other_solitary_confinement,
    state_enum_strings.state_incarceration_period_housing_unit_type_mental_health_solitary_confinement,
    state_enum_strings.state_incarceration_period_housing_unit_type_hospital,
    state_enum_strings.state_incarceration_period_housing_unit_type_general,
    state_enum_strings.state_incarceration_period_housing_unit_type_permanent_solitary,
    state_enum_strings.internal_unknown,
    state_enum_strings.external_unknown,
    name="state_incarceration_period_housing_unit_type",
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
    state_enum_strings.state_incarceration_period_release_reason_return_from_escape,
    state_enum_strings.state_incarceration_period_release_reason_return_from_temporary_release,
    state_enum_strings.state_incarceration_period_release_reason_sentence_served,
    state_enum_strings.state_incarceration_period_release_reason_temporary_release,
    state_enum_strings.state_incarceration_period_release_reason_transfer,
    state_enum_strings.state_incarceration_period_release_reason_transfer_to_other_jurisdiction,
    state_enum_strings.state_incarceration_period_release_reason_vacated,
    state_enum_strings.state_incarceration_period_release_reason_status_change,
    state_enum_strings.state_incarceration_period_release_reason_release_from_weekend_confinement,
    state_enum_strings.internal_unknown,
    state_enum_strings.external_unknown,
    name="state_incarceration_period_release_reason",
)

state_supervision_period_admission_reason = Enum(
    state_enum_strings.state_supervision_period_admission_reason_absconsion,
    state_enum_strings.state_supervision_period_admission_reason_release_from_incarceration,
    state_enum_strings.state_supervision_period_admission_reason_court_sentence,
    state_enum_strings.state_supervision_period_admission_reason_investigation,
    state_enum_strings.state_supervision_period_admission_reason_transfer_from_other_jurisdiction,
    state_enum_strings.state_supervision_period_admission_reason_transfer_within_state,
    state_enum_strings.state_supervision_period_admission_reason_return_from_absconsion,
    state_enum_strings.state_supervision_period_admission_reason_return_from_suspension,
    state_enum_strings.state_supervision_period_admission_reason_return_from_weekend_confinement,
    state_enum_strings.internal_unknown,
    state_enum_strings.external_unknown,
    name="state_supervision_period_admission_reason",
)

state_supervision_level = Enum(
    state_enum_strings.state_supervision_period_supervision_level_minimum,
    state_enum_strings.state_supervision_period_supervision_level_low_medium,
    state_enum_strings.state_supervision_period_supervision_level_medium,
    state_enum_strings.state_supervision_period_supervision_level_high,
    state_enum_strings.state_supervision_period_supervision_level_maximum,
    state_enum_strings.state_supervision_period_supervision_level_in_custody,
    state_enum_strings.state_supervision_period_supervision_level_diversion,
    state_enum_strings.state_supervision_period_supervision_level_interstate_compact,
    state_enum_strings.state_supervision_period_supervision_level_limited,
    state_enum_strings.state_supervision_period_supervision_level_electronic_monitoring_only,
    state_enum_strings.state_supervision_period_supervision_level_unsupervised,
    state_enum_strings.state_supervision_period_supervision_level_unassigned,
    state_enum_strings.state_supervision_period_supervision_level_warrant,
    state_enum_strings.state_supervision_period_supervision_level_absconsion,
    state_enum_strings.state_supervision_period_supervision_level_intake,
    state_enum_strings.state_supervision_period_supervision_level_residential_program,
    state_enum_strings.state_supervision_period_supervision_level_furlough,
    state_enum_strings.internal_unknown,
    state_enum_strings.external_unknown,
    state_enum_strings.present_without_info,
    name="state_supervision_level",
)

state_supervision_period_termination_reason = Enum(
    state_enum_strings.state_supervision_period_termination_reason_absconsion,
    state_enum_strings.state_supervision_period_termination_reason_admitted_to_incarceration,
    state_enum_strings.state_supervision_period_termination_reason_commuted,
    state_enum_strings.state_supervision_period_termination_reason_death,
    state_enum_strings.state_supervision_period_termination_reason_discharge,
    state_enum_strings.state_supervision_period_termination_reason_expiration,
    state_enum_strings.state_supervision_period_termination_reason_investigation,
    state_enum_strings.state_supervision_period_termination_reason_pardoned,
    state_enum_strings.state_supervision_period_termination_reason_transfer_to_other_jurisdiction,
    state_enum_strings.state_supervision_period_termination_reason_transfer_within_state,
    state_enum_strings.state_supervision_period_termination_reason_return_from_absconsion,
    state_enum_strings.state_supervision_period_termination_reason_revocation,
    state_enum_strings.state_supervision_period_termination_reason_suspension,
    state_enum_strings.state_supervision_period_termination_reason_vacated,
    state_enum_strings.state_supervision_period_termination_reason_weekend_confinement,
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
    state_enum_strings.state_supervision_period_supervision_type_warrant_status,
    state_enum_strings.state_supervision_period_supervision_type_deported,
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

state_incarceration_incident_severity = Enum(
    state_enum_strings.state_incarceration_incident_severity_highest,
    state_enum_strings.state_incarceration_incident_severity_second_highest,
    state_enum_strings.state_incarceration_incident_severity_third_highest,
    state_enum_strings.state_incarceration_incident_severity_fourth_highest,
    state_enum_strings.state_incarceration_incident_severity_fifth_highest,
    state_enum_strings.state_incarceration_incident_severity_sixth_highest,
    state_enum_strings.state_incarceration_incident_severity_seventh_highest,
    state_enum_strings.internal_unknown,
    state_enum_strings.external_unknown,
    name="state_incarceration_incident_severity",
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

state_supervision_violation_severity = Enum(
    state_enum_strings.state_supervision_violation_severity_highest,
    state_enum_strings.state_supervision_violation_severity_second_highest,
    state_enum_strings.state_supervision_violation_severity_third_highest,
    state_enum_strings.state_supervision_violation_severity_fourth_highest,
    state_enum_strings.state_supervision_violation_severity_fifth_highest,
    state_enum_strings.state_supervision_violation_severity_sixth_highest,
    state_enum_strings.state_supervision_violation_severity_seventh_highest,
    state_enum_strings.internal_unknown,
    state_enum_strings.external_unknown,
    name="state_supervision_violation_severity",
)


state_supervision_violated_condition_type = Enum(
    state_enum_strings.state_supervision_violated_condition_type_employment,
    state_enum_strings.state_supervision_violated_condition_type_failure_to_notify,
    state_enum_strings.state_supervision_violated_condition_type_failure_to_report,
    state_enum_strings.state_supervision_violated_condition_type_financial,
    state_enum_strings.state_supervision_violated_condition_type_law,
    state_enum_strings.state_supervision_violated_condition_type_special_conditions,
    state_enum_strings.state_supervision_violated_condition_type_substance,
    state_enum_strings.state_supervision_violated_condition_type_treatment_compliance,
    state_enum_strings.internal_unknown,
    state_enum_strings.external_unknown,
    name="state_supervision_violated_condition_type",
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

state_supervision_violation_response_severity = Enum(
    state_enum_strings.state_supervision_violation_response_severity_highest,
    state_enum_strings.state_supervision_violation_response_severity_second_highest,
    state_enum_strings.state_supervision_violation_response_severity_third_highest,
    state_enum_strings.state_supervision_violation_response_severity_fourth_highest,
    state_enum_strings.state_supervision_violation_response_severity_fifth_highest,
    state_enum_strings.state_supervision_violation_response_severity_sixth_highest,
    state_enum_strings.state_supervision_violation_response_severity_seventh_highest,
    state_enum_strings.internal_unknown,
    state_enum_strings.external_unknown,
    name="state_supervision_violation_response_severity",
)

state_program_assignment_participation_status = Enum(
    state_enum_strings.present_without_info,
    state_enum_strings.state_program_assignment_participation_status_deceased,
    state_enum_strings.state_program_assignment_participation_status_denied,
    state_enum_strings.state_program_assignment_participation_status_discharged_successful,
    state_enum_strings.state_program_assignment_participation_status_discharged_successful_with_discretion,
    state_enum_strings.state_program_assignment_participation_status_discharged_unsuccessful,
    state_enum_strings.state_program_assignment_participation_status_discharged_other,
    state_enum_strings.state_program_assignment_participation_status_discharged_unknown,
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
    state_enum_strings.state_specialized_purpose_for_incarceration_safekeeping,
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
    state_enum_strings.state_custodial_authority_county,
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
    state_enum_strings.state_supervision_contact_location_alternative_place_of_employment,
    state_enum_strings.internal_unknown,
    state_enum_strings.external_unknown,
    name="state_supervision_contact_location",
)

state_supervision_contact_status = Enum(
    state_enum_strings.state_supervision_contact_status_attempted,
    state_enum_strings.state_supervision_contact_status_completed,
    state_enum_strings.state_supervision_contact_status_scheduled,
    state_enum_strings.present_without_info,
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

state_scheduled_supervision_contact_location = Enum(
    state_enum_strings.state_scheduled_supervision_contact_location_court,
    state_enum_strings.state_scheduled_supervision_contact_location_field,
    state_enum_strings.state_scheduled_supervision_contact_location_jail,
    state_enum_strings.state_scheduled_supervision_contact_location_place_of_employment,
    state_enum_strings.state_scheduled_supervision_contact_location_residence,
    state_enum_strings.state_scheduled_supervision_contact_location_supervision_office,
    state_enum_strings.state_scheduled_supervision_contact_location_treatment_provider,
    state_enum_strings.state_scheduled_supervision_contact_location_law_enforcement_agency,
    state_enum_strings.state_scheduled_supervision_contact_location_parole_commission,
    state_enum_strings.state_scheduled_supervision_contact_location_alternative_place_of_employment,
    state_enum_strings.internal_unknown,
    state_enum_strings.external_unknown,
    name="state_scheduled_supervision_contact_location",
)

state_scheduled_supervision_contact_status = Enum(
    state_enum_strings.state_scheduled_supervision_contact_status_deleted,
    state_enum_strings.state_scheduled_supervision_contact_status_scheduled,
    state_enum_strings.present_without_info,
    state_enum_strings.internal_unknown,
    state_enum_strings.external_unknown,
    name="state_scheduled_supervision_contact_status",
)

state_scheduled_supervision_contact_reason = Enum(
    state_enum_strings.state_scheduled_supervision_contact_reason_emergency_contact,
    state_enum_strings.state_scheduled_supervision_contact_reason_general_contact,
    state_enum_strings.state_scheduled_supervision_contact_reason_initial_contact,
    state_enum_strings.internal_unknown,
    state_enum_strings.external_unknown,
    name="state_scheduled_supervision_contact_reason",
)

state_scheduled_supervision_contact_type = Enum(
    state_enum_strings.state_scheduled_supervision_contact_type_collateral,
    state_enum_strings.state_scheduled_supervision_contact_type_direct,
    state_enum_strings.state_scheduled_supervision_contact_type_both_collateral_and_direct,
    state_enum_strings.internal_unknown,
    state_enum_strings.external_unknown,
    name="state_scheduled_supervision_contact_type",
)

state_scheduled_supervision_contact_method = Enum(
    state_enum_strings.state_scheduled_supervision_contact_method_in_person,
    state_enum_strings.state_scheduled_supervision_contact_method_telephone,
    state_enum_strings.state_scheduled_supervision_contact_method_virtual,
    state_enum_strings.state_scheduled_supervision_contact_method_written_message,
    state_enum_strings.internal_unknown,
    state_enum_strings.external_unknown,
    name="state_scheduled_supervision_contact_method",
)

state_employment_period_employment_status = Enum(
    state_enum_strings.state_employment_period_employment_status_alternate_income_source,
    state_enum_strings.state_employment_period_employment_status_employed_unknown_amount,
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

state_task_type = Enum(
    state_enum_strings.state_task_type_appeal_for_transfer_to_supervision_from_incarceration,
    state_enum_strings.state_task_type_arrest_check,
    state_enum_strings.state_task_type_supervision_case_plan_update,
    state_enum_strings.state_task_type_discharge_early_from_supervision,
    state_enum_strings.state_task_type_discharge_from_incarceration,
    state_enum_strings.state_task_type_discharge_from_incarceration_min,
    state_enum_strings.state_task_type_discharge_from_supervision,
    state_enum_strings.state_task_type_drug_screen,
    state_enum_strings.state_task_type_employment_verification,
    state_enum_strings.state_task_type_face_to_face_contact,
    state_enum_strings.state_task_type_home_visit,
    state_enum_strings.state_task_type_new_assessment,
    state_enum_strings.state_task_type_payment_verification,
    state_enum_strings.state_task_type_special_condition_verification,
    state_enum_strings.state_task_type_transfer_to_administrative_supervision,
    state_enum_strings.state_task_type_transfer_to_supervision_from_incarceration,
    state_enum_strings.state_task_type_treatment_referral,
    state_enum_strings.state_task_type_treatment_verification,
    state_enum_strings.state_task_type_parole_hearing,
    state_enum_strings.internal_unknown,
    state_enum_strings.external_unknown,
    name="state_task_type",
)

state_staff_role_type = Enum(
    state_enum_strings.state_staff_role_type_supervision_officer,
    state_enum_strings.state_staff_role_type_reentry_officer,
    state_enum_strings.internal_unknown,
    state_enum_strings.external_unknown,
    name="state_staff_role_type",
)

state_staff_role_subtype = Enum(
    state_enum_strings.state_staff_role_subtype_supervision_officer,
    state_enum_strings.state_staff_role_subtype_supervision_officer_supervisor,
    state_enum_strings.state_staff_role_subtype_supervision_regional_manager,
    state_enum_strings.state_staff_role_subtype_supervision_district_manager,
    state_enum_strings.state_staff_role_subtype_supervision_state_leadership,
    state_enum_strings.internal_unknown,
    state_enum_strings.external_unknown,
    name="state_staff_role_subtype",
)

state_staff_caseload_type = Enum(
    state_enum_strings.state_staff_caseload_type_sex_offense,
    state_enum_strings.state_staff_caseload_type_administrative_supervision,
    state_enum_strings.state_staff_caseload_type_alcohol_and_drug,
    state_enum_strings.state_staff_caseload_type_intensive,
    state_enum_strings.state_staff_caseload_type_mental_health,
    state_enum_strings.state_staff_caseload_type_electronic_monitoring,
    state_enum_strings.state_staff_caseload_type_other_court,
    state_enum_strings.state_staff_caseload_type_drug_court,
    state_enum_strings.state_staff_caseload_type_veterans_court,
    state_enum_strings.state_staff_caseload_type_community_facility,
    state_enum_strings.state_staff_caseload_type_domestic_violence,
    state_enum_strings.state_staff_caseload_type_transitional,
    state_enum_strings.state_staff_caseload_type_other,
    state_enum_strings.state_staff_caseload_type_general,
    state_enum_strings.internal_unknown,
    state_enum_strings.external_unknown,
    name="state_staff_caseload_type",
)

state_system_type = Enum(
    state_enum_strings.state_system_type_incarceration,
    state_enum_strings.state_system_type_supervision,
    state_enum_strings.internal_unknown,
    name="state_system_type",
)

state_person_staff_relationship_type = Enum(
    state_enum_strings.state_person_staff_relationship_type_case_manager,
    state_enum_strings.state_person_staff_relationship_type_supervising_officer,
    state_enum_strings.internal_unknown,
    name="state_person_staff_relationship_type",
)

# Association tables for many-to-many relationships.
# See different examples of your use case with our version of SQLAlchemy here:
# https://docs.sqlalchemy.org/en/14/orm/basic_relationships.html#many-to-many
state_charge_v2_state_sentence_association_table = Table(
    "state_charge_v2_state_sentence_association",
    StateBase.metadata,
    Column(
        "charge_v2_id", Integer, ForeignKey("state_charge_v2.charge_v2_id"), index=True
    ),
    Column(
        "sentence_id", Integer, ForeignKey("state_sentence.sentence_id"), index=True
    ),
)
# TODO(#26240): Update usage of state_charge with state_charge_v2
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
    def __new__(cls: Any, *_: Any, **__: Any) -> Any:
        if cls is _ReferencesStatePersonSharedColumns:
            raise NotImplementedError(f"[{cls}] cannot be instantiated")
        return super().__new__(cls)  # type: ignore

    @declared_attr
    def person_id(self) -> Column:
        return Column(
            Integer,
            ForeignKey("state_person.person_id", deferrable=True, initially="DEFERRED"),
            index=True,
            nullable=False,
        )


class StatePersonAddressPeriod(StateBase, _ReferencesStatePersonSharedColumns):
    """Represents a historical ledger for when a given person had a given address."""

    __tablename__ = "state_person_address_period"

    person_address_period_id = Column(Integer, primary_key=True)

    state_code = Column(String(255), nullable=False, index=True)

    address_line_1 = Column(String(255))

    address_line_2 = Column(String(255))

    address_city = Column(String(255))

    address_state = Column(String(255))

    address_country = Column(String(255))

    address_zip = Column(String(255))

    address_county = Column(String(255))

    full_address = Column(String(255))

    address_start_date = Column(Date)

    address_end_date = Column(Date)

    address_is_verified = Column(Boolean)

    address_type = Column(state_person_address_type, nullable=False)

    address_type_raw_text = Column(String(255))

    address_metadata = Column(String(255))


class StatePersonHousingStatusPeriod(StateBase, _ReferencesStatePersonSharedColumns):
    """Represents a StatePersonHousingStatusPeriod in the SQL schema"""

    __tablename__ = "state_person_housing_status_period"

    person_housing_status_period_id = Column(Integer, primary_key=True)

    state_code = Column(String(255), nullable=False, index=True)

    housing_status_start_date = Column(Date)

    housing_status_end_date = Column(Date)

    housing_status_type = Column(state_person_housing_status_type, nullable=False)

    housing_status_type_raw_text = Column(String(255))


class StatePersonExternalId(StateBase, _ReferencesStatePersonSharedColumns):
    """Represents a StatePersonExternalId in the SQL schema"""

    __tablename__ = "state_person_external_id"
    __table_args__: tuple = (
        UniqueConstraint(
            "state_code",
            "id_type",
            "external_id",
            name="person_external_ids_unique_within_type_and_region",
            deferrable=True,
            initially="DEFERRED",
        ),
        {},
    )

    person_external_id_id = Column(Integer, primary_key=True)

    external_id = Column(String(255), nullable=False, index=True)
    state_code = Column(String(255), nullable=False, index=True)
    id_type = Column(String(255), nullable=False)
    is_current_display_id_for_type = Column(Boolean)
    is_stable_id_for_type = Column(Boolean)
    id_active_from_datetime = Column(DateTime)
    id_active_to_datetime = Column(DateTime)


class StatePersonAlias(StateBase, _ReferencesStatePersonSharedColumns):
    """Represents a StatePersonAlias in the SQL schema"""

    __tablename__ = "state_person_alias"

    person_alias_id = Column(Integer, primary_key=True)

    state_code = Column(String(255), nullable=False, index=True)
    full_name = Column(String(255), nullable=False)
    alias_type = Column(state_person_alias_type)
    alias_type_raw_text = Column(String(255))


class StatePersonRace(StateBase, _ReferencesStatePersonSharedColumns):
    """Represents a StatePersonRace in the SQL schema"""

    __tablename__ = "state_person_race"

    person_race_id = Column(Integer, primary_key=True)

    state_code = Column(String(255), nullable=False, index=True)
    race = Column(state_race, nullable=False)
    race_raw_text = Column(String(255))


class StatePersonEthnicity(StateBase, _ReferencesStatePersonSharedColumns):
    """Represents a state person in the SQL schema"""

    __tablename__ = "state_person_ethnicity"

    person_ethnicity_id = Column(Integer, primary_key=True)

    state_code = Column(String(255), nullable=False, index=True)
    ethnicity = Column(state_ethnicity, nullable=False)
    ethnicity_raw_text = Column(String(255))


class StatePerson(StateBase):
    """Represents a StatePerson in the state SQL schema"""

    __tablename__ = "state_person"

    person_id = Column(Integer, primary_key=True)

    state_code = Column(String(255), nullable=False, index=True)

    current_address = Column(Text)

    full_name = Column(String(255), index=True)

    birthdate = Column(Date, index=True)

    gender = Column(state_gender)
    gender_raw_text = Column(String(255))

    sex = Column(state_sex)
    sex_raw_text = Column(String(255))

    residency_status = Column(state_residency_status)
    residency_status_raw_text = Column(String(255))

    current_email_address = Column(Text)
    current_phone_number = Column(Text)

    external_ids = relationship(
        "StatePersonExternalId", backref="person", lazy="selectin"
    )
    aliases = relationship("StatePersonAlias", backref="person", lazy="selectin")
    races = relationship("StatePersonRace", backref="person", lazy="selectin")
    ethnicities = relationship(
        "StatePersonEthnicity", backref="person", lazy="selectin"
    )
    assessments = relationship("StateAssessment", backref="person", lazy="selectin")
    sentences = relationship("StateSentence", backref="person", lazy="selectin")
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
    scheduled_supervision_contacts = relationship(
        "StateScheduledSupervisionContact", backref="person", lazy="selectin"
    )
    employment_periods = relationship(
        "StateEmploymentPeriod", backref="person", lazy="selectin"
    )
    drug_screens = relationship("StateDrugScreen", backref="person", lazy="selectin")
    task_deadlines = relationship(
        "StateTaskDeadline", backref="person", lazy="selectin"
    )
    address_periods = relationship(
        "StatePersonAddressPeriod", backref="person", lazy="selectin"
    )
    housing_status_periods = relationship(
        "StatePersonHousingStatusPeriod", backref="person", lazy="selectin"
    )
    sentence_groups = relationship(
        "StateSentenceGroup", backref="person", lazy="selectin"
    )
    staff_relationship_periods = relationship(
        "StatePersonStaffRelationshipPeriod", backref="person", lazy="selectin"
    )


class StateCharge(StateBase, _ReferencesStatePersonSharedColumns):
    """Represents a StateCharge in the SQL schema"""

    __tablename__ = "state_charge"
    __table_args__: tuple = (
        UniqueConstraint(
            "state_code",
            "external_id",
            name="charge_external_ids_unique_within_state",
            deferrable=True,
            initially="DEFERRED",
        ),
        {},
    )

    charge_id = Column(Integer, primary_key=True)

    external_id = Column(String(255), nullable=False, index=True)
    status = Column(state_charge_status, nullable=False)
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
    is_sex_offense = Column(Boolean)
    is_drug = Column(Boolean)
    counts = Column(Integer)
    charge_notes = Column(Text)
    charging_entity = Column(String(255))
    is_controlling = Column(Boolean)
    judge_full_name = Column(String(255))
    judge_external_id = Column(String(255))
    judicial_district_code = Column(String(255))

    # Cross-entity relationships
    person = relationship("StatePerson", uselist=False)


class StateAssessment(StateBase, _ReferencesStatePersonSharedColumns):
    """Represents a StateAssessment in the SQL schema"""

    __tablename__ = "state_assessment"
    __table_args__ = tuple(
        CheckConstraint(
            "(conducting_staff_external_id IS NULL AND conducting_staff_external_id_type IS NULL) OR (conducting_staff_external_id IS NOT NULL AND conducting_staff_external_id_type IS NOT NULL)",
            name="conducting_staff_external_id_fields_consistent",
        ),
    )

    assessment_id = Column(Integer, primary_key=True)

    external_id = Column(String(255), nullable=False, index=True)
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
    conducting_staff_external_id = Column(String(255))
    conducting_staff_external_id_type = Column(String(255))


class StateSupervisionSentence(StateBase, _ReferencesStatePersonSharedColumns):
    """Represents a StateSupervisionSentence in the SQL schema"""

    __tablename__ = "state_supervision_sentence"

    supervision_sentence_id = Column(Integer, primary_key=True)

    external_id = Column(String(255), nullable=False, index=True)
    status = Column(state_sentence_status, nullable=False)
    status_raw_text = Column(String(255))
    supervision_type = Column(state_supervision_sentence_supervision_type)
    supervision_type_raw_text = Column(String(255))
    date_imposed = Column(Date)
    effective_date = Column(Date)
    projected_completion_date = Column(Date)
    completion_date = Column(Date)
    is_life = Column(Boolean)
    state_code = Column(String(255), nullable=False, index=True)
    county_code = Column(String(255), index=True)
    min_length_days = Column(Integer)
    max_length_days = Column(Integer)
    sentence_metadata = Column(Text)
    # This field can contain an arbitrarily long list of conditions, so we do not restrict the length of the string like
    # we do for most other String fields.
    conditions = Column(Text)

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
    __table_args__: tuple = (
        UniqueConstraint(
            "state_code",
            "external_id",
            name="incarceration_sentence_external_ids_unique_within_state",
            deferrable=True,
            initially="DEFERRED",
        ),
        {},
    )

    incarceration_sentence_id = Column(Integer, primary_key=True)

    external_id = Column(String(255), nullable=False, index=True)
    status = Column(state_sentence_status, nullable=False)
    status_raw_text = Column(String(255))
    incarceration_type = Column(state_incarceration_type)
    incarceration_type_raw_text = Column(String(255))
    date_imposed = Column(Date)
    effective_date = Column(Date)
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
    sentence_metadata = Column(Text)
    # This field can contain an arbitrarily long list of conditions, so we do not restrict the length of the string like
    # we do for most other String fields.
    conditions = Column(Text)

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
    __table_args__: tuple = (
        UniqueConstraint(
            "state_code",
            "external_id",
            name="incarceration_period_external_ids_unique_within_state",
            deferrable=True,
            initially="DEFERRED",
        ),
        {},
    )
    incarceration_period_id = Column(Integer, primary_key=True)

    external_id = Column(String(255), nullable=False, index=True)
    incarceration_type = Column(state_incarceration_type)
    incarceration_type_raw_text = Column(String(255))
    admission_date = Column(Date)
    release_date = Column(Date)
    state_code = Column(String(255), nullable=False, index=True)
    county_code = Column(String(255), index=True)
    facility = Column(String(255))
    housing_unit = Column(String(255))
    housing_unit_category = Column(state_incarceration_period_housing_unit_category)
    housing_unit_category_raw_text = Column(String(255))
    housing_unit_type = Column(state_incarceration_period_housing_unit_type)
    housing_unit_type_raw_text = Column(String(255))
    admission_reason = Column(state_incarceration_period_admission_reason)
    admission_reason_raw_text = Column(String(255))
    release_reason = Column(state_incarceration_period_release_reason)
    release_reason_raw_text = Column(String(255))
    custody_level = Column(state_incarceration_period_custody_level)
    custody_level_raw_text = Column(String(255))
    specialized_purpose_for_incarceration = Column(
        state_specialized_purpose_for_incarceration
    )
    specialized_purpose_for_incarceration_raw_text = Column(String(255))
    custodial_authority = Column(state_custodial_authority)
    custodial_authority_raw_text = Column(String(255))


class StateSupervisionPeriod(StateBase, _ReferencesStatePersonSharedColumns):
    """Represents a StateSupervisionPeriod in the SQL schema"""

    __tablename__ = "state_supervision_period"
    __table_args__ = tuple(
        CheckConstraint(
            "(supervising_officer_staff_external_id IS NULL AND supervising_officer_staff_external_id_type IS NULL) OR (supervising_officer_staff_external_id IS NOT NULL AND supervising_officer_staff_external_id_type IS NOT NULL)",
            name="supervising_officer_staff_external_id_fields_consistent",
        ),
    )

    supervision_period_id = Column(Integer, primary_key=True)

    external_id = Column(String(255), nullable=False, index=True)
    supervision_type = Column(state_supervision_period_supervision_type)
    supervision_type_raw_text = Column(String(255))
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
    supervision_period_metadata = Column(String(255))

    # This field can contain an arbitrarily long list of conditions, so we do not restrict the length of the string like
    # we do for most other String fields.
    conditions = Column(Text)
    custodial_authority = Column(state_custodial_authority)
    custodial_authority_raw_text = Column(String(255))
    supervising_officer_staff_external_id = Column(String(255))
    supervising_officer_staff_external_id_type = Column(String(255))

    case_type_entries = relationship(
        "StateSupervisionCaseTypeEntry", backref="supervision_period", lazy="selectin"
    )


class StateSupervisionCaseTypeEntry(StateBase, _ReferencesStatePersonSharedColumns):
    """Represents a StateSupervisionCaseTypeEntry in the SQL schema"""

    __tablename__ = "state_supervision_case_type_entry"

    supervision_case_type_entry_id = Column(Integer, primary_key=True)

    case_type = Column(state_supervision_case_type, nullable=False)
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

    person = relationship("StatePerson", uselist=False)


class StateIncarcerationIncident(StateBase, _ReferencesStatePersonSharedColumns):
    """Represents a StateIncarcerationIncident in the SQL schema"""

    __tablename__ = "state_incarceration_incident"
    __table_args__: tuple = (
        UniqueConstraint(
            "state_code",
            "external_id",
            name="incarceration_incident_external_ids_unique_within_state",
            deferrable=True,
            initially="DEFERRED",
        ),
        {},
    )

    incarceration_incident_id = Column(Integer, primary_key=True)

    external_id = Column(String(255), nullable=False, index=True)
    incident_type = Column(state_incarceration_incident_type)
    incident_type_raw_text = Column(String(255))
    incident_severity = Column(state_incarceration_incident_severity)
    incident_severity_raw_text = Column(String(255))
    incident_date = Column(Date)
    state_code = Column(String(255), nullable=False, index=True)
    facility = Column(String(255))
    location_within_facility = Column(String(255))
    incident_details = Column(Text)
    incident_metadata = Column(Text)

    incarceration_incident_outcomes = relationship(
        "StateIncarcerationIncidentOutcome",
        backref="incarceration_incident",
        lazy="selectin",
    )


class StateIncarcerationIncidentOutcome(StateBase, _ReferencesStatePersonSharedColumns):
    """Represents a StateIncarcerationIncidentOutcome in the SQL schema"""

    __tablename__ = "state_incarceration_incident_outcome"
    __table_args__: tuple = (
        UniqueConstraint(
            "state_code",
            "external_id",
            name="incarceration_incident_outcome_external_ids_unique_within_state",
            deferrable=True,
            initially="DEFERRED",
        ),
        {},
    )

    incarceration_incident_outcome_id = Column(Integer, primary_key=True)

    external_id = Column(String(255), nullable=False, index=True)
    outcome_type = Column(state_incarceration_incident_outcome_type)
    outcome_type_raw_text = Column(String(255))
    state_code = Column(String(255), nullable=False, index=True)
    date_effective = Column(Date)
    projected_end_date = Column(Date)
    hearing_date = Column(Date)
    report_date = Column(Date)
    outcome_description = Column(String(255))
    punishment_length_days = Column(Integer)
    outcome_metadata = Column(Text)

    @declared_attr
    def incarceration_incident_id(self) -> Column:
        return Column(
            Integer,
            ForeignKey("state_incarceration_incident.incarceration_incident_id"),
            index=True,
            nullable=True,
        )

    person = relationship("StatePerson", uselist=False)


class StateSupervisionViolationTypeEntry(
    StateBase, _ReferencesStatePersonSharedColumns
):
    """Represents a StateSupervisionViolationTypeEntry in the SQL schema."""

    __tablename__ = "state_supervision_violation_type_entry"

    supervision_violation_type_entry_id = Column(Integer, primary_key=True)

    state_code = Column(String(255), nullable=False, index=True)
    violation_type = Column(state_supervision_violation_type, nullable=False)
    violation_type_raw_text = Column(String(255))

    @declared_attr
    def supervision_violation_id(self) -> Column:
        return Column(
            Integer,
            ForeignKey("state_supervision_violation.supervision_violation_id"),
            index=True,
            nullable=True,
        )

    person = relationship("StatePerson", uselist=False)


class StateSupervisionViolatedConditionEntry(
    StateBase, _ReferencesStatePersonSharedColumns
):
    """Represents a StateSupervisionViolatedConditionEntry in the SQL schema."""

    __tablename__ = "state_supervision_violated_condition_entry"

    supervision_violated_condition_entry_id = Column(Integer, primary_key=True)

    state_code = Column(String(255), nullable=False, index=True)

    condition = Column(state_supervision_violated_condition_type, nullable=False)

    condition_raw_text = Column(String(255))

    @declared_attr
    def supervision_violation_id(self) -> Column:
        return Column(
            Integer,
            ForeignKey("state_supervision_violation.supervision_violation_id"),
            index=True,
            nullable=True,
        )

    person = relationship("StatePerson", uselist=False)


class StateSupervisionViolation(StateBase, _ReferencesStatePersonSharedColumns):
    """Represents a StateSupervisionViolation in the SQL schema"""

    __tablename__ = "state_supervision_violation"
    __table_args__: tuple = (
        UniqueConstraint(
            "state_code",
            "external_id",
            name="supervision_violation_external_ids_unique_within_state",
            deferrable=True,
            initially="DEFERRED",
        ),
        {},
    )

    supervision_violation_id = Column(Integer, primary_key=True)

    external_id = Column(String(255), nullable=False, index=True)

    violation_date = Column(Date)
    state_code = Column(String(255), nullable=False, index=True)
    is_violent = Column(Boolean)
    is_sex_offense = Column(Boolean)
    violation_metadata = Column(Text)
    violation_severity = Column(state_supervision_violation_severity)
    violation_severity_raw_text = Column(String(255))

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

    supervision_violation_response_decision_entry_id = Column(Integer, primary_key=True)

    state_code = Column(String(255), nullable=False, index=True)
    decision = Column(state_supervision_violation_response_decision, nullable=False)
    decision_raw_text = Column(String(255))

    @declared_attr
    def supervision_violation_response_id(self) -> Column:
        return Column(
            Integer,
            ForeignKey(
                "state_supervision_violation_response.supervision_violation_response_id"
            ),
            index=True,
            nullable=True,
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
        CheckConstraint(
            "(deciding_staff_external_id IS NULL AND deciding_staff_external_id_type IS NULL) OR (deciding_staff_external_id IS NOT NULL AND deciding_staff_external_id_type IS NOT NULL)",
            name="deciding_staff_external_id_fields_consistent",
        ),
    )

    supervision_violation_response_id = Column(Integer, primary_key=True)

    external_id = Column(String(255), nullable=False, index=True)
    response_type = Column(state_supervision_violation_response_type)
    response_type_raw_text = Column(String(255))
    response_subtype = Column(String(255))
    response_date = Column(Date)
    state_code = Column(String(255), nullable=False, index=True)
    deciding_body_type = Column(state_supervision_violation_response_deciding_body_type)
    deciding_body_type_raw_text = Column(String(255))
    deciding_staff_external_id = Column(String(255))
    deciding_staff_external_id_type = Column(String(255))
    violation_response_severity = Column(state_supervision_violation_response_severity)
    violation_response_severity_raw_text = Column(String(255))

    is_draft = Column(Boolean)
    violation_response_metadata = Column(Text)

    @declared_attr
    def supervision_violation_id(self) -> Column:
        return Column(
            Integer,
            ForeignKey("state_supervision_violation.supervision_violation_id"),
            index=True,
            nullable=True,
        )

    person = relationship("StatePerson", uselist=False)
    supervision_violation_response_decisions = relationship(
        "StateSupervisionViolationResponseDecisionEntry",
        backref="supervision_violation_response",
        lazy="selectin",
    )


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
        CheckConstraint(
            "(referring_staff_external_id IS NULL AND referring_staff_external_id_type IS NULL) OR (referring_staff_external_id IS NOT NULL AND referring_staff_external_id_type IS NOT NULL)",
            name="referring_staff_external_id_fields_consistent",
        ),
    )

    program_assignment_id = Column(Integer, primary_key=True)

    external_id = Column(String(255), nullable=False, index=True)
    state_code = Column(String(255), nullable=False, index=True)
    # TODO(#2450): Switch program_id/location_id for a program foreign key once
    # we've ingested program information into our schema.
    program_id = Column(String(255))
    program_location_id = Column(String(255))

    participation_status = Column(
        state_program_assignment_participation_status, nullable=False
    )
    participation_status_raw_text = Column(String(255))
    referral_date = Column(Date)
    start_date = Column(Date)
    discharge_date = Column(Date)
    referral_metadata = Column(Text)
    referring_staff_external_id = Column(String(255))
    referring_staff_external_id_type = Column(String(255))


class StateEarlyDischarge(StateBase, _ReferencesStatePersonSharedColumns):
    """Represents a StateEarlyDischarge in the SQL schema."""

    __tablename__ = "state_early_discharge"
    __table_args__: tuple = (
        UniqueConstraint(
            "state_code",
            "external_id",
            name="early_discharge_external_ids_unique_within_state",
            deferrable=True,
            initially="DEFERRED",
        ),
        {},
    )

    early_discharge_id = Column(Integer, primary_key=True)

    external_id = Column(String(255), nullable=False, index=True)
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
        CheckConstraint(
            "(contacting_staff_external_id IS NULL AND contacting_staff_external_id_type IS NULL) OR (contacting_staff_external_id IS NOT NULL AND contacting_staff_external_id_type IS NOT NULL)",
            name="contacting_staff_external_id_fields_consistent",
        ),
    )

    supervision_contact_id = Column(Integer, primary_key=True)

    external_id = Column(String(255), nullable=False, index=True)
    state_code = Column(String(255), nullable=False, index=True)

    contact_date = Column(Date)
    contact_datetime = Column(DateTime)
    contact_reason = Column(state_supervision_contact_reason)
    contact_reason_raw_text = Column(String(255))
    contact_type = Column(state_supervision_contact_type)
    contact_type_raw_text = Column(String(255))
    contact_method = Column(state_supervision_contact_method)
    contact_method_raw_text = Column(String(255))
    contacting_staff_external_id = Column(String(255))
    contacting_staff_external_id_type = Column(String(255))
    location = Column(state_supervision_contact_location)
    location_raw_text = Column(String(255))
    resulted_in_arrest = Column(Boolean)
    status = Column(state_supervision_contact_status, nullable=False)
    status_raw_text = Column(String(255))
    verified_employment = Column(Boolean)

    supervision_contact_metadata = Column(Text)


class StateScheduledSupervisionContact(StateBase, _ReferencesStatePersonSharedColumns):
    """Represents a StateScheduledSupervisionContact in the SQL schema."""

    __tablename__ = "state_scheduled_supervision_contact"
    __table_args__ = (
        UniqueConstraint(
            "state_code",
            "external_id",
            name="scheduled_supervision_contact_external_ids_unique_within_state",
            deferrable=True,
            initially="DEFERRED",
        ),
        UniqueConstraint(
            "state_code",
            "contact_method",
            "contacting_staff_external_id",
            "contacting_staff_external_id_type",
            "update_datetime",
            name="state_scheduled_supervision_contact_unique_per_person_staff_method_update_date",
            deferrable=True,
            initially="DEFERRED",
        ),
        CheckConstraint(
            "(contacting_staff_external_id IS NULL AND contacting_staff_external_id_type IS NULL) OR (contacting_staff_external_id IS NOT NULL AND contacting_staff_external_id_type IS NOT NULL)",
            name="contacting_staff_external_id_fields_consistent",
        ),
    )

    scheduled_supervision_contact_id = Column(Integer, primary_key=True)

    external_id = Column(String(255), nullable=False, index=True)
    state_code = Column(String(255), nullable=False, index=True)

    scheduled_contact_date = Column(Date)
    scheduled_contact_datetime = Column(DateTime)
    update_datetime = Column(DateTime)
    contact_reason = Column(state_scheduled_supervision_contact_reason)
    contact_reason_raw_text = Column(String(255))
    contact_type = Column(state_scheduled_supervision_contact_type)
    contact_type_raw_text = Column(String(255))
    contact_method = Column(state_scheduled_supervision_contact_method)
    contact_method_raw_text = Column(String(255))
    contacting_staff_external_id = Column(String(255))
    contacting_staff_external_id_type = Column(String(255))
    location = Column(state_scheduled_supervision_contact_location)
    location_raw_text = Column(String(255))
    status = Column(state_scheduled_supervision_contact_status, nullable=False)
    status_raw_text = Column(String(255))
    sequence_num = Column(Integer, nullable=False)
    contact_meeting_address = Column(String(255))

    scheduled_supervision_contact_metadata = Column(Text)


class StateEmploymentPeriod(StateBase, _ReferencesStatePersonSharedColumns):
    """Represents a StateEmploymentPeriod in the SQL schema."""

    __tablename__ = "state_employment_period"
    __table_args__: tuple = (
        UniqueConstraint(
            "state_code",
            "external_id",
            name="employment_period_external_ids_unique_within_state",
            deferrable=True,
            initially="DEFERRED",
        ),
        {},
    )

    employment_period_id = Column(Integer, primary_key=True)

    external_id = Column(String(255), nullable=False, index=True)
    state_code = Column(String(255), nullable=False, index=True)

    employment_status = Column(state_employment_period_employment_status)
    employment_status_raw_text = Column(String(255))

    start_date = Column(Date, nullable=False)
    end_date = Column(Date)
    last_verified_date = Column(Date)

    employer_name = Column(String(255))
    employer_address = Column(String(255))
    job_title = Column(String(255))

    end_reason = Column(state_employment_period_end_reason)
    end_reason_raw_text = Column(String(255))


class StateDrugScreen(StateBase, _ReferencesStatePersonSharedColumns):
    """Represents a StateDrugScreen in the SQL schema."""

    __tablename__ = "state_drug_screen"
    __table_args__: tuple = (
        UniqueConstraint(
            "state_code",
            "external_id",
            name="state_drug_screen_external_ids_unique_within_state",
            deferrable=True,
            initially="DEFERRED",
        ),
        {},
    )

    drug_screen_id = Column(Integer, primary_key=True)
    external_id = Column(String(255), nullable=False, index=True)
    state_code = Column(String(255), nullable=False, index=True)
    drug_screen_date = Column(Date, nullable=False)
    drug_screen_result = Column(state_drug_screen_result)
    drug_screen_result_raw_text = Column(String(255))
    sample_type = Column(state_drug_screen_sample_type)
    sample_type_raw_text = Column(String(255))

    drug_screen_metadata = Column(Text)


class StateTaskDeadline(StateBase, _ReferencesStatePersonSharedColumns):
    """Represents a StateTaskDeadline in the SQL schema."""

    __tablename__ = "state_task_deadline"
    __table_args__ = tuple(
        CheckConstraint(
            "eligible_date IS NULL OR due_date IS NULL OR eligible_date <= due_date",
            name="eligible_date_before_due_date",
        ),
    )

    task_deadline_id = Column(Integer, primary_key=True)
    state_code = Column(String(255), nullable=False, index=True)

    task_type = Column(state_task_type, nullable=False)
    task_type_raw_text = Column(String(255))

    task_subtype = Column(String(255))

    eligible_date = Column(Date)
    due_date = Column(Date)

    update_datetime = Column(DateTime, nullable=False)

    task_metadata = Column(Text)
    sequence_num = Column(Integer, nullable=True)


class StateStaff(StateBase):
    """Represents a StateStaff in the SQL schema"""

    __tablename__ = "state_staff"

    staff_id = Column(Integer, primary_key=True)

    state_code = Column(String(255), nullable=False, index=True)
    full_name = Column(String(255))
    email = Column(String(255))
    phone_number = Column(String(255))

    # Cross-entity relationships
    external_ids = relationship(
        "StateStaffExternalId", backref="staff", lazy="selectin"
    )
    role_periods = relationship(
        "StateStaffRolePeriod", backref="staff", lazy="selectin"
    )
    supervisor_periods = relationship(
        "StateStaffSupervisorPeriod", backref="staff", lazy="selectin"
    )
    location_periods = relationship(
        "StateStaffLocationPeriod", backref="staff", lazy="selectin"
    )
    caseload_type_periods = relationship(
        "StateStaffCaseloadTypePeriod", backref="staff", lazy="selectin"
    )


# Shared mixin columns
class _ReferencesStateStaffSharedColumns:
    """A mixin which defines columns for any table whose rows reference an
    individual StateStaff"""

    # Consider this class a mixin and only allow instantiating subclasses
    def __new__(cls: Any, *_: Any, **__: Any) -> Any:
        if cls is _ReferencesStateStaffSharedColumns:
            raise NotImplementedError(f"[{cls}] cannot be instantiated")
        return super().__new__(cls)  # type: ignore

    @declared_attr
    def staff_id(self) -> Column:
        return Column(
            Integer,
            ForeignKey("state_staff.staff_id", deferrable=True, initially="DEFERRED"),
            index=True,
            nullable=False,
        )


class StateStaffExternalId(StateBase, _ReferencesStateStaffSharedColumns):
    """Represents a StateStaffExternalId in the SQL schema"""

    __tablename__ = "state_staff_external_id"
    __table_args__: tuple = (
        UniqueConstraint(
            "state_code",
            "id_type",
            "external_id",
            name="staff_external_ids_unique_within_type_and_region",
            deferrable=True,
            initially="DEFERRED",
        ),
        {},
    )

    staff_external_id_id = Column(Integer, primary_key=True)

    external_id = Column(String(255), nullable=False, index=True)
    state_code = Column(String(255), nullable=False, index=True)
    id_type = Column(String(255), nullable=False)


class StateStaffRolePeriod(StateBase, _ReferencesStateStaffSharedColumns):
    """Represents a StateStaffRolePeriod in the SQL schema"""

    __tablename__ = "state_staff_role_period"
    __table_args__: tuple = (
        UniqueConstraint(
            "state_code",
            "external_id",
            name="staff_role_periods_unique_within_region",
            deferrable=True,
            initially="DEFERRED",
        ),
        {},
    )

    staff_role_period_id = Column(Integer, primary_key=True)

    external_id = Column(String(255), nullable=False, index=True)
    state_code = Column(String(255), nullable=False, index=True)
    start_date = Column(Date, nullable=False)
    end_date = Column(Date)
    role_type = Column(state_staff_role_type, nullable=False)
    role_type_raw_text = Column(String(255))
    role_subtype = Column(state_staff_role_subtype)
    role_subtype_raw_text = Column(String(255))


class StateStaffSupervisorPeriod(StateBase, _ReferencesStateStaffSharedColumns):
    """Represents a StateStaffSupervisorPeriod in the SQL schema"""

    __tablename__ = "state_staff_supervisor_period"
    __table_args__: tuple = (
        UniqueConstraint(
            "state_code",
            "external_id",
            name="staff_supervisor_periods_unique_within_region",
            deferrable=True,
            initially="DEFERRED",
        ),
        {},
    )

    staff_supervisor_period_id = Column(Integer, primary_key=True)

    external_id = Column(String(255), nullable=False, index=True)
    state_code = Column(String(255), nullable=False, index=True)
    start_date = Column(Date, nullable=False)
    end_date = Column(Date)
    supervisor_staff_external_id = Column(String(255), nullable=False)
    supervisor_staff_external_id_type = Column(String(255), nullable=False)


class StateStaffLocationPeriod(StateBase, _ReferencesStateStaffSharedColumns):
    """Represents a StateStaffLocationPeriod in the SQL schema"""

    __tablename__ = "state_staff_location_period"
    __table_args__: tuple = (
        UniqueConstraint(
            "state_code",
            "external_id",
            name="staff_location_periods_unique_within_region",
            deferrable=True,
            initially="DEFERRED",
        ),
        {},
    )

    staff_location_period_id = Column(Integer, primary_key=True)

    external_id = Column(String(255), nullable=False, index=True)
    state_code = Column(String(255), nullable=False, index=True)
    start_date = Column(Date, nullable=False)
    end_date = Column(Date)
    location_external_id = Column(String(255), nullable=False)


class StateStaffCaseloadTypePeriod(StateBase, _ReferencesStateStaffSharedColumns):
    """Represents a StateStaffCaseloadTypePeriod in the SQL schema"""

    __tablename__ = "state_staff_caseload_type_period"

    staff_caseload_type_period_id = Column(Integer, primary_key=True)

    external_id = Column(String(255), nullable=False, index=True)

    state_code = Column(String(255), nullable=False, index=True)

    caseload_type = Column(state_staff_caseload_type, nullable=False)

    caseload_type_raw_text = Column(String(255))

    start_date = Column(Date, nullable=False)
    end_date = Column(Date)


class StateSentence(StateBase, _ReferencesStatePersonSharedColumns):
    """Represents a StateSentence in the SQL schema"""

    __tablename__ = "state_sentence"
    __table_args__: tuple = (
        UniqueConstraint(
            "state_code",
            "external_id",
            name="sentence_external_ids_unique_within_state",
            deferrable=True,
            initially="DEFERRED",
        ),
        {},
    )
    sentence_id = Column(Integer, primary_key=True)
    state_code = Column(String(255), nullable=False, index=True)
    external_id = Column(String(255), nullable=False, index=True)

    sentence_group_external_id = Column(String, nullable=True)

    imposed_date = Column(Date, nullable=False)
    current_state_provided_start_date = Column(Date, nullable=True)

    initial_time_served_days = Column(Integer, nullable=True)

    sentence_type = Column(state_sentence_type, nullable=False)

    # The class of authority imposing this sentence: COUNTY, STATE, etc.
    # A value of COUNTY means a county court imposed this sentence.
    # Only optional for parsing. We expect this to exist once entities are merged up.
    sentencing_authority = Column(state_sentencing_authority, nullable=False)

    sentencing_authority_raw_text = Column(String, nullable=True)

    sentence_type_raw_text = Column(String, nullable=True)

    is_life = Column(Boolean, nullable=True)

    is_capital_punishment = Column(Boolean, nullable=True)

    parole_possible = Column(Boolean, nullable=True)

    county_code = Column(String, nullable=True)

    parent_sentence_external_id_array = Column(String, nullable=True)

    conditions = Column(String, nullable=True)

    sentence_metadata = Column(String, nullable=True)

    # Cross-entity relationships
    charges = relationship(
        "StateChargeV2",
        secondary=state_charge_v2_state_sentence_association_table,
        # This argument is the name of the attr in the other SQLAlchemy model,
        # so the "sentences" attr in the StateChargeV2 model
        back_populates="sentences",
        lazy="selectin",
    )
    sentence_status_snapshots = relationship(
        "StateSentenceStatusSnapshot", backref="sentence", lazy="selectin"
    )
    sentence_lengths = relationship(
        "StateSentenceLength", backref="sentence", lazy="selectin"
    )


class StateChargeV2(StateBase, _ReferencesStatePersonSharedColumns):
    """Represents a StateCharge in the SQL schema"""

    # TODO(#26240): Replace StateCharge with this model

    __tablename__ = "state_charge_v2"
    __table_args__: tuple = (
        UniqueConstraint(
            "state_code",
            "external_id",
            name="charge_v2_external_ids_unique_within_state",
            deferrable=True,
            initially="DEFERRED",
        ),
        {},
    )

    charge_v2_id = Column(Integer, primary_key=True)

    external_id = Column(String(255), nullable=False, index=True)
    status = Column(state_charge_v2_status, nullable=False)
    status_raw_text = Column(String(255))
    offense_date = Column(Date)
    date_charged = Column(Date)
    state_code = Column(String(255), nullable=False, index=True)
    county_code = Column(String(255), index=True)
    ncic_code = Column(String(255))
    statute = Column(String(255))
    description = Column(Text)
    attempted = Column(Boolean)
    classification_type = Column(state_charge_v2_classification_type)
    classification_type_raw_text = Column(String(255))
    classification_subtype = Column(String(255))
    offense_type = Column(String(255))
    is_violent = Column(Boolean)
    is_sex_offense = Column(Boolean)
    is_drug = Column(Boolean)
    counts = Column(Integer)
    charge_notes = Column(Text)
    charging_entity = Column(String(255))
    is_controlling = Column(Boolean)
    judge_full_name = Column(String(255))
    judge_external_id = Column(String(255))
    judicial_district_code = Column(String(255))

    # Cross-entity relationships
    person = relationship("StatePerson", uselist=False)
    sentences = relationship(
        "StateSentence",
        secondary=state_charge_v2_state_sentence_association_table,
        # This argument is the name of the attr in the other SQLAlchemy model,
        # so the "charges" attr in the StateSentence model.
        back_populates="charges",
        lazy="selectin",
    )


class StateSentenceStatusSnapshot(StateBase, _ReferencesStatePersonSharedColumns):
    """Represents a historical ledger for when a given sentence had a given status."""

    __tablename__ = "state_sentence_status_snapshot"

    sentence_status_snapshot_id = Column(Integer, primary_key=True)
    state_code = Column(String(255), nullable=False, index=True)
    status_update_datetime = Column(DateTime, nullable=False)
    status = Column(state_sentence_status, nullable=False)

    status_raw_text = Column(String(255), nullable=True)
    sentence_id = Column(
        Integer,
        ForeignKey("state_sentence.sentence_id", deferrable=True, initially="DEFERRED"),
    )
    sequence_num = Column(Integer, nullable=True)

    # Cross-entity relationships
    person = relationship("StatePerson", uselist=False)


class StateSentenceLength(StateBase, _ReferencesStatePersonSharedColumns):
    """Represents a historical ledger for when a given sentence had a given status."""

    __tablename__ = "state_sentence_length"

    sentence_length_id = Column(Integer, primary_key=True)
    state_code = Column(String(255), nullable=False, index=True)
    sentence_id = Column(
        Integer,
        ForeignKey("state_sentence.sentence_id", deferrable=True, initially="DEFERRED"),
    )
    length_update_datetime = Column(DateTime, nullable=False)
    sentence_length_days_min = Column(Integer, nullable=True)
    sentence_length_days_max = Column(Integer, nullable=True)
    good_time_days = Column(Integer, nullable=True)
    earned_time_days = Column(Integer, nullable=True)
    parole_eligibility_date_external = Column(Date, nullable=True)
    projected_parole_release_date_external = Column(Date, nullable=True)
    projected_completion_date_min_external = Column(Date, nullable=True)
    projected_completion_date_max_external = Column(Date, nullable=True)
    sequence_num = Column(Integer, nullable=True)

    # Cross-entity relationships
    person = relationship("StatePerson", uselist=False)


class StateSentenceGroup(StateBase, _ReferencesStatePersonSharedColumns):
    """
    Represents a logical grouping of sentences that encompass an
    individual's interactions with a department of corrections.
    It begins with an individual's first sentence imposition and ends at liberty.
    This is a state agnostic term used by Recidiviz for a state
    specific administrative phenomena.
    """

    __tablename__ = "state_sentence_group"
    external_id = Column(String(255), nullable=False, index=True)
    state_code = Column(String(255), nullable=False, index=True)
    sentence_group_id = Column(Integer, primary_key=True)
    sentence_group_lengths = relationship(
        "StateSentenceGroupLength", backref="sentence_group", lazy="selectin"
    )


class StateSentenceGroupLength(StateBase, _ReferencesStatePersonSharedColumns):
    """Represents a historical ledger of attributes relating to a state designated group of sentences."""

    __tablename__ = "state_sentence_group_length"

    sentence_group_length_id = Column(Integer, primary_key=True)
    sentence_group_id = Column(
        Integer,
        ForeignKey(
            "state_sentence_group.sentence_group_id",
            deferrable=True,
            initially="DEFERRED",
        ),
    )
    state_code = Column(String(255), nullable=False, index=True)
    group_update_datetime = Column(DateTime, nullable=False)
    parole_eligibility_date_external = Column(Date, nullable=True)
    projected_parole_release_date_external = Column(Date, nullable=True)
    projected_full_term_release_date_min_external = Column(Date, nullable=True)
    projected_full_term_release_date_max_external = Column(Date, nullable=True)
    sequence_num = Column(Integer, nullable=True)
    sentence_group_length_metadata = Column(Text)

    # Cross-entity relationships
    person = relationship("StatePerson", uselist=False)


class StatePersonStaffRelationshipPeriod(
    StateBase, _ReferencesStatePersonSharedColumns
):
    """The StatePersonStaffRelationshipPeriod object represents a period of time during
    which a staff member has a defined relationships with a justice impacted individual.
    """

    __tablename__ = "state_person_staff_relationship_period"
    __table_args__ = (
        {
            "comment": (
                "Defines a period of time during which some staff member has a defined "
                "relationship with a justice impacted individual."
            )
        },
    )
    person_staff_relationship_period_id = Column(Integer, primary_key=True)

    state_code = Column(String(255), nullable=False, index=True)

    system_type = Column(state_system_type, nullable=False)
    system_type_raw_text = Column(String(255))

    relationship_type = Column(state_person_staff_relationship_type, nullable=False)
    relationship_type_raw_text = Column(String(255))

    location_external_id = Column(String(255))

    relationship_start_date = Column(Date, nullable=False)
    relationship_end_date_exclusive = Column(Date)

    associated_staff_external_id = Column(String(255), nullable=False)
    associated_staff_external_id_type = Column(String(255), nullable=False)

    relationship_priority = Column(Integer)
