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
"""A script for generating one LookML view for each state schema table"""

from types import ModuleType

from recidiviz.looker.lookml_field_registry import LookMLFieldRegistry
from recidiviz.looker.lookml_view_field import LookMLViewField
from recidiviz.persistence.entity.state import entities as state_entities
from recidiviz.persistence.entity.state import normalized_entities
from recidiviz.tools.looker.entity.entity_lookml_field_factory import (
    EntityLookMLFieldFactory,
)

DEFAULT_FIELDS: list[LookMLViewField] = [EntityLookMLFieldFactory.count_measure()]
STATE_CUSTOM_FIELDS: dict[str, list[LookMLViewField]] = {
    "state_incarceration_period": [
        EntityLookMLFieldFactory.person_id_with_open_period_indicator(
            "state_incarceration_period", "release_date"
        )
    ],
    "state_supervision_period": [
        EntityLookMLFieldFactory.person_id_with_open_period_indicator(
            "state_supervision_period", "termination_date"
        )
    ],
    "state_supervision_sentence": [
        EntityLookMLFieldFactory.average_measure("max_length_days"),
        EntityLookMLFieldFactory.sum_measure("max_length_days"),
    ],
    "state_incarceration_sentence": [
        EntityLookMLFieldFactory.average_measure("earned_time_days"),
        EntityLookMLFieldFactory.sum_measure("earned_time_days"),
    ],
    "state_incarceration_incident_outcome": [
        EntityLookMLFieldFactory.average_measure("punishment_length_days"),
        EntityLookMLFieldFactory.sum_measure("punishment_length_days"),
        EntityLookMLFieldFactory.count_measure(
            drill_fields=[
                "person_id",
                "state_code",
                "hearing_date",
                "date_effective_date",
                "punishment_length_days",
                "outcome_type",
                "outcome_type_raw_text",
                "outcome_description",
                "incarceration_incident_outcome_id",
            ]
        ),
    ],
    "state_assessment": [
        EntityLookMLFieldFactory.average_measure("assessment_score"),
        EntityLookMLFieldFactory.sum_measure("assessment_score"),
    ],
    "state_charge_v2": [
        EntityLookMLFieldFactory.average_measure("counts"),
        EntityLookMLFieldFactory.sum_measure("counts"),
        EntityLookMLFieldFactory.count_measure(
            drill_fields=["charge_v2_id", "description"]
        ),
    ],
    "state_supervision_violation_type_entry": [
        EntityLookMLFieldFactory.count_measure(
            drill_fields=[
                "person_id",
                "state_code",
                "violation_type",
                "violation_type_raw_text",
                "supervision_violation_type_entry_id",
            ]
        ),
    ],
    "state_supervision_violated_condition_entry": [
        EntityLookMLFieldFactory.count_measure(
            drill_fields=["person_id", "state_code", "condition"]
        ),
    ],
    "state_supervision_case_type_entry": [
        EntityLookMLFieldFactory.count_measure(
            drill_fields=[
                "person_id",
                "state_code",
                "case_type",
                "case_type_raw_text",
                "supervision_period_id",
            ]
        ),
    ],
    "state_person_ethnicity": [
        EntityLookMLFieldFactory.count_measure(
            drill_fields=[
                "person_ethnicity_id",
                "state_code",
                "ethnicity",
                "ethnicity_raw_text",
            ]
        ),
    ],
    "state_person_alias": [
        EntityLookMLFieldFactory.count_measure(
            drill_fields=[
                "person_alias_id",
                "state_code",
                "full_name",
                "alias_type",
                "alias_type_raw_text",
            ]
        ),
    ],
    "state_person_race": [
        EntityLookMLFieldFactory.count_measure(
            drill_fields=["person_id", "state_code", "race", "race_raw_text"]
        ),
    ],
    "state_task_deadline": [
        EntityLookMLFieldFactory.count_field_measure(field="due_date"),
        EntityLookMLFieldFactory.count_field_measure(field="eligible_date"),
        EntityLookMLFieldFactory.count_task_deadline_no_date(),
    ],
    "state_person": [
        EntityLookMLFieldFactory.full_name_clean(),
        EntityLookMLFieldFactory.actions(),
        EntityLookMLFieldFactory.count_measure(
            ["person_id", "state_code", "full_name"]
        ),
    ],
    "state_person_external_id": [
        EntityLookMLFieldFactory.id_array(),
        EntityLookMLFieldFactory.external_id_with_type(),
        EntityLookMLFieldFactory.count_measure(
            drill_fields=["state_code", "external_id", "id_type"]
        ),
    ],
    "state_program_assignment": [
        EntityLookMLFieldFactory.referrals_array(),
    ],
}


def get_custom_field_registry_for_entity_module(
    entities_module: ModuleType,
) -> LookMLFieldRegistry:
    """Returns the custom field registry for the given entity module."""
    if entities_module == state_entities:
        return LookMLFieldRegistry(
            default_fields=DEFAULT_FIELDS, table_fields=STATE_CUSTOM_FIELDS
        )
    if entities_module == normalized_entities:
        # TODO(#39355) Add custom fields for normalized entities
        return LookMLFieldRegistry(
            default_fields=DEFAULT_FIELDS, table_fields=STATE_CUSTOM_FIELDS
        )

    raise ValueError(f"Unsupported entities module: [{entities_module}]")
