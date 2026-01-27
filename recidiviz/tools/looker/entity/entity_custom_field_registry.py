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

from collections import defaultdict
from types import ModuleType

from recidiviz.looker.lookml_field_registry import LookMLFieldRegistry
from recidiviz.looker.lookml_view_field import LookMLViewField
from recidiviz.persistence.entity.state import entities as state_entities
from recidiviz.persistence.entity.state import normalized_entities
from recidiviz.tools.looker.entity.entity_lookml_field_factory import (
    EntityLookMLFieldFactory,
)

DEFAULT_FIELDS: list[LookMLViewField] = [EntityLookMLFieldFactory.count_measure()]
COMMON_CUSTOM_FIELDS: dict[str, list[LookMLViewField]] = {
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
        EntityLookMLFieldFactory.count_measure(
            ["person_id", "state_code", "full_name"]
        ),
    ],
    "state_person_external_id": [
        EntityLookMLFieldFactory.external_id_with_type(),
        EntityLookMLFieldFactory.list_field_measure("external_id_with_type"),
        EntityLookMLFieldFactory.count_measure(
            drill_fields=["state_code", "external_id", "id_type"]
        ),
    ],
    "state_program_assignment": [
        EntityLookMLFieldFactory.referrals_array(),
    ],
}
STATE_CUSTOM_FIELDS: dict[str, list[LookMLViewField]] = {
    "state_incarceration_period": [
        EntityLookMLFieldFactory.person_id_with_open_period_indicator(
            view_name="state_incarceration_period",
            period_end_date_field="release_date",
        )
    ],
    "state_supervision_period": [
        EntityLookMLFieldFactory.person_id_with_open_period_indicator(
            view_name="state_supervision_period",
            period_end_date_field="termination_date",
        )
    ],
    "state_person": [
        EntityLookMLFieldFactory.actions(
            root_entity_view_name="state_person",
            external_id_entity_view_name="state_person_external_id",
        )
    ],
}
NORMALIZED_STATE_CUSTOM_FIELDS: dict[str, list[LookMLViewField]] = {
    "state_charge": [
        # Hide duplicated columns that are represented as *_external in normalized state_charge
        EntityLookMLFieldFactory.hidden_dimension("is_drug"),
        EntityLookMLFieldFactory.hidden_dimension("is_sex_offense"),
        EntityLookMLFieldFactory.hidden_dimension("is_violent"),
        EntityLookMLFieldFactory.hidden_dimension("ncic_code"),
    ],
    "state_incarceration_period": [
        EntityLookMLFieldFactory.person_id_with_open_period_indicator(
            view_name="normalized_state_incarceration_period",
            period_end_date_field="release_date",
        )
    ],
    "state_supervision_period": [
        EntityLookMLFieldFactory.person_id_with_open_period_indicator(
            view_name="normalized_state_supervision_period",
            period_end_date_field="termination_date",
        )
    ],
    "state_person": [
        EntityLookMLFieldFactory.actions(
            root_entity_view_name="normalized_state_person",
            external_id_entity_view_name="normalized_state_person_external_id",
        )
    ],
}


def get_custom_field_registry_for_entity_module(
    entities_module: ModuleType,
) -> LookMLFieldRegistry:
    """Returns the custom field registry for the given entity module."""

    def merge_dicts(dict1: dict[str, list], dict2: dict[str, list]) -> dict[str, list]:
        merged = defaultdict(list)

        for key, value in dict1.items():
            merged[key].extend(value)
        for key, value in dict2.items():
            merged[key].extend(value)

        return dict(merged)

    if entities_module == state_entities:
        return LookMLFieldRegistry(
            default_fields=DEFAULT_FIELDS,
            table_fields=merge_dicts(COMMON_CUSTOM_FIELDS, STATE_CUSTOM_FIELDS),
        )
    if entities_module == normalized_entities:
        return LookMLFieldRegistry(
            default_fields=DEFAULT_FIELDS,
            table_fields=merge_dicts(
                COMMON_CUSTOM_FIELDS, NORMALIZED_STATE_CUSTOM_FIELDS
            ),
        )

    raise ValueError(f"Unsupported entities module: [{entities_module}]")
