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
"""Provides custom LookML fields for a state entity."""
import attr

from recidiviz.big_query.big_query_schema_validator import BigQuerySchemaValidator
from recidiviz.common import attr_validators
from recidiviz.looker.lookml_field_factory import LookMLFieldFactory
from recidiviz.looker.lookml_field_registry import LookMLFieldRegistry
from recidiviz.looker.lookml_view_field import LookMLViewField
from recidiviz.tools.looker.entity.entity_lookml_field_factory import (
    EntityLookMLFieldFactory,
)


@attr.define
class StateEntityLookMLCustomFieldProvider:
    """
    Class to provide custom LookML fields for a state entity. Schema fields referenced in
    custom lookml fields are validated to ensure they exist in the table's schema.

    Attributes:
        field_validator (SchemaFieldValidator): Validates that fields that are
            referenced in a custom field exist in the table's schema.
        _registry (LookMLFieldRegistry): Registry for custom LookML fields lookup.
        _field_definitions (Dict[str, List[LookMLViewField]]): Custom field definition
            map for table_id -> custom fields.
    """

    field_validator: BigQuerySchemaValidator = attr.ib(
        validator=attr.validators.instance_of(BigQuerySchemaValidator)
    )
    _registry: LookMLFieldRegistry = attr.ib(
        validator=attr.validators.instance_of(LookMLFieldRegistry), init=False
    )
    _field_definitions: dict[str, list[LookMLViewField]] = attr.ib(
        validator=attr_validators.is_dict, init=False
    )

    def __attrs_post_init__(self) -> None:
        self._registry = LookMLFieldRegistry(default_fields=self._default_fields)
        self._field_definitions = self._validate_and_create_fields()
        self._initialize_registry()

    def _validate_and_create_fields(self) -> dict[str, list[LookMLViewField]]:
        """Validates and creates custom LookML fields for state entities.
        Any referenced schema fields that are not found in the table's schema will raise an error.
        """
        return {
            "state_incarceration_period": [
                EntityLookMLFieldFactory.person_id_with_open_period_indicator(
                    table_id="state_incarceration_period",
                    period_end_date_field=self._validate_field(
                        table_id="state_incarceration_period", field="release_date"
                    ),
                )
            ],
            "state_supervision_period": [
                EntityLookMLFieldFactory.person_id_with_open_period_indicator(
                    table_id="state_supervision_period",
                    period_end_date_field=self._validate_field(
                        table_id="state_supervision_period", field="termination_date"
                    ),
                )
            ],
            "state_supervision_sentence": [
                EntityLookMLFieldFactory.average_measure(
                    field=self._validate_field(
                        table_id="state_supervision_sentence", field="max_length_days"
                    )
                ),
                EntityLookMLFieldFactory.sum_measure(
                    field=self._validate_field(
                        table_id="state_supervision_sentence", field="max_length_days"
                    )
                ),
            ],
            "state_incarceration_sentence": [
                EntityLookMLFieldFactory.average_measure(
                    field=self._validate_field(
                        table_id="state_incarceration_sentence",
                        field="earned_time_days",
                    )
                ),
                EntityLookMLFieldFactory.sum_measure(
                    field=self._validate_field(
                        table_id="state_incarceration_sentence",
                        field="earned_time_days",
                    )
                ),
            ],
            "state_incarceration_incident_outcome": [
                EntityLookMLFieldFactory.average_measure(
                    field=self._validate_field(
                        table_id="state_incarceration_incident_outcome",
                        field="punishment_length_days",
                    )
                ),
                EntityLookMLFieldFactory.sum_measure(
                    field=self._validate_field(
                        table_id="state_incarceration_incident_outcome",
                        field="punishment_length_days",
                    )
                ),
                EntityLookMLFieldFactory.count_measure(
                    drill_fields=self._validate_fields(
                        table_id="state_incarceration_incident_outcome",
                        fields=[
                            "person_id",
                            "state_code",
                            "hearing_date",
                            "date_effective",
                            "punishment_length_days",
                            "outcome_type",
                            "outcome_type_raw_text",
                            "outcome_description",
                            "incarceration_incident_outcome_id",
                        ],
                    )
                ),
            ],
            "state_assessment": [
                EntityLookMLFieldFactory.average_measure(
                    field=self._validate_field(
                        table_id="state_assessment", field="assessment_score"
                    )
                ),
                EntityLookMLFieldFactory.sum_measure(
                    field=self._validate_field(
                        table_id="state_assessment", field="assessment_score"
                    )
                ),
            ],
            "state_charge_v2": [
                EntityLookMLFieldFactory.average_measure(
                    field=self._validate_field(
                        table_id="state_charge_v2", field="counts"
                    )
                ),
                EntityLookMLFieldFactory.sum_measure(
                    field=self._validate_field(
                        table_id="state_charge_v2", field="counts"
                    )
                ),
                EntityLookMLFieldFactory.count_measure(
                    drill_fields=self._validate_fields(
                        table_id="state_charge_v2",
                        fields=["charge_v2_id", "description"],
                    )
                ),
            ],
            "state_supervision_violation_type_entry": [
                EntityLookMLFieldFactory.count_measure(
                    drill_fields=self._validate_fields(
                        table_id="state_supervision_violation_type_entry",
                        fields=[
                            "person_id",
                            "state_code",
                            "violation_type",
                            "violation_type_raw_text",
                            "supervision_violation_type_entry_id",
                        ],
                    )
                ),
            ],
            "state_supervision_violated_condition_entry": [
                EntityLookMLFieldFactory.count_measure(
                    drill_fields=self._validate_fields(
                        table_id="state_supervision_violated_condition_entry",
                        fields=["person_id", "state_code", "condition"],
                    )
                ),
            ],
            "state_supervision_case_type_entry": [
                EntityLookMLFieldFactory.count_measure(
                    drill_fields=self._validate_fields(
                        table_id="state_supervision_case_type_entry",
                        fields=[
                            "person_id",
                            "state_code",
                            "case_type",
                            "case_type_raw_text",
                            "supervision_period_id",
                        ],
                    )
                ),
            ],
            "state_person_ethnicity": [
                EntityLookMLFieldFactory.count_measure(
                    drill_fields=self._validate_fields(
                        table_id="state_person_ethnicity",
                        fields=[
                            "person_ethnicity_id",
                            "state_code",
                            "ethnicity",
                            "ethnicity_raw_text",
                        ],
                    )
                ),
            ],
            "state_person_alias": [
                EntityLookMLFieldFactory.count_measure(
                    drill_fields=self._validate_fields(
                        table_id="state_person_alias",
                        fields=[
                            "person_alias_id",
                            "state_code",
                            "full_name",
                            "alias_type",
                            "alias_type_raw_text",
                        ],
                    )
                ),
            ],
            "state_person_race": [
                EntityLookMLFieldFactory.count_measure(
                    drill_fields=self._validate_fields(
                        table_id="state_person_race",
                        fields=["person_id", "state_code", "race", "race_raw_text"],
                    )
                ),
            ],
            "state_task_deadline": [
                EntityLookMLFieldFactory.count_field_measure(
                    field=self._validate_field(
                        table_id="state_task_deadline", field="due_date"
                    )
                ),
                EntityLookMLFieldFactory.count_field_measure(
                    field=self._validate_field(
                        table_id="state_task_deadline", field="eligible_date"
                    )
                ),
                EntityLookMLFieldFactory.count_task_deadline_no_date(),
            ],
            "state_person": [
                EntityLookMLFieldFactory.full_name_clean(),
                EntityLookMLFieldFactory.actions(),
                EntityLookMLFieldFactory.count_measure(
                    drill_fields=self._validate_fields(
                        table_id="state_person",
                        fields=["person_id", "state_code", "full_name"],
                    )
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

    @property
    def _default_fields(self) -> list[LookMLViewField]:
        """Returns the default LookML fields for a state entity."""
        return [LookMLFieldFactory.count_measure()]

    def _validate_fields(self, table_id: str, fields: list[str]) -> list[str]:
        """Validates multiple fields are found in their table's schema fields."""
        return self.field_validator.validate_table_contains_fields(table_id, fields)

    def _validate_field(self, table_id: str, field: str) -> str:
        """Validates a single field is found in its table's schema fields."""
        return self.field_validator.validate_table_contains_field(table_id, field)

    def _initialize_registry(self) -> None:
        """Initializes the LookML field registry with custom fields."""
        for table_id, fields in self._field_definitions.items():
            self._registry.register(table_id, fields)

    def get(self, table_id: str) -> list[LookMLViewField]:
        """Returns the custom LookML fields for a given table_id."""
        return self._registry.get(table_id)
