# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2024 Recidiviz, Inc.
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
"""Tests the classes defined in normalized_entities_v2.py"""

import datetime
import unittest
from typing import ForwardRef, List, Set, Type

import attr

from recidiviz.common.attr_mixins import (
    CachedAttributeInfo,
    attribute_field_type_reference_for_class,
)
from recidiviz.common.attr_utils import (
    get_non_optional_type,
    get_referenced_class_name_from_type,
    is_flat_field,
    is_list_type,
    is_optional_type,
)
from recidiviz.common.attr_validators import IsListOfValidator, IsOptionalValidator
from recidiviz.common.constants.state.state_person_staff_relationship_period import (
    StatePersonStaffRelationshipType,
)
from recidiviz.common.constants.state.state_sentence import (
    StateSentenceStatus,
    StateSentenceType,
    StateSentencingAuthority,
)
from recidiviz.common.constants.state.state_system_type import StateSystemType
from recidiviz.common.constants.states import StateCode
from recidiviz.persistence.entity.base_entity import Entity
from recidiviz.persistence.entity.entities_module_context_factory import (
    entities_module_context_for_module,
)
from recidiviz.persistence.entity.entity_utils import get_all_entity_classes_in_module
from recidiviz.persistence.entity.normalized_entities_utils import (
    update_entity_with_globally_unique_id,
)
from recidiviz.persistence.entity.state import entities as state_entities
from recidiviz.persistence.entity.state import normalized_entities
from recidiviz.persistence.entity.state.entity_field_validators import (
    ParsingOptionalOnlyValidator,
)
from recidiviz.persistence.entity.state.normalized_entities import (
    EntityBackedgeValidator,
)
from recidiviz.persistence.entity.state.normalized_state_entity import (
    NormalizedStateEntity,
)
from recidiviz.utils.types import non_optional

NORMALIZED_PREFIX = "Normalized"

# Fields that are present in state entities but not in normalized entities.
# TODO(#15559): Add NormalizedStateCharge / NormalizedStateChargeV2 fields that we
#  plan to delete here.
STATE_DATASET_ONLY_FIELDS = {
    state_entities.StateSentence.__name__: {
        "current_state_provided_start_date",
    }
}

NORMALIZATION_ONLY_ENTITIES = {
    normalized_entities.NormalizedStateSentenceInferredGroup.__name__,
    normalized_entities.NormalizedStateSentenceImposedGroup.__name__,
}


class TestNormalizedEntities(unittest.TestCase):
    """Tests the classes defined in normalized_entities_v2.py"""

    def setUp(self) -> None:
        self.normalized_entity_classes: List[Type[Entity]] = list(
            get_all_entity_classes_in_module(normalized_entities)
        )

        self.normalized_entity_classes_by_name = {
            c.__name__: c for c in self.normalized_entity_classes
        }

        entities_module_context = entities_module_context_for_module(
            normalized_entities
        )
        self.direction_checker = entities_module_context.direction_checker()

    def _assert_valid_default(
        self,
        entity_class: Type[Entity],
        field_name: str,
        field_info: CachedAttributeInfo,
    ) -> None:
        """Assert that the given field has an appropriate default value set."""
        entity_class_name = entity_class.__name__
        field_type = non_optional(field_info.attribute.type)
        field_default = field_info.attribute.default
        has_default = field_default != attr.NOTHING
        if is_optional_type(field_type):
            if has_default:
                self.assertIsNone(
                    field_default,
                    f"Found invalid default for optional field [{field_name}]: "
                    f"{field_name}. Should be `None`.",
                )
            return

        if is_list_type(field_type):
            default_is_list_factory = field_default is not None and (
                hasattr(field_default, "factory") and field_default.factory is list
            )
            if not default_is_list_factory:
                raise ValueError(
                    f"Found list field [{field_name}] on class "
                    f"[{entity_class_name}] with invalid default "
                    f"value set: {field_info.attribute.default}."
                    f"All list fields should have `factory=list` defaults "
                    f"set.",
                )
            return

        is_backedge_field = self.direction_checker.is_back_edge(
            entity_class, field_name
        )
        # Default fields can be None for backedges or explicit uses of attr.Factory
        if (is_backedge_field and field_default is None) or isinstance(
            field_default, attr.Factory  # type: ignore
        ):
            default_ok = True
        else:
            default_ok = not has_default

        if not default_ok:
            raise ValueError(
                f"Found non-optional [{field_name}] on class "
                f"[{entity_class_name}] with invalid default "
                f"value set: {field_info.attribute.default}",
            )

    def _assert_valid_validator(
        self,
        entity_class: Type[Entity],
        field_name: str,
        field_info: CachedAttributeInfo,
    ) -> None:
        """Assert that the given field has an appropriate validator set."""
        entity_class_name = entity_class.__name__
        field_validator = field_info.attribute.validator
        if not field_validator:
            self.fail(
                f"Found field [{field_name}] on class [{entity_class_name}] which does "
                f"not have a validator"
            )
        field_type = non_optional(field_info.attribute.type)
        if not is_optional_type(field_type):
            self.assertFalse(
                isinstance(field_validator, IsOptionalValidator),
                f"Found non-optional [{field_name}] on class [{entity_class_name}] "
                f"which has a validator that allows for optional values.",
            )

        is_backedge_field = self.direction_checker.is_back_edge(
            entity_class, field_name
        )

        if is_backedge_field:
            # Backedges use EntityBackedgeValidator which also validate that the
            # value is a list.
            self.assertTrue(
                isinstance(field_validator, EntityBackedgeValidator),
                f"Found backedge field [{field_name}] on class [{entity_class_name}] "
                f"which does not use an EntityBackedgeValidator validator.",
            )
        elif is_list_type(field_type):
            self.assertTrue(
                isinstance(field_validator, IsListOfValidator),
                f"Found list field [{field_name}] on class [{entity_class_name}] "
                f"which does not use an is_list_of() validator.",
            )

    def test_normalized_entities_correct_setup(self) -> None:
        for normalized_entity_class in self.normalized_entity_classes:
            normalized_entity_class_name = normalized_entity_class.__name__
            normalized_entity_class_ref = attribute_field_type_reference_for_class(
                normalized_entity_class
            )
            self.assertTrue(
                issubclass(normalized_entity_class, Entity),
                f"Class [{normalized_entity_class}] is not a subclass of Entity",
            )
            self.assertTrue(
                issubclass(normalized_entity_class, NormalizedStateEntity),
                f"Class [{normalized_entity_class}] is not a subclass of "
                f"NormalizedStateEntity",
            )
            self.assertTrue(
                normalized_entity_class_name.startswith(NORMALIZED_PREFIX),
                f"Class [{normalized_entity_class_name}] does not start with the "
                f"{NORMALIZED_PREFIX} prefix",
            )
            for field_name in normalized_entity_class_ref.fields:
                normalized_field_info = normalized_entity_class_ref.get_field_info(
                    field_name
                )
                if normalized_field_info.referenced_cls_name:
                    self.assertTrue(
                        normalized_field_info.referenced_cls_name.startswith(
                            NORMALIZED_PREFIX
                        ),
                        f"Referenced class [{normalized_field_info.referenced_cls_name}] "
                        f"is not a Normalized* entity",
                    )

                self._assert_valid_default(
                    entity_class=normalized_entity_class,
                    field_name=field_name,
                    field_info=normalized_field_info,
                )
                self._assert_valid_validator(
                    entity_class=normalized_entity_class,
                    field_name=field_name,
                    field_info=normalized_field_info,
                )
                field_type = non_optional(normalized_field_info.attribute.type)
                primary_key_col_name = normalized_entity_class.get_class_id_name()
                if field_name == primary_key_col_name:
                    self.assertFalse(
                        is_optional_type(field_type),
                        f"Found primary key [{field_name}] which has optional type "
                        f"[{field_type}]",
                    )
                    self.assertEqual(int, get_non_optional_type(field_type))

    def test_state_entities_and_normalized_entities_parity(self) -> None:
        entity_classes: Set[Type[Entity]] = get_all_entity_classes_in_module(
            state_entities
        )

        for entity_class in entity_classes:
            entity_class_name = entity_class.__name__
            entity_class_reference = attribute_field_type_reference_for_class(
                entity_class
            )
            entity_fields = entity_class_reference.fields

            normalized_entity_class_name = f"{NORMALIZED_PREFIX}{entity_class_name}"
            if (
                normalized_entity_class_name
                not in self.normalized_entity_classes_by_name
            ):
                raise ValueError(
                    f"Did not find class [{normalized_entity_class_name}]. Expected to "
                    f"find a normalized entity corresponding to [{entity_class_name}]."
                )

            normalized_entity_class = self.normalized_entity_classes_by_name[
                normalized_entity_class_name
            ]

            normalized_entity_class_reference = (
                attribute_field_type_reference_for_class(normalized_entity_class)
            )
            normalized_entity_fields = normalized_entity_class_reference.fields
            state_dataset_only_fields = STATE_DATASET_ONLY_FIELDS.get(
                entity_class.__name__, set()
            )

            missing_normalized_entity_fields = (
                set(entity_fields)
                - state_dataset_only_fields
                - set(normalized_entity_fields)
            )
            if missing_normalized_entity_fields:
                raise ValueError(
                    f"Found fields on class [{entity_class_name}] which are not "
                    f"present on [{normalized_entity_class_name}]: "
                    f"{missing_normalized_entity_fields}"
                )

            for field_name in normalized_entity_fields:
                normalized_field_info = (
                    normalized_entity_class_reference.get_field_info(field_name)
                )
                # We don't check if the field is for an entity created only in normalization.
                if (
                    normalized_field_info.referenced_cls_name
                    in NORMALIZATION_ONLY_ENTITIES
                ):
                    continue
                if field_name not in entity_fields:
                    # This is a new field only present on the normalized entity
                    attribute = normalized_field_info.attribute
                    if not is_flat_field(attribute):
                        raise ValueError(
                            "Only flat fields are supported as additional fields on "
                            f"NormalizedStateEntities. Found: {attribute} in field "
                            f"{field_name}."
                        )

                    continue
                entity_field_info = entity_class_reference.get_field_info(field_name)
                self.assertEqual(
                    normalized_field_info.field_type,
                    entity_field_info.field_type,
                    f"Field type for field [{field_name}] differs on class "
                    f"[{normalized_entity_class_name}] and [{entity_class_name}]",
                )
                self.assertEqual(
                    normalized_field_info.enum_cls, entity_field_info.enum_cls
                )

                entity_field_type = non_optional(entity_field_info.attribute.type)
                entity_type_is_optional = is_optional_type(entity_field_type)
                normalized_entity_field_type = non_optional(
                    normalized_field_info.attribute.type
                )

                normalized_entity_type_is_optional = is_optional_type(
                    normalized_entity_field_type
                )

                if not entity_type_is_optional:
                    self.assertFalse(
                        normalized_entity_type_is_optional,
                        f"Field [{field_name}] is non-optional on the "
                        f"{entity_class_name} but optional on "
                        f"{normalized_entity_class_name} ",
                    )

                self._check_field_types(
                    field_name=field_name,
                    entity_class_name=entity_class_name,
                    entity_field_type=entity_field_type,
                    normalized_entity_class_name=normalized_entity_class_name,
                    normalized_entity_field_type=normalized_entity_field_type,
                )

                entity_validator = entity_field_info.attribute.validator
                normalized_entity_validator = normalized_field_info.attribute.validator

                if isinstance(entity_validator, ParsingOptionalOnlyValidator):
                    self.assertFalse(
                        isinstance(
                            normalized_entity_validator,
                            IsOptionalValidator,
                        ),
                        f"Found [{field_name}] which has a validator that "
                        f"allows for optional values even though it is marked as "
                        f"parsing_opt_only() in state/entities.py.",
                    )
                if not issubclass(normalized_entity_class, Entity):
                    raise ValueError(
                        f"Expected normalized entity class [{normalized_entity_class}] "
                        f"to be subclass of Entity"
                    )

                missing_global_constraints = set(
                    entity_class.global_unique_constraints()
                ) - set(normalized_entity_class.global_unique_constraints())
                if missing_global_constraints:
                    raise ValueError(
                        f"Found global constraints defined on [{entity_class_name}] "
                        f"which are not defined for [{normalized_entity_class_name}]: "
                        f"{missing_global_constraints}"
                    )

                missing_entity_tree_constraints = set(
                    entity_class.entity_tree_unique_constraints()
                ) - set(normalized_entity_class.entity_tree_unique_constraints())
                if missing_entity_tree_constraints:
                    raise ValueError(
                        f"Found entity_tree constraints defined on [{entity_class_name}] "
                        f"which are not defined for [{normalized_entity_class_name}]: "
                        f"{missing_entity_tree_constraints}"
                    )

    def _check_field_types(
        self,
        *,
        field_name: str,
        entity_class_name: str,
        entity_field_type: Type,
        normalized_entity_class_name: str,
        normalized_entity_field_type: Type,
    ) -> None:
        """Checks that the types for field |field_name| are comparable in the
        non-normalized and normalized versions of the given classes.
        """
        entity_type_is_optional = is_optional_type(entity_field_type)
        normalized_entity_type_is_optional = is_optional_type(
            normalized_entity_field_type
        )

        if not entity_type_is_optional:
            self.assertFalse(
                normalized_entity_type_is_optional,
                f"Field [{field_name}] is non-optional on the "
                f"{entity_class_name} but optional on "
                f"{normalized_entity_class_name} ",
            )

        if entity_type_is_optional and is_list_type(
            get_non_optional_type(entity_field_type)
        ):
            raise ValueError(
                f"Schema should not have optional list types. Found type "
                f"[{entity_field_type}] on field [{field_name}] of "
                f"[{entity_class_name}]."
            )

        if normalized_entity_type_is_optional and is_list_type(
            get_non_optional_type(normalized_entity_field_type)
        ):
            raise ValueError(
                f"Schema should not have optional list types. Found type "
                f"[{normalized_entity_field_type}] on field [{field_name}] of "
                f"[{normalized_entity_class_name}]."
            )

        entity_type_is_list = is_list_type(entity_field_type)
        normalized_entity_type_is_list = is_list_type(normalized_entity_field_type)

        self.assertEqual(entity_type_is_list, normalized_entity_type_is_list)

        entity_type_to_compare: Type | str
        if entity_type_is_optional:
            entity_type_to_compare = get_non_optional_type(entity_field_type)
        elif entity_type_is_list:
            entity_type_to_compare = non_optional(
                get_referenced_class_name_from_type(entity_field_type)
            )
        else:
            entity_type_to_compare = entity_field_type

        normalized_entity_type_to_compare: Type | str
        if normalized_entity_type_is_optional:
            normalized_entity_type_to_compare = get_non_optional_type(
                normalized_entity_field_type
            )
        elif normalized_entity_type_is_list:
            normalized_entity_type_to_compare = non_optional(
                get_referenced_class_name_from_type(normalized_entity_field_type)
            )
        else:
            normalized_entity_type_to_compare = normalized_entity_field_type

        if isinstance(entity_type_to_compare, ForwardRef):
            entity_type_to_compare = entity_type_to_compare.__forward_arg__

        if isinstance(normalized_entity_type_to_compare, ForwardRef):
            normalized_entity_type_to_compare = (
                normalized_entity_type_to_compare.__forward_arg__
            )
            if not isinstance(normalized_entity_type_to_compare, str):
                raise ValueError(
                    f"Expected ForwardRef arg to return a string, found "
                    f"[{normalized_entity_type_to_compare}] for field [{field_name}]"
                )

        if isinstance(normalized_entity_type_to_compare, str):
            if not normalized_entity_type_to_compare.startswith(NORMALIZED_PREFIX):
                raise ValueError(
                    f"Found field [{field_name}] on class "
                    f"[{normalized_entity_class_name}] that references class "
                    f"[{normalized_entity_type_to_compare}], which is not a normalized "
                    f"type."
                )
            normalized_entity_type_to_compare = normalized_entity_type_to_compare[
                len(NORMALIZED_PREFIX) :
            ]

        self.assertEqual(
            entity_type_to_compare,
            normalized_entity_type_to_compare,
            f"Field [{field_name}] has non-equivalent type "
            f"[{normalized_entity_field_type}] on {normalized_entity_class_name}. Type "
            f"on {entity_class_name}: {entity_field_type}",
        )


class TestSentencingEntities(unittest.TestCase):
    """This class is for testing specfic functionality and methods on normalized sentencing v2 entities."""

    def test_sentence_inferred_group_external_id(self) -> None:
        delim = (
            normalized_entities.NormalizedStateSentenceInferredGroup.external_id_delimiter()
        )
        inferred_group = normalized_entities.NormalizedStateSentenceInferredGroup(
            state_code=StateCode.US_XX.value,
            external_id=f"sentence-01{delim}sentence-02",
        )
        assert inferred_group.sentence_external_ids == ["sentence-01", "sentence-02"]

    def test_first_serving_status_to_terminating_status_dt_range(self) -> None:
        first_dt = datetime.datetime(2022, 1, 1, 12)
        final_dt = datetime.datetime(2022, 4, 15, 6)
        sentence = normalized_entities.NormalizedStateSentence(
            external_id="test",
            sentence_id=1,
            sentence_inferred_group_id=None,
            sentence_imposed_group_id=None,
            state_code=StateCode.US_XX.value,
            imposed_date=first_dt.date(),
            sentencing_authority=StateSentencingAuthority.STATE,
            sentence_type=StateSentenceType.STATE_PRISON,
            sentence_status_snapshots=[
                normalized_entities.NormalizedStateSentenceStatusSnapshot(
                    state_code=StateCode.US_XX.value,
                    status_update_datetime=first_dt,
                    status_end_datetime=final_dt,
                    status=StateSentenceStatus.SERVING,
                    sentence_status_snapshot_id=11,
                    sequence_num=1,
                ),
                normalized_entities.NormalizedStateSentenceStatusSnapshot(
                    state_code=StateCode.US_XX.value,
                    status_update_datetime=final_dt,
                    status_end_datetime=None,
                    status=StateSentenceStatus.COMPLETED,
                    sentence_status_snapshot_id=12,
                    sequence_num=2,
                ),
            ],
        )
        dt_range = sentence.first_serving_status_to_terminating_status_dt_range
        assert dt_range is not None
        assert dt_range.lower_bound_inclusive == first_dt
        assert dt_range.upper_bound_exclusive == final_dt

    def test_first_serving_status_to_terminating_status_dt_range__no_serving(
        self,
    ) -> None:
        first_dt = datetime.datetime(2022, 1, 1, 12)
        final_dt = datetime.datetime(2022, 4, 15, 6)
        sentence = normalized_entities.NormalizedStateSentence(
            external_id="test",
            sentence_id=1,
            sentence_inferred_group_id=None,
            sentence_imposed_group_id=None,
            state_code=StateCode.US_XX.value,
            imposed_date=first_dt.date(),
            sentencing_authority=StateSentencingAuthority.STATE,
            sentence_type=StateSentenceType.STATE_PRISON,
            sentence_status_snapshots=[
                normalized_entities.NormalizedStateSentenceStatusSnapshot(
                    state_code=StateCode.US_XX.value,
                    status_update_datetime=first_dt,
                    status_end_datetime=final_dt,
                    status=StateSentenceStatus.SUSPENDED,
                    sentence_status_snapshot_id=11,
                    sequence_num=1,
                ),
                normalized_entities.NormalizedStateSentenceStatusSnapshot(
                    state_code=StateCode.US_XX.value,
                    status_update_datetime=final_dt,
                    status_end_datetime=None,
                    status=StateSentenceStatus.COMPLETED,
                    sentence_status_snapshot_id=12,
                    sequence_num=2,
                ),
            ],
        )
        dt_range = sentence.first_serving_status_to_terminating_status_dt_range
        assert dt_range is None

    def test_first_serving_status_to_terminating_status_dt_range__serving_suspended(
        self,
    ) -> None:
        first_dt = datetime.datetime(2022, 1, 1, 12)
        final_dt = datetime.datetime(2022, 4, 15, 6)
        sentence = normalized_entities.NormalizedStateSentence(
            external_id="test",
            sentence_id=1,
            sentence_inferred_group_id=None,
            sentence_imposed_group_id=None,
            state_code=StateCode.US_XX.value,
            imposed_date=first_dt.date(),
            sentencing_authority=StateSentencingAuthority.STATE,
            sentence_type=StateSentenceType.STATE_PRISON,
            sentence_status_snapshots=[
                normalized_entities.NormalizedStateSentenceStatusSnapshot(
                    state_code=StateCode.US_XX.value,
                    status_update_datetime=first_dt,
                    status_end_datetime=final_dt,
                    status=StateSentenceStatus.SERVING,
                    sentence_status_snapshot_id=11,
                    sequence_num=1,
                ),
                normalized_entities.NormalizedStateSentenceStatusSnapshot(
                    state_code=StateCode.US_XX.value,
                    status_update_datetime=final_dt,
                    status_end_datetime=None,
                    status=StateSentenceStatus.SUSPENDED,
                    sentence_status_snapshot_id=12,
                    sequence_num=2,
                ),
            ],
        )
        dt_range = sentence.first_serving_status_to_terminating_status_dt_range
        assert dt_range is not None
        assert dt_range.lower_bound_inclusive == first_dt
        assert dt_range.upper_bound_exclusive is None

    def test_first_serving_status_to_terminating_status_dt_range__all_serving(
        self,
    ) -> None:
        first_dt = datetime.datetime(2022, 1, 1, 12)
        middle_dt = datetime.datetime(2022, 3, 2, 5, 30)
        final_dt = datetime.datetime(2022, 4, 15, 6)
        sentence = normalized_entities.NormalizedStateSentence(
            external_id="test",
            sentence_id=1,
            sentence_inferred_group_id=None,
            sentence_imposed_group_id=None,
            state_code=StateCode.US_XX.value,
            imposed_date=first_dt.date(),
            sentencing_authority=StateSentencingAuthority.STATE,
            sentence_type=StateSentenceType.STATE_PRISON,
            sentence_status_snapshots=[
                normalized_entities.NormalizedStateSentenceStatusSnapshot(
                    state_code=StateCode.US_XX.value,
                    status_update_datetime=first_dt,
                    status_end_datetime=middle_dt,
                    status=StateSentenceStatus.SERVING,
                    sentence_status_snapshot_id=11,
                    sequence_num=1,
                ),
                normalized_entities.NormalizedStateSentenceStatusSnapshot(
                    state_code=StateCode.US_XX.value,
                    status_update_datetime=middle_dt,
                    status_end_datetime=final_dt,
                    status=StateSentenceStatus.SERVING,
                    sentence_status_snapshot_id=12,
                    sequence_num=2,
                ),
                normalized_entities.NormalizedStateSentenceStatusSnapshot(
                    state_code=StateCode.US_XX.value,
                    status_update_datetime=final_dt,
                    status_end_datetime=None,
                    status=StateSentenceStatus.SERVING,
                    sentence_status_snapshot_id=13,
                    sequence_num=3,
                ),
            ],
        )
        dt_range = sentence.first_serving_status_to_terminating_status_dt_range
        assert dt_range is not None
        assert dt_range.lower_bound_inclusive == first_dt
        assert dt_range.upper_bound_exclusive is None

    def test_first_serving_status_to_terminating_status_dt_range__no_snapshots(
        self,
    ) -> None:
        sentence = normalized_entities.NormalizedStateSentence(
            external_id="test",
            sentence_id=1,
            sentence_inferred_group_id=None,
            sentence_imposed_group_id=None,
            state_code=StateCode.US_XX.value,
            imposed_date=datetime.date(2022, 1, 1),
            sentencing_authority=StateSentencingAuthority.STATE,
            sentence_type=StateSentenceType.STATE_PRISON,
            sentence_status_snapshots=[],
        )
        dt_range = sentence.first_serving_status_to_terminating_status_dt_range
        assert dt_range is None


class TestNormalizedStatePersonStaffRelationshipPeriod(unittest.TestCase):
    """Tests for the NormalizedStatePersonStaffRelationshipPeriod entity."""

    def test_simple(self) -> None:
        # These are valid relationship periods and shouldn't crash
        period = normalized_entities.NormalizedStatePersonStaffRelationshipPeriod(
            person_staff_relationship_period_id=123,
            state_code=StateCode.US_XX.value,
            relationship_start_date=datetime.date(2021, 1, 1),
            relationship_end_date_exclusive=None,
            system_type=StateSystemType.INCARCERATION,
            system_type_raw_text=None,
            relationship_type=StatePersonStaffRelationshipType.CASE_MANAGER,
            relationship_type_raw_text=None,
            associated_staff_external_id="EMP2",
            associated_staff_external_id_type="US_XX_STAFF_ID",
            associated_staff_id=1,
            relationship_priority=1,
            location_external_id=None,
        )
        self.assertIsInstance(period.person_staff_relationship_period_id, int)
        update_entity_with_globally_unique_id(root_entity_id=111, entity=period)
        self.assertEqual(
            9002845054915570502, period.person_staff_relationship_period_id
        )

        _ = normalized_entities.NormalizedStatePersonStaffRelationshipPeriod(
            person_staff_relationship_period_id=123,
            state_code=StateCode.US_XX.value,
            relationship_start_date=datetime.date(2021, 1, 1),
            relationship_end_date_exclusive=datetime.date(2023, 1, 1),
            system_type=StateSystemType.INCARCERATION,
            system_type_raw_text="CM",
            relationship_type=StatePersonStaffRelationshipType.CASE_MANAGER,
            relationship_type_raw_text="CM",
            associated_staff_external_id="EMP2",
            associated_staff_external_id_type="US_XX_STAFF_ID",
            associated_staff_id=1,
            relationship_priority=1,
            location_external_id="UNIT 1",
        )

    def test_enforce_nonnull_positive_relationship_priority(self) -> None:
        with self.assertRaisesRegex(
            ValueError,
            r"Field \[relationship_priority\] on "
            r"\[NormalizedStatePersonStaffRelationshipPeriod\] must be a positive "
            r"integer. Found value \[0\]",
        ):
            _ = normalized_entities.NormalizedStatePersonStaffRelationshipPeriod(
                person_staff_relationship_period_id=123,
                state_code=StateCode.US_XX.value,
                relationship_start_date=datetime.date(2021, 1, 1),
                relationship_end_date_exclusive=datetime.date(2023, 1, 1),
                system_type=StateSystemType.INCARCERATION,
                system_type_raw_text="CM",
                relationship_type=StatePersonStaffRelationshipType.CASE_MANAGER,
                relationship_type_raw_text="CM",
                associated_staff_external_id="EMP2",
                associated_staff_external_id_type="US_XX_STAFF_ID",
                associated_staff_id=1,
                relationship_priority=0,
                location_external_id=None,
            )

        with self.assertRaisesRegex(
            TypeError,
            r"'relationship_priority' must be <class 'int'> \(got None that is a "
            r"<class 'NoneType'>\)",
        ):
            _ = normalized_entities.NormalizedStatePersonStaffRelationshipPeriod(
                person_staff_relationship_period_id=123,
                state_code=StateCode.US_XX.value,
                relationship_start_date=datetime.date(2021, 1, 1),
                relationship_end_date_exclusive=datetime.date(2023, 1, 1),
                system_type=StateSystemType.INCARCERATION,
                system_type_raw_text="CM",
                relationship_type=StatePersonStaffRelationshipType.CASE_MANAGER,
                relationship_type_raw_text="CM",
                associated_staff_external_id="EMP2",
                associated_staff_external_id_type="US_XX_STAFF_ID",
                associated_staff_id=1,
                relationship_priority=None,  # type: ignore[arg-type]
                location_external_id=None,
            )

    def test_zero_day_relationship_periods(self) -> None:
        with self.assertRaisesRegex(
            ValueError,
            r"Found NormalizedStatePersonStaffRelationshipPeriod\("
            r"person_staff_relationship_period_id=123\) with "
            r"relationship_start_date datetime 2021-01-01 equal to "
            r"relationship_end_date_exclusive datetime 2021-01-01.",
        ):
            _ = normalized_entities.NormalizedStatePersonStaffRelationshipPeriod(
                person_staff_relationship_period_id=123,
                state_code=StateCode.US_XX.value,
                relationship_start_date=datetime.date(2021, 1, 1),
                relationship_end_date_exclusive=datetime.date(2021, 1, 1),
                system_type=StateSystemType.INCARCERATION,
                system_type_raw_text="CM",
                relationship_type=StatePersonStaffRelationshipType.CASE_MANAGER,
                relationship_type_raw_text="CM",
                associated_staff_external_id="EMP2",
                associated_staff_external_id_type="US_XX_STAFF_ID",
                associated_staff_id=1,
                relationship_priority=1,
                location_external_id="UNIT 1",
            )

    def test_negative_day_relationship_periods(self) -> None:
        with self.assertRaisesRegex(
            ValueError,
            r"Found NormalizedStatePersonStaffRelationshipPeriod\("
            r"person_staff_relationship_period_id=123\) with "
            r"relationship_start_date datetime 2022-01-01 after "
            r"relationship_end_date_exclusive datetime 2021-01-01.",
        ):
            _ = normalized_entities.NormalizedStatePersonStaffRelationshipPeriod(
                person_staff_relationship_period_id=123,
                state_code=StateCode.US_XX.value,
                relationship_start_date=datetime.date(2022, 1, 1),
                relationship_end_date_exclusive=datetime.date(2021, 1, 1),
                system_type=StateSystemType.INCARCERATION,
                system_type_raw_text="CM",
                relationship_type=StatePersonStaffRelationshipType.CASE_MANAGER,
                relationship_type_raw_text="CM",
                associated_staff_external_id="EMP2",
                associated_staff_external_id_type="US_XX_STAFF_ID",
                associated_staff_id=1,
                relationship_priority=1,
                location_external_id="UNIT 1",
            )
