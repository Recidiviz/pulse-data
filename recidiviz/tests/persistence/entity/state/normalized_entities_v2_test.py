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

import unittest
from typing import Dict, ForwardRef, List, Set, Type

import attr

from recidiviz.common.attr_mixins import attribute_field_type_reference_for_class
from recidiviz.common.attr_utils import (
    get_non_optional_type,
    get_referenced_class_name_from_type,
    is_flat_field,
    is_list_type,
    is_optional_type,
)
from recidiviz.common.attr_validators import IsListOfValidator, IsOptionalValidator
from recidiviz.persistence.entity.base_entity import Entity
from recidiviz.persistence.entity.entity_utils import get_all_entity_classes_in_module
from recidiviz.persistence.entity.state import entities as state_entities
from recidiviz.persistence.entity.state import (
    normalized_entities,
    normalized_entities_v2,
)
from recidiviz.persistence.entity.state.entity_field_validators import (
    PreNormOptionalValidator,
)
from recidiviz.persistence.entity.state.normalized_entities_v2 import (
    EntityBackedgeValidator,
)
from recidiviz.tests.persistence.entity.state.normalized_entities_test import (
    NORMALIZED_PREFIX,
)
from recidiviz.utils.types import non_optional

# TODO(#30075): This list should become empty and get removed once we've implemented all
#  normalized v2 entities.
UNIMPLEMENTED_NORMALIZED_V2_CLASSES = {
    "StateStaff",
    "StateStaffCaseloadTypePeriod",
    "StateStaffExternalId",
    "StateStaffLocationPeriod",
    "StateStaffRolePeriod",
    "StateStaffSupervisorPeriod",
}

EXPECTED_MISSING_NORMALIZED_FIELDS: Dict[Type[Entity], Set[str]] = {
    # TODO(#15559): Add NormalizedStateCharge / NormalizedStateChargeV2 fields that we
    #  plan to delete here.
}


class TestNormalizedEntities(unittest.TestCase):
    """Tests the classes defined in normalized_entities_v2.py"""

    def setUp(self) -> None:
        self.normalized_entity_classes: List[Type[Entity]] = list(
            get_all_entity_classes_in_module(normalized_entities_v2)
        )

        self.normalized_entity_classes_by_name = {
            c.__name__: c for c in self.normalized_entity_classes
        }

    # TODO(#30075): Delete this test when UNIMPLEMENTED_NORMALIZED_V2_CLASSES is empty
    def test_UNIMPLEMENTED_NORMALIZED_V2_CLASSES_cleanup(self) -> None:
        for normalized_class_name in self.normalized_entity_classes_by_name:
            non_normalized_name = normalized_class_name[len(NORMALIZED_PREFIX) :]
            if non_normalized_name in UNIMPLEMENTED_NORMALIZED_V2_CLASSES:
                self.fail(
                    f"Found class [{normalized_class_name}] - can remove "
                    f"[{non_normalized_name}] from UNIMPLEMENTED_NORMALIZED_V2_CLASSES."
                )

    def test_normalized_entities_correct_setup(self) -> None:
        for normalized_entity_class in self.normalized_entity_classes:
            normalized_entity_class_name = normalized_entity_class.__name__
            normalized_entity_fields = attribute_field_type_reference_for_class(
                normalized_entity_class
            )
            self.assertTrue(normalized_entity_class_name.startswith(NORMALIZED_PREFIX))
            for field_name, normalized_field_info in normalized_entity_fields.items():
                if normalized_field_info.referenced_cls_name:
                    self.assertTrue(
                        normalized_field_info.referenced_cls_name.startswith(
                            NORMALIZED_PREFIX
                        ),
                        f"Referenced class [{normalized_field_info.referenced_cls_name}] "
                        f"is not a Normalized* entity",
                    )

                field_validator = normalized_field_info.attribute.validator
                if not field_validator:
                    self.fail(
                        f"Found field [{field_name}] on class "
                        f"[{normalized_entity_class_name}] which does not have a "
                        f"validator"
                    )
                field_type = non_optional(normalized_field_info.attribute.type)
                # TODO(#30075): Define a class hierarchy for normalized entities so we
                #  can use that to determine if the field is a backedge field, then
                #  assert that all backedge fields have an EntityBackedgeValidator
                is_backedge_field = isinstance(field_validator, EntityBackedgeValidator)
                if not is_optional_type(field_type):
                    self.assertFalse(
                        isinstance(field_validator, IsOptionalValidator),
                        f"Found non-optional [{field_name}] on class "
                        f"[{normalized_entity_class_name}] which has a validator that "
                        f"allows for optional values.",
                    )
                    if (
                        # We allow list type back edge fields to have a default
                        not is_backedge_field
                        and normalized_field_info.attribute.default != attr.NOTHING
                    ):
                        raise ValueError(
                            f"Found non-optional [{field_name}] on class "
                            f"[{normalized_entity_class_name}] with default value set.",
                        )

                if not issubclass(normalized_entity_class, Entity):
                    raise ValueError(
                        f"Expected normalized entity class [{normalized_entity_class}] "
                        f"to be subclass of Entity"
                    )

                if is_list_type(field_type):
                    # Backedges use EntityBackedgeValidator which also validate that the
                    # value is a list.
                    if not is_backedge_field:
                        self.assertTrue(
                            isinstance(field_validator, IsListOfValidator),
                            f"Found list field [{field_name}] on class "
                            f"[{normalized_entity_class_name}] which does not use an "
                            f"is_list_of() validator.",
                        )

                primary_key_col_name = normalized_entity_class.get_class_id_name()
                if field_name == primary_key_col_name:
                    self.assertFalse(
                        is_optional_type(field_type),
                        f"Found primary key [{field_name}] which has optional type "
                        f"[{field_type}]",
                    )
                    self.assertEqual(int, get_non_optional_type(field_type))

    # TODO(#30075): This test can be deleted once we've migrated to use v2 versions of
    #  these entities everywhere.
    def test_compare_v1_and_v2_normalized_entities(self) -> None:
        for legacy_normalized_entity_class in get_all_entity_classes_in_module(
            normalized_entities
        ):
            normalized_entity_class_name = legacy_normalized_entity_class.__name__
            entity_class_name = normalized_entity_class_name[len(NORMALIZED_PREFIX) :]
            if entity_class_name in UNIMPLEMENTED_NORMALIZED_V2_CLASSES:
                continue

            new_normalized_entity_class = self.normalized_entity_classes_by_name[
                normalized_entity_class_name
            ]

            legacy_normalized_entity_fields = attribute_field_type_reference_for_class(
                legacy_normalized_entity_class
            )
            new_normalized_entity_fields = attribute_field_type_reference_for_class(
                new_normalized_entity_class
            )

            missing_new_fields = (
                set(legacy_normalized_entity_fields)
                - set(new_normalized_entity_fields)
                - EXPECTED_MISSING_NORMALIZED_FIELDS.get(
                    new_normalized_entity_class, set()
                )
            )
            if missing_new_fields:
                raise ValueError(
                    f"Found fields on legacy [{entity_class_name}] which are not "
                    f"present on the new version defined in normalized_entities_v2.py: "
                    f"{missing_new_fields}"
                )

            missing_old_fields = set(new_normalized_entity_fields) - set(
                legacy_normalized_entity_fields
            )
            if missing_old_fields:
                raise ValueError(
                    f"Found fields on the new version of [{entity_class_name}] defined "
                    f"in normalized_entities_v2.py which are not present on the legacy "
                    f"version in normalized_entities.py: "
                    f"{missing_old_fields}"
                )

    def test_state_entities_and_normalized_entities_parity(self) -> None:
        entity_classes: Set[Type[Entity]] = get_all_entity_classes_in_module(
            state_entities
        )

        for entity_class in entity_classes:
            entity_class_name = entity_class.__name__
            if entity_class_name in UNIMPLEMENTED_NORMALIZED_V2_CLASSES:
                continue
            entity_fields = attribute_field_type_reference_for_class(entity_class)

            normalized_entity_class_name = f"{NORMALIZED_PREFIX}{entity_class_name}"
            normalized_entity_class = self.normalized_entity_classes_by_name[
                normalized_entity_class_name
            ]
            normalized_entity_fields = attribute_field_type_reference_for_class(
                normalized_entity_class
            )

            missing_normalized_entity_fields = (
                set(entity_fields)
                - set(normalized_entity_fields)
                - EXPECTED_MISSING_NORMALIZED_FIELDS.get(normalized_entity_class, set())
            )
            if missing_normalized_entity_fields:
                raise ValueError(
                    f"Found fields on class [{entity_class_name}] which are not "
                    f"present on [{normalized_entity_class_name}]: "
                    f"{missing_normalized_entity_fields}"
                )

            for field_name, normalized_field_info in normalized_entity_fields.items():
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
                entity_field_info = entity_fields[field_name]
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

                if isinstance(entity_validator, PreNormOptionalValidator):
                    self.assertFalse(
                        isinstance(
                            normalized_entity_validator,
                            IsOptionalValidator,
                        ),
                        f"Found [{field_name}] which has a validator that "
                        f"allows for optional values even though it is marked as "
                        f"pre_norm_opt() in state/entities.py.",
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
