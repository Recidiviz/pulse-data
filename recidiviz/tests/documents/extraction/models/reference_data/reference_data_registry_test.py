# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2026 Recidiviz, Inc.
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
"""Tests for reference_data_registry.py."""
from unittest import TestCase

from recidiviz.common.constants.states import StateCode
from recidiviz.documents.extraction.models.llm_extractor_config import (
    get_states_with_extractor_configs,
)
from recidiviz.documents.extraction.models.reference_data.acronym_reference_data_entry import (
    AcronymReferenceDataEntry,
)
from recidiviz.documents.extraction.models.reference_data.known_organization_reference_data_entry import (
    KnownOrganizationReferenceDataEntry,
)
from recidiviz.documents.extraction.models.reference_data.organization_type import (
    OrganizationType,
)
from recidiviz.documents.extraction.models.reference_data.reference_data_registry import (
    SHARED_DIR_NAME,
    ReferenceDataRegistry,
    ReferenceDataType,
    StateSpecificReferenceDataRegistry,
    load_full_reference_data_registry,
)
from recidiviz.tests.documents import fake_config
from recidiviz.tests.documents.extraction.models.reference_data import (
    fixtures_config as bad_reference_data_config,
)
from recidiviz.utils.types import assert_type


class ReferenceDataTypeTest(TestCase):
    """Tests for the ReferenceDataType discriminator enum."""

    def test_entry_class_for_every_value(self) -> None:
        # Every member must map to an entry class; a new member with no
        # `entry_class` branch would hit the `raise` and fail here.
        for reference_data_type in ReferenceDataType:
            self.assertTrue(issubclass(reference_data_type.entry_class, object))

    def test_entry_class_values(self) -> None:
        self.assertIs(
            KnownOrganizationReferenceDataEntry,
            ReferenceDataType.KNOWN_ORGANIZATIONS.entry_class,
        )
        self.assertIs(AcronymReferenceDataEntry, ReferenceDataType.ACRONYMS.entry_class)


class ReferenceDataRegistryTest(TestCase):
    """Tests for the single-file ReferenceDataRegistry."""

    def test_entries_by_dedup_key(self) -> None:
        registry = ReferenceDataRegistry(
            entry_type=AcronymReferenceDataEntry,
            entries=[
                AcronymReferenceDataEntry(acronym="PO", expansion="Parole Officer"),
                AcronymReferenceDataEntry(acronym="FT", expansion="Full-Time"),
            ],
        )
        self.assertEqual(["PO", "FT"], list(registry.entries_by_dedup_key))

    def test_duplicate_dedup_key_raises(self) -> None:
        with self.assertRaisesRegex(
            ValueError,
            r"^Found duplicate reference-data entries with dedup_key \[PO\].$",
        ):
            ReferenceDataRegistry(
                entry_type=AcronymReferenceDataEntry,
                entries=[
                    AcronymReferenceDataEntry(acronym="PO", expansion="Parole Officer"),
                    AcronymReferenceDataEntry(acronym="PO", expansion="Probation"),
                ],
            )

    def test_entry_type_not_subclass_raises(self) -> None:
        with self.assertRaisesRegex(
            ValueError,
            r"\[entry_type\] .* which is not a subclass of \[ReferenceDataEntry\]",
        ):
            ReferenceDataRegistry(entry_type=str, entries=[])  # type: ignore[type-var]

    def test_entry_of_wrong_type_raises(self) -> None:
        with self.assertRaisesRegex(
            ValueError,
            r"has an entry of type \[KnownOrganizationReferenceDataEntry\], "
            r"expected \[AcronymReferenceDataEntry\]",
        ):
            ReferenceDataRegistry(  # type: ignore[misc]
                entry_type=AcronymReferenceDataEntry,
                entries=[
                    KnownOrganizationReferenceDataEntry(
                        name="X",
                        organization_type=OrganizationType.EMPLOYER,
                        aliases=[],
                    )
                ],
            )

    def test_from_yaml_file_for_reference_data_type(self) -> None:
        registry = ReferenceDataRegistry.from_yaml_file_for_reference_data_type(
            reference_data_type=ReferenceDataType.ACRONYMS,
            subdir=SHARED_DIR_NAME,
            config_module=fake_config,
        )
        self.assertIs(AcronymReferenceDataEntry, registry.entry_type)
        self.assertEqual(["PO", "CRC"], list(registry.entries_by_dedup_key))

    def test_from_yaml_file_declared_type_mismatch_raises(self) -> None:
        with self.assertRaisesRegex(
            ValueError,
            r"declares type \[known_organizations\], but its location implies "
            r"\[acronyms\]",
        ):
            ReferenceDataRegistry.from_yaml_file_for_reference_data_type(
                reference_data_type=ReferenceDataType.ACRONYMS,
                subdir="declared_type_mismatch",
                config_module=bad_reference_data_config,
            )

    def test_from_yaml_file_unexpected_key_raises(self) -> None:
        with self.assertRaisesRegex(
            ValueError, r"^Found unexpected config values in reference data file at"
        ):
            ReferenceDataRegistry.from_yaml_file_for_reference_data_type(
                reference_data_type=ReferenceDataType.ACRONYMS,
                subdir="unexpected_key",
                config_module=bad_reference_data_config,
            )

    def test_from_yaml_file_missing_file_raises(self) -> None:
        with self.assertRaises(FileNotFoundError):
            ReferenceDataRegistry.from_yaml_file_for_reference_data_type(
                reference_data_type=ReferenceDataType.ACRONYMS,
                subdir="nonexistent_subdir",
                config_module=fake_config,
            )


class StateSpecificReferenceDataRegistryTest(TestCase):
    """Tests for the merged, state-specific registry."""

    def test_load_for_reference_data_type_merges_shared_and_state(self) -> None:
        registry = StateSpecificReferenceDataRegistry.load_for_reference_data_type(
            state_code=StateCode.US_XX,
            reference_data_type=ReferenceDataType.ACRONYMS,
            config_module=fake_config,
        )
        self.assertEqual(StateCode.US_XX, registry.state_code)
        # shared (PO, CRC) overlaid with state (CRC override, XX): shared order
        # preserved, override in place, new state entry appended.
        self.assertEqual(["PO", "CRC", "XX"], list(registry.entries_by_dedup_key))
        # State wins on the CRC collision.
        self.assertEqual(
            "Citadel Reentry Center",
            assert_type(
                registry.entries_by_dedup_key["CRC"], AcronymReferenceDataEntry
            ).expansion,
        )


class LoadFullReferenceDataRegistryTest(TestCase):
    """Tests for the load_full_reference_data_registry entry point."""

    def test_load_full_real_config(self) -> None:
        registry = load_full_reference_data_registry(StateCode.US_OZ)
        self.assertEqual(set(ReferenceDataType), set(registry))
        # US_OZ overrides the shared CRC acronym.
        self.assertEqual(
            "Citadel Reentry Center",
            assert_type(
                registry[ReferenceDataType.ACRONYMS].entries_by_dedup_key["CRC"],
                AcronymReferenceDataEntry,
            ).expansion,
        )

    def test_load_full_fake_config(self) -> None:
        registry = load_full_reference_data_registry(
            StateCode.US_XX, config_module=fake_config
        )
        self.assertEqual(set(ReferenceDataType), set(registry))
        self.assertEqual(
            ["PO", "CRC", "XX"],
            list(registry[ReferenceDataType.ACRONYMS].entries_by_dedup_key),
        )
        self.assertEqual([], registry[ReferenceDataType.KNOWN_ORGANIZATIONS].entries)

    def test_load_full_is_cached(self) -> None:
        self.assertIs(
            load_full_reference_data_registry(StateCode.US_OZ),
            load_full_reference_data_registry(StateCode.US_OZ),
        )

    def test_load_full_missing_state_files_raises(self) -> None:
        # A state with no reference-data directory has no file for the required
        # categories, which is an error.
        with self.assertRaises(FileNotFoundError):
            load_full_reference_data_registry(StateCode.US_PA)

    def test_reference_data_loads_for_every_extractor_state(self) -> None:
        # Every state that defines an extractor must have loadable reference data,
        # since resolving the extractor binds it. Enumerate those states without
        # resolving the extractors (which would itself load the reference data),
        # so this is an independent guard.
        for state_code in get_states_with_extractor_configs():
            with self.subTest(state=state_code):
                load_full_reference_data_registry(state_code)
