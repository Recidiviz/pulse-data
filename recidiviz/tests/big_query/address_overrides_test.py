# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2022 Recidiviz, Inc.
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
"""Tests for BigQueryAddressOverrides."""
import unittest

from recidiviz.big_query.address_overrides import BigQueryAddressOverrides
from recidiviz.big_query.big_query_address import BigQueryAddress

_DATASET_1 = "dataset_1"
_DATASET_2 = "dataset_2"

_TABLE_1 = "table_1"
_TABLE_2 = "table_2"


class BigQueryAddressOverridesTest(unittest.TestCase):
    """Tests for BigQueryAddressOverrides."""

    def test_address_overrides_empty(self) -> None:
        overrides = BigQueryAddressOverrides.Builder(sandbox_prefix="my_prefix").build()
        self.assertIsNone(
            overrides.get_sandbox_address(
                BigQueryAddress(dataset_id=_DATASET_1, table_id=_TABLE_1)
            )
        )

    def test_address_overrides_addresses_only(self) -> None:
        address_1 = BigQueryAddress(dataset_id=_DATASET_1, table_id=_TABLE_1)
        address_2 = BigQueryAddress(dataset_id=_DATASET_2, table_id=_TABLE_1)
        address_3 = BigQueryAddress(dataset_id=_DATASET_2, table_id=_TABLE_2)
        overrides = (
            BigQueryAddressOverrides.Builder(sandbox_prefix="my_prefix")
            .register_sandbox_override_for_address(address_1)
            .register_sandbox_override_for_address(address_2)
            .register_sandbox_override_for_address(address_3)
            .build()
        )
        self.assertEqual(
            BigQueryAddress(dataset_id=f"my_prefix_{_DATASET_1}", table_id=_TABLE_1),
            overrides.get_sandbox_address(address_1),
        )
        self.assertEqual(
            BigQueryAddress(dataset_id=f"my_prefix_{_DATASET_2}", table_id=_TABLE_1),
            overrides.get_sandbox_address(address_2),
        )
        self.assertEqual(
            BigQueryAddress(dataset_id=f"my_prefix_{_DATASET_2}", table_id=_TABLE_2),
            overrides.get_sandbox_address(address_3),
        )

        self.assertIsNone(
            overrides.get_sandbox_address(
                BigQueryAddress(dataset_id=_DATASET_2, table_id="another_table")
            ),
        )

    def test_address_overrides_full_datasets(self) -> None:
        address_1 = BigQueryAddress(dataset_id=_DATASET_1, table_id=_TABLE_1)
        address_2 = BigQueryAddress(dataset_id=_DATASET_2, table_id=_TABLE_1)
        address_3 = BigQueryAddress(dataset_id=_DATASET_2, table_id=_TABLE_2)
        overrides = (
            BigQueryAddressOverrides.Builder(sandbox_prefix="my_prefix")
            .register_sandbox_override_for_entire_dataset(_DATASET_1)
            .register_sandbox_override_for_entire_dataset(_DATASET_2)
            .build()
        )
        self.assertEqual(
            BigQueryAddress(dataset_id=f"my_prefix_{_DATASET_1}", table_id=_TABLE_1),
            overrides.get_sandbox_address(address_1),
        )
        self.assertEqual(
            BigQueryAddress(dataset_id=f"my_prefix_{_DATASET_2}", table_id=_TABLE_1),
            overrides.get_sandbox_address(address_2),
        )
        self.assertEqual(
            BigQueryAddress(dataset_id=f"my_prefix_{_DATASET_2}", table_id=_TABLE_2),
            overrides.get_sandbox_address(address_3),
        )
        self.assertEqual(
            BigQueryAddress(
                dataset_id=f"my_prefix_{_DATASET_1}", table_id="another_table"
            ),
            overrides.get_sandbox_address(
                BigQueryAddress(dataset_id=_DATASET_1, table_id="another_table")
            ),
        )
        self.assertEqual(
            BigQueryAddress(
                dataset_id=f"my_prefix_{_DATASET_2}", table_id="another_table"
            ),
            overrides.get_sandbox_address(
                BigQueryAddress(dataset_id=_DATASET_2, table_id="another_table")
            ),
        )

    def test_address_overrides_custom_dataset_overrides(self) -> None:
        address_1 = BigQueryAddress(dataset_id=_DATASET_1, table_id=_TABLE_1)
        overrides = (
            BigQueryAddressOverrides.Builder(sandbox_prefix="my_prefix")
            .register_custom_dataset_override(
                _DATASET_1, f"some_other_prefix_{_DATASET_1}"
            )
            .build()
        )
        self.assertEqual(
            BigQueryAddress(
                dataset_id=f"some_other_prefix_{_DATASET_1}", table_id=_TABLE_1
            ),
            overrides.get_sandbox_address(address_1),
        )

    def test_address_overrides_addresses_and_full_datasets(self) -> None:
        address_1 = BigQueryAddress(dataset_id=_DATASET_1, table_id=_TABLE_1)
        address_2 = BigQueryAddress(dataset_id=_DATASET_2, table_id=_TABLE_1)
        address_3 = BigQueryAddress(dataset_id=_DATASET_2, table_id=_TABLE_2)
        overrides = (
            BigQueryAddressOverrides.Builder(sandbox_prefix="my_prefix")
            .register_sandbox_override_for_address(address_1)
            .register_sandbox_override_for_entire_dataset(_DATASET_2)
            .build()
        )
        self.assertEqual(
            BigQueryAddress(dataset_id=f"my_prefix_{_DATASET_1}", table_id=_TABLE_1),
            overrides.get_sandbox_address(address_1),
        )
        self.assertEqual(
            BigQueryAddress(dataset_id=f"my_prefix_{_DATASET_2}", table_id=_TABLE_1),
            overrides.get_sandbox_address(address_2),
        )
        self.assertEqual(
            BigQueryAddress(dataset_id=f"my_prefix_{_DATASET_2}", table_id=_TABLE_2),
            overrides.get_sandbox_address(address_3),
        )

        self.assertIsNone(
            overrides.get_sandbox_address(
                BigQueryAddress(dataset_id=_DATASET_1, table_id="another_table")
            ),
        )
        self.assertEqual(
            BigQueryAddress(
                dataset_id=f"my_prefix_{_DATASET_2}", table_id="another_table"
            ),
            overrides.get_sandbox_address(
                BigQueryAddress(dataset_id=_DATASET_2, table_id="another_table")
            ),
        )

    def test_address_overrides_full_datasets_and_custom(self) -> None:
        address_1 = BigQueryAddress(dataset_id=_DATASET_1, table_id=_TABLE_1)
        address_2 = BigQueryAddress(dataset_id=_DATASET_2, table_id=_TABLE_1)
        overrides = (
            BigQueryAddressOverrides.Builder(sandbox_prefix="my_prefix")
            .register_sandbox_override_for_entire_dataset(_DATASET_1)
            .register_custom_dataset_override(
                _DATASET_2, f"some_other_prefix_{_DATASET_2}"
            )
            .build()
        )
        self.assertEqual(
            BigQueryAddress(dataset_id=f"my_prefix_{_DATASET_1}", table_id=_TABLE_1),
            overrides.get_sandbox_address(address_1),
        )
        self.assertEqual(
            BigQueryAddress(
                dataset_id=f"some_other_prefix_{_DATASET_2}", table_id=_TABLE_1
            ),
            overrides.get_sandbox_address(address_2),
        )
        self.assertEqual(
            BigQueryAddress(
                dataset_id=f"my_prefix_{_DATASET_1}", table_id="another_table"
            ),
            overrides.get_sandbox_address(
                BigQueryAddress(dataset_id=_DATASET_1, table_id="another_table")
            ),
        )
        self.assertEqual(
            BigQueryAddress(
                dataset_id=f"some_other_prefix_{_DATASET_2}", table_id="another_table"
            ),
            overrides.get_sandbox_address(
                BigQueryAddress(dataset_id=_DATASET_2, table_id="another_table")
            ),
        )

    def test_conflicting_address_and_full_dataset(self) -> None:
        builder = BigQueryAddressOverrides.Builder(
            sandbox_prefix="my_prefix"
        ).register_sandbox_override_for_entire_dataset(_DATASET_1)

        with self.assertRaisesRegex(
            ValueError,
            r"Dataset \[dataset_1\] for address "
            r"\[BigQueryAddress\(dataset_id='dataset_1', table_id='table_1'\)\] already "
            r"has full dataset override set: \[my_prefix_dataset_1\]",
        ):
            builder.register_sandbox_override_for_address(
                BigQueryAddress(dataset_id=_DATASET_1, table_id=_TABLE_1)
            )

        builder = BigQueryAddressOverrides.Builder(
            sandbox_prefix="my_prefix"
        ).register_sandbox_override_for_address(
            BigQueryAddress(dataset_id=_DATASET_1, table_id=_TABLE_1)
        )

        with self.assertRaisesRegex(
            ValueError,
            r"Found conflicting address overrides already set for addresses in "
            r"\[dataset_1\]",
        ):
            builder.register_sandbox_override_for_entire_dataset(_DATASET_1)

    def test_register_address_twice(self) -> None:
        address = BigQueryAddress(dataset_id=_DATASET_1, table_id=_TABLE_1)
        builder = BigQueryAddressOverrides.Builder(
            sandbox_prefix="my_prefix"
        ).register_sandbox_override_for_address(address)

        with self.assertRaisesRegex(
            ValueError,
            r"Address \[BigQueryAddress\(dataset_id='dataset_1', table_id='table_1'\)\] "
            r"already has override set",
        ):
            builder.register_sandbox_override_for_address(address)

    def test_custom_override_is_not_custom(self) -> None:
        builder = BigQueryAddressOverrides.Builder(sandbox_prefix="my_prefix")

        with self.assertRaisesRegex(
            ValueError,
            r"The new_dataset_id \[my_prefix_dataset_1\] matches the standard sandbox "
            r"override for original_dataset_id \[dataset_1\]",
        ):
            builder.register_custom_dataset_override(
                _DATASET_1, f"my_prefix_{_DATASET_1}"
            )

    def test_custom_override_and_full_dataset_override_conflict(self) -> None:
        builder = BigQueryAddressOverrides.Builder(
            sandbox_prefix="my_prefix"
        ).register_sandbox_override_for_entire_dataset(_DATASET_1)

        with self.assertRaisesRegex(
            ValueError,
            r"Dataset \[dataset_1\] already has override set: \[my_prefix_dataset_1\]",
        ):
            builder.register_custom_dataset_override(
                _DATASET_1, f"some_other_prefix_{_DATASET_1}"
            )

        builder = BigQueryAddressOverrides.Builder(
            sandbox_prefix="my_prefix"
        ).register_custom_dataset_override(
            _DATASET_1, f"some_other_prefix_{_DATASET_1}"
        )

        with self.assertRaisesRegex(
            ValueError,
            r"Dataset \[dataset_1\] already has override set: "
            r"\[some_other_prefix_dataset_1\]",
        ):
            builder.register_sandbox_override_for_entire_dataset(_DATASET_1)

    def test_custom_override_and_address_override_conflict(self) -> None:
        address = BigQueryAddress(dataset_id=_DATASET_1, table_id=_TABLE_1)
        builder = BigQueryAddressOverrides.Builder(
            sandbox_prefix="my_prefix"
        ).register_sandbox_override_for_address(address)

        with self.assertRaisesRegex(
            ValueError,
            r"Found conflicting address overrides already set for addresses in "
            r"\[dataset_1\]",
        ):
            builder.register_custom_dataset_override(
                _DATASET_1, f"some_other_prefix_{_DATASET_1}"
            )

        builder = BigQueryAddressOverrides.Builder(
            sandbox_prefix="my_prefix"
        ).register_custom_dataset_override(
            _DATASET_1, f"some_other_prefix_{_DATASET_1}"
        )

        with self.assertRaisesRegex(
            ValueError,
            r"Dataset \[dataset_1\] for address "
            r"\[BigQueryAddress\(dataset_id='dataset_1', table_id='table_1'\)\] "
            r"already has full dataset override set",
        ):
            builder.register_sandbox_override_for_address(address)
