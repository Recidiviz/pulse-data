# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2021 Recidiviz, Inc.
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
"""Tests for address_overrides.py."""

import unittest
from typing import List, Sequence, Set

from recidiviz.big_query.address_overrides import BigQueryAddressOverrides
from recidiviz.big_query.big_query_address import BigQueryAddress
from recidiviz.big_query.big_query_view import (
    BigQueryViewBuilder,
    SimpleBigQueryViewBuilder,
)
from recidiviz.calculator.query.state.dataset_config import DATAFLOW_METRICS_DATASET
from recidiviz.view_registry.address_overrides_factory import (
    address_overrides_for_input_source_tables,
    address_overrides_for_view_builders,
)


class TestAddressOverrides(unittest.TestCase):
    """Tests for address_overrides.py."""

    @staticmethod
    def _all_datasets(builders: List[BigQueryViewBuilder]) -> Set[str]:
        datasets = set()
        for builder in builders:
            datasets.add(builder.dataset_id)
            if builder.materialized_address:
                datasets.add(builder.materialized_address.dataset_id)
        return datasets

    def test_address_overrides_for_view_builders(self) -> None:
        view_builders = [
            SimpleBigQueryViewBuilder(
                dataset_id="dataset_1",
                view_id="my_fake_view",
                description="my_fake_view description",
                view_query_template="SELECT NULL LIMIT 0",
                should_materialize=True,
            ),
            SimpleBigQueryViewBuilder(
                dataset_id="dataset_2",
                view_id="my_fake_view_2",
                description="my_fake_view_2 description",
                view_query_template="SELECT NULL LIMIT 0",
                should_materialize=True,
                materialized_address_override=BigQueryAddress(
                    dataset_id="materialized_dataset", table_id="table_materialized"
                ),
            ),
        ]

        prefix = "my_prefix"
        overrides = address_overrides_for_view_builders(prefix, view_builders)

        self.assert_has_overrides_for_all_builders(
            overrides, view_builders, prefix, expected_skipped_datasets=set()
        )

        self.assertIsNone(
            overrides.get_sandbox_address(
                BigQueryAddress(
                    dataset_id=DATAFLOW_METRICS_DATASET, table_id="some_random_table"
                )
            )
        )

    def assert_has_overrides_for_all_builders(
        self,
        overrides: BigQueryAddressOverrides,
        builders: Sequence[BigQueryViewBuilder],
        expected_prefix: str,
        expected_skipped_datasets: Set[str],
    ) -> None:
        """Throws if there isn't an override set for each of the provided builders."""
        for builder in builders:
            if builder.dataset_id in expected_skipped_datasets:
                continue
            address = BigQueryAddress(
                dataset_id=builder.dataset_id, table_id=builder.view_id
            )
            override_address = overrides.get_sandbox_address(address)
            if override_address is None:
                raise ValueError(f"Found no override for {address}")
            self.assertTrue(override_address.dataset_id.startswith(expected_prefix))

            if builder.materialized_address:
                sandbox_materialized_address = overrides.get_sandbox_address(
                    builder.materialized_address
                )
                if sandbox_materialized_address is None:
                    raise ValueError(
                        f"Found no override for {builder.materialized_address}"
                    )
                self.assertTrue(
                    sandbox_materialized_address.dataset_id.startswith(expected_prefix)
                )

    def test_address_overrides_for_input_source_tables_empty(self) -> None:
        overrides = address_overrides_for_input_source_tables({})

        self.assertEqual(
            None,
            overrides.get_sandbox_address(BigQueryAddress.from_str("random.table")),
        )

    def test_address_overrides_for_input_source_tables(self) -> None:
        overrides = address_overrides_for_input_source_tables(
            {
                "us_ca_state": "some_prefix_us_ca_state",
                "us_ca_normalized_state": "another_prefix_us_ca_normalized_state",
            }
        )

        self.assertEqual(
            None,
            overrides.get_sandbox_address(BigQueryAddress.from_str("random.table")),
        )

        self.assertEqual(
            BigQueryAddress.from_str("some_prefix_us_ca_state.state_staff"),
            overrides.get_sandbox_address(
                BigQueryAddress.from_str("us_ca_state.state_staff"),
            ),
        )

        self.assertEqual(
            BigQueryAddress.from_str(
                "another_prefix_us_ca_normalized_state.state_person"
            ),
            overrides.get_sandbox_address(
                BigQueryAddress.from_str("us_ca_normalized_state.state_person"),
            ),
        )

    def test_address_overrides_for_input_source_tables_invalid_table(self) -> None:
        with self.assertRaisesRegex(
            ValueError,
            r"Dataset \[state\] is not a valid source table dataset - cannot override.",
        ):
            _ = address_overrides_for_input_source_tables(
                {
                    "state": "some_prefix_us_ca_state",
                }
            )

    def test_address_overrides_for_input_source_tables_sandbox_matches_dataset(
        self,
    ) -> None:
        with self.assertRaisesRegex(
            ValueError,
            r"Input dataset override for \[us_ca_state\] must be different than the "
            r"original dataset.",
        ):
            _ = address_overrides_for_input_source_tables(
                {
                    "us_ca_state": "us_ca_state",
                }
            )
