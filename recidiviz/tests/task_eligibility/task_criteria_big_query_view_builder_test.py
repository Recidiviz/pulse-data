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
"""Tests for TaskCriteriaBigQueryViewBuilder classes."""
import unittest
from unittest.mock import Mock, patch

from recidiviz.big_query.address_overrides import BigQueryAddressOverrides
from recidiviz.big_query.big_query_address import BigQueryAddress
from recidiviz.common.constants.states import StateCode
from recidiviz.task_eligibility.task_criteria_big_query_view_builder import (
    StateAgnosticTaskCriteriaBigQueryViewBuilder,
    StateSpecificTaskCriteriaBigQueryViewBuilder,
)


@patch("recidiviz.utils.metadata.project_id", Mock(return_value="recidiviz-456"))
class TestStateSpecificTaskCriteriaBigQueryViewBuilder(unittest.TestCase):
    """Tests for the StateSpecificTaskCriteriaBigQueryViewBuilder."""

    def setUp(self) -> None:
        self.us_xx_criteria_dataset = "task_eligibility_criteria_us_xx"

    def test_simple_criteria(self) -> None:
        builder = StateSpecificTaskCriteriaBigQueryViewBuilder(
            state_code=StateCode.US_XX,
            criteria_name="US_XX_SIMPLE_CRITERIA",
            criteria_spans_query_template="SELECT * FROM `{project_id}.{raw_data_dataset}.foo`;",
            description="Simple criteria description",
            raw_data_dataset="raw_data",
        )
        view = builder.build()

        self.assertEqual(StateCode.US_XX, builder.state_code)
        self.assertEqual("US_XX_SIMPLE_CRITERIA", builder.criteria_name)
        self.assertEqual(
            view.address,
            BigQueryAddress(
                dataset_id=self.us_xx_criteria_dataset, table_id="simple_criteria"
            ),
        )
        self.assertEqual(
            view.materialized_address,
            BigQueryAddress(
                dataset_id=self.us_xx_criteria_dataset,
                table_id="simple_criteria_materialized",
            ),
        )

        self.assertEqual(view.view_query, "SELECT * FROM `recidiviz-456.raw_data.foo`;")

    def test_build_with_address_overrides(self) -> None:
        builder = StateSpecificTaskCriteriaBigQueryViewBuilder(
            state_code=StateCode.US_XX,
            criteria_name="US_XX_SIMPLE_CRITERIA",
            criteria_spans_query_template="SELECT * FROM `{project_id}.{raw_data_dataset}.foo`;",
            description="Simple criteria description",
            raw_data_dataset="raw_data",
        )
        address_overrides = (
            BigQueryAddressOverrides.Builder("my_prefix")
            .register_sandbox_override_for_entire_dataset("raw_data")
            .register_sandbox_override_for_entire_dataset(self.us_xx_criteria_dataset)
            .build()
        )
        view = builder.build(address_overrides=address_overrides)

        self.assertEqual(StateCode.US_XX, builder.state_code)
        self.assertEqual("US_XX_SIMPLE_CRITERIA", builder.criteria_name)
        self.assertEqual(
            view.address,
            BigQueryAddress(
                dataset_id=f"my_prefix_{self.us_xx_criteria_dataset}",
                table_id="simple_criteria",
            ),
        )
        self.assertEqual(
            view.materialized_address,
            BigQueryAddress(
                dataset_id=f"my_prefix_{self.us_xx_criteria_dataset}",
                table_id="simple_criteria_materialized",
            ),
        )

        self.assertEqual(
            view.view_query, "SELECT * FROM `recidiviz-456.my_prefix_raw_data.foo`;"
        )

    def test_no_state_prefix_on_criteria_throws(self) -> None:
        with self.assertRaisesRegex(
            ValueError,
            r"Found state-specific task criteria \[SIMPLE_CRITERIA\] whose name does not start with "
            r"\[US_XX_\].",
        ):
            _ = StateSpecificTaskCriteriaBigQueryViewBuilder(
                state_code=StateCode.US_XX,
                criteria_name="SIMPLE_CRITERIA",
                criteria_spans_query_template="SELECT * FROM `{project_id}.{raw_data_dataset}.foo`;",
                description="Simple criteria description",
                raw_data_dataset="raw_data",
            )

    def test_wrong_state_prefix_on_criteria_throws(self) -> None:
        with self.assertRaisesRegex(
            ValueError,
            r"Found state-specific task criteria \[US_YY_SIMPLE_CRITERIA\] whose name does not start with "
            r"\[US_XX_\].",
        ):
            _ = StateSpecificTaskCriteriaBigQueryViewBuilder(
                state_code=StateCode.US_XX,
                criteria_name="US_YY_SIMPLE_CRITERIA",
                criteria_spans_query_template="SELECT * FROM `{project_id}.{raw_data_dataset}.foo`;",
                description="Simple criteria description",
                raw_data_dataset="raw_data",
            )

    def test_lowercase_criteria_name_throws(self) -> None:
        with self.assertRaisesRegex(
            ValueError,
            r"Criteria name \[US_XX_simple_criteria\] must be upper case.",
        ):
            _ = StateSpecificTaskCriteriaBigQueryViewBuilder(
                state_code=StateCode.US_XX,
                criteria_name="US_XX_simple_criteria",
                criteria_spans_query_template="SELECT * FROM `{project_id}.{raw_data_dataset}.foo`;",
                description="Simple criteria description",
                raw_data_dataset="raw_data",
            )


@patch("recidiviz.utils.metadata.project_id", Mock(return_value="recidiviz-456"))
class TestStateAgnosticTaskCriteriaBigQueryViewBuilder(unittest.TestCase):
    """Tests for the StateAgnosticTaskCriteriaBigQueryViewBuilder."""

    def setUp(self) -> None:
        self.general_criteria_dataset = "task_eligibility_criteria_general"

    def test_simple_criteria(self) -> None:
        builder = StateAgnosticTaskCriteriaBigQueryViewBuilder(
            criteria_name="SIMPLE_CRITERIA",
            criteria_spans_query_template="SELECT * FROM `{project_id}.{ingested_data_dataset}.foo`;",
            description="Simple criteria description",
            ingested_data_dataset="ingested_data",
        )
        view = builder.build()

        self.assertEqual("SIMPLE_CRITERIA", builder.criteria_name)
        self.assertEqual(
            view.address,
            BigQueryAddress(
                dataset_id=self.general_criteria_dataset,
                table_id="simple_criteria",
            ),
        )
        self.assertEqual(
            view.materialized_address,
            BigQueryAddress(
                dataset_id=self.general_criteria_dataset,
                table_id="simple_criteria_materialized",
            ),
        )

        self.assertEqual(
            view.view_query, "SELECT * FROM `recidiviz-456.ingested_data.foo`;"
        )

    def test_build_with_address_overrides(self) -> None:
        builder = StateAgnosticTaskCriteriaBigQueryViewBuilder(
            criteria_name="SIMPLE_CRITERIA",
            criteria_spans_query_template="SELECT * FROM `{project_id}.{ingested_data_dataset}.foo`;",
            description="Simple criteria description",
            ingested_data_dataset="ingested_data",
        )
        address_overrides = (
            BigQueryAddressOverrides.Builder("my_prefix")
            .register_sandbox_override_for_entire_dataset("ingested_data")
            .register_sandbox_override_for_entire_dataset(self.general_criteria_dataset)
            .build()
        )
        view = builder.build(address_overrides=address_overrides)

        self.assertEqual("SIMPLE_CRITERIA", builder.criteria_name)
        self.assertEqual(
            view.address,
            BigQueryAddress(
                dataset_id=f"my_prefix_{self.general_criteria_dataset}",
                table_id="simple_criteria",
            ),
        )
        self.assertEqual(
            view.materialized_address,
            BigQueryAddress(
                dataset_id=f"my_prefix_{self.general_criteria_dataset}",
                table_id="simple_criteria_materialized",
            ),
        )

        self.assertEqual(
            view.view_query,
            "SELECT * FROM `recidiviz-456.my_prefix_ingested_data.foo`;",
        )

    def test_lowercase_criteria_name_throws(self) -> None:
        with self.assertRaisesRegex(
            ValueError,
            r"Criteria name \[simple_criteria\] must be upper case.",
        ):
            _ = StateAgnosticTaskCriteriaBigQueryViewBuilder(
                criteria_name="simple_criteria",
                criteria_spans_query_template="SELECT * FROM `{project_id}.{ingested_data_dataset}.foo`;",
                description="Simple criteria description",
                ingested_data_dataset="ingested_data",
            )
