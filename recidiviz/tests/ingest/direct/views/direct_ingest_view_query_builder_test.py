# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2020 Recidiviz, Inc.
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
"""Tests for types defined in direct_ingest_view_query_builder.py"""
import datetime
import unittest

from mock import patch

from recidiviz.ingest.direct.types.direct_ingest_instance import DirectIngestInstance
from recidiviz.ingest.direct.views.direct_ingest_view_query_builder import (
    UPDATE_DATETIME_PARAM_NAME,
    DirectIngestViewQueryBuilder,
)
from recidiviz.tests.ingest.direct import fake_regions as fake_regions_module


class DirectIngestViewQueryBuilderTest(unittest.TestCase):
    """Tests for types defined in direct_ingest_view_query_builder.py"""

    PROJECT_ID = "recidiviz-456"

    DEFAULT_CONFIG = DirectIngestViewQueryBuilder.QueryStructureConfig(
        raw_data_datetime_upper_bound=datetime.datetime(2000, 1, 2, 3, 4, 5, 6),
        raw_data_source_instance=DirectIngestInstance.PRIMARY,
    )

    def setUp(self) -> None:
        self.metadata_patcher = patch("recidiviz.utils.metadata.project_id")
        self.mock_project_id_fn = self.metadata_patcher.start()
        self.mock_project_id_fn.return_value = self.PROJECT_ID
        self.maxDiff = None

    def tearDown(self) -> None:
        self.metadata_patcher.stop()

    def test_direct_ingest_preprocessed_view(self) -> None:
        view_query_template = """SELECT * FROM {file_tag_first}
LEFT OUTER JOIN {file_tag_second}
USING (col1);"""

        view = DirectIngestViewQueryBuilder(
            region="us_xx",
            ingest_view_name="ingest_view_tag",
            view_query_template=view_query_template,
            region_module=fake_regions_module,
        )

        expected_parameterized_view_query = """WITH
-- Pulls the latest data from file_tag_first received on or before 2000-01-02 03:04:05.000006
file_tag_first_generated_view AS (
    WITH filtered_rows AS (
        SELECT
            * EXCEPT (recency_rank)
        FROM (
            SELECT
                *,
                ROW_NUMBER() OVER (PARTITION BY col_name_1a, col_name_1b
                                   ORDER BY update_datetime DESC) AS recency_rank
            FROM
                `recidiviz-456.us_xx_raw_data.file_tag_first`
            WHERE update_datetime <= DATETIME "2000-01-02T03:04:05.000006"
        ) a
        WHERE
            recency_rank = 1
            AND is_deleted = False
    )
    SELECT col_name_1a, col_name_1b
    FROM filtered_rows
),
-- Pulls the latest data from file_tag_second received on or before 2000-01-02 03:04:05.000006
file_tag_second_generated_view AS (
    WITH filtered_rows AS (
        SELECT
            * EXCEPT (recency_rank)
        FROM (
            SELECT
                *,
                ROW_NUMBER() OVER (PARTITION BY col_name_2a
                                   ORDER BY update_datetime DESC) AS recency_rank
            FROM
                `recidiviz-456.us_xx_raw_data.file_tag_second`
            WHERE update_datetime <= DATETIME "2000-01-02T03:04:05.000006"
        ) a
        WHERE
            recency_rank = 1
            AND is_deleted = False
    )
    SELECT col_name_2a
    FROM filtered_rows
)
SELECT * FROM file_tag_first_generated_view
LEFT OUTER JOIN file_tag_second_generated_view
USING (col1);"""

        self.assertEqual(
            expected_parameterized_view_query,
            view.build_query(query_structure_config=self.DEFAULT_CONFIG),
        )

    def test_direct_ingest_preprocessed_view_no_raw_file_config_columns_defined(
        self,
    ) -> None:
        view_query_template = """SELECT * FROM {tagColumnsMissing};"""

        view = DirectIngestViewQueryBuilder(
            region="us_ww",
            ingest_view_name="ingest_view_tag",
            view_query_template=view_query_template,
            region_module=fake_regions_module,
        )
        with self.assertRaisesRegex(
            ValueError,
            r"^Cannot use undocumented raw file \[tagColumnsMissing\] as a dependency "
            r"in an ingest view.$",
        ):
            view.build_query(query_structure_config=self.DEFAULT_CONFIG)

    def test_direct_ingest_preprocessed_view_with_reference_table(self) -> None:
        view_query_template = """SELECT * FROM {file_tag_first}
LEFT OUTER JOIN `{project_id}.reference_tables.my_table`
USING (col1);"""

        view = DirectIngestViewQueryBuilder(
            region="us_xx",
            ingest_view_name="ingest_view_tag",
            view_query_template=view_query_template,
            region_module=fake_regions_module,
        )

        self.assertEqual(
            {"file_tag_first"},
            view.raw_data_table_dependency_file_tags,
        )

        expected_view_query = """WITH
-- Pulls the latest data from file_tag_first received on or before 2000-01-02 03:04:05.000006
file_tag_first_generated_view AS (
    WITH filtered_rows AS (
        SELECT
            * EXCEPT (recency_rank)
        FROM (
            SELECT
                *,
                ROW_NUMBER() OVER (PARTITION BY col_name_1a, col_name_1b
                                   ORDER BY update_datetime DESC) AS recency_rank
            FROM
                `recidiviz-456.us_xx_raw_data.file_tag_first`
            WHERE update_datetime <= DATETIME "2000-01-02T03:04:05.000006"
        ) a
        WHERE
            recency_rank = 1
            AND is_deleted = False
    )
    SELECT col_name_1a, col_name_1b
    FROM filtered_rows
)
SELECT * FROM file_tag_first_generated_view
LEFT OUTER JOIN `recidiviz-456.reference_tables.my_table`
USING (col1);"""

        self.assertEqual(
            expected_view_query,
            view.build_query(query_structure_config=self.DEFAULT_CONFIG),
        )

        star = "*"
        view_query_template_but_inside_an_f_string = f"""SELECT {star} FROM {{file_tag_first}}
LEFT OUTER JOIN `{{project_id}}.reference_tables.my_table`
USING (col1);"""

        view = DirectIngestViewQueryBuilder(
            region="us_xx",
            ingest_view_name="ingest_view_tag",
            view_query_template=view_query_template_but_inside_an_f_string,
            region_module=fake_regions_module,
        )
        self.assertEqual(
            expected_view_query,
            view.build_query(query_structure_config=self.DEFAULT_CONFIG),
        )

        expected_date_parameterized_view_query = """WITH
-- Pulls the latest data from file_tag_first received on or before 2000-01-02 03:04:05.000006
file_tag_first_generated_view AS (
    WITH filtered_rows AS (
        SELECT
            * EXCEPT (recency_rank)
        FROM (
            SELECT
                *,
                ROW_NUMBER() OVER (PARTITION BY col_name_1a, col_name_1b
                                   ORDER BY update_datetime DESC) AS recency_rank
            FROM
                `recidiviz-456.us_xx_raw_data.file_tag_first`
            WHERE update_datetime <= DATETIME "2000-01-02T03:04:05.000006"
        ) a
        WHERE
            recency_rank = 1
            AND is_deleted = False
    )
    SELECT col_name_1a, col_name_1b
    FROM filtered_rows
)
SELECT * FROM file_tag_first_generated_view
LEFT OUTER JOIN `recidiviz-456.reference_tables.my_table`
USING (col1);"""

        self.assertEqual(
            expected_date_parameterized_view_query,
            view.build_query(query_structure_config=self.DEFAULT_CONFIG),
        )

    def test_direct_ingest_preprocessed_view_same_table_multiple_places(self) -> None:
        view_query_template = """SELECT * FROM {file_tag_first}
LEFT OUTER JOIN {file_tag_first}
USING (col1);"""

        view = DirectIngestViewQueryBuilder(
            region="us_xx",
            ingest_view_name="ingest_view_tag",
            view_query_template=view_query_template,
            region_module=fake_regions_module,
        )

        self.assertEqual(
            {"file_tag_first"},
            view.raw_data_table_dependency_file_tags,
        )

        expected_view_query = """WITH
-- Pulls the latest data from file_tag_first received on or before 2000-01-02 03:04:05.000006
file_tag_first_generated_view AS (
    WITH filtered_rows AS (
        SELECT
            * EXCEPT (recency_rank)
        FROM (
            SELECT
                *,
                ROW_NUMBER() OVER (PARTITION BY col_name_1a, col_name_1b
                                   ORDER BY update_datetime DESC) AS recency_rank
            FROM
                `recidiviz-456.us_xx_raw_data.file_tag_first`
            WHERE update_datetime <= DATETIME "2000-01-02T03:04:05.000006"
        ) a
        WHERE
            recency_rank = 1
            AND is_deleted = False
    )
    SELECT col_name_1a, col_name_1b
    FROM filtered_rows
)
SELECT * FROM file_tag_first_generated_view
LEFT OUTER JOIN file_tag_first_generated_view
USING (col1);"""

        self.assertEqual(
            expected_view_query,
            view.build_query(query_structure_config=self.DEFAULT_CONFIG),
        )

    def test_direct_ingest_preprocessed_view_with_subqueries(self) -> None:
        view_query_template = """WITH
foo AS (SELECT * FROM bar)
SELECT * FROM {file_tag_first}
LEFT OUTER JOIN {file_tag_second}
USING (col1);"""

        view = DirectIngestViewQueryBuilder(
            region="us_xx",
            ingest_view_name="ingest_view_tag",
            view_query_template=view_query_template,
            region_module=fake_regions_module,
        )

        self.assertEqual(
            {"file_tag_first", "file_tag_second"},
            view.raw_data_table_dependency_file_tags,
        )

        expected_parameterized_view_query = """WITH
-- Pulls the latest data from file_tag_first received on or before 2000-01-02 03:04:05.000006
file_tag_first_generated_view AS (
    WITH filtered_rows AS (
        SELECT
            * EXCEPT (recency_rank)
        FROM (
            SELECT
                *,
                ROW_NUMBER() OVER (PARTITION BY col_name_1a, col_name_1b
                                   ORDER BY update_datetime DESC) AS recency_rank
            FROM
                `recidiviz-456.us_xx_raw_data.file_tag_first`
            WHERE update_datetime <= DATETIME "2000-01-02T03:04:05.000006"
        ) a
        WHERE
            recency_rank = 1
            AND is_deleted = False
    )
    SELECT col_name_1a, col_name_1b
    FROM filtered_rows
),
-- Pulls the latest data from file_tag_second received on or before 2000-01-02 03:04:05.000006
file_tag_second_generated_view AS (
    WITH filtered_rows AS (
        SELECT
            * EXCEPT (recency_rank)
        FROM (
            SELECT
                *,
                ROW_NUMBER() OVER (PARTITION BY col_name_2a
                                   ORDER BY update_datetime DESC) AS recency_rank
            FROM
                `recidiviz-456.us_xx_raw_data.file_tag_second`
            WHERE update_datetime <= DATETIME "2000-01-02T03:04:05.000006"
        ) a
        WHERE
            recency_rank = 1
            AND is_deleted = False
    )
    SELECT col_name_2a
    FROM filtered_rows
),
foo AS (SELECT * FROM bar)
SELECT * FROM file_tag_first_generated_view
LEFT OUTER JOIN file_tag_second_generated_view
USING (col1);"""

        self.assertEqual(
            expected_parameterized_view_query,
            view.build_query(query_structure_config=self.DEFAULT_CONFIG),
        )

        # Also check that appending whitespace on or before the WITH prefix produces the same results
        view_query_template = "\n " + view_query_template

        view = DirectIngestViewQueryBuilder(
            region="us_xx",
            ingest_view_name="ingest_view_tag",
            view_query_template=view_query_template,
            region_module=fake_regions_module,
        )

        self.assertEqual(
            {"file_tag_first", "file_tag_second"},
            view.raw_data_table_dependency_file_tags,
        )

        self.assertEqual(
            expected_parameterized_view_query,
            view.build_query(query_structure_config=self.DEFAULT_CONFIG),
        )

    def test_direct_ingest_preprocessed_view_throws_for_unexpected_tag(self) -> None:
        view_query_template = """SELECT * FROM {file_tag_first}
LEFT OUTER JOIN {file_tag_not_in_config}
USING (col1);"""

        view = DirectIngestViewQueryBuilder(
            region="us_xx",
            ingest_view_name="ingest_view_tag",
            view_query_template=view_query_template,
            region_module=fake_regions_module,
        )

        with self.assertRaisesRegex(
            ValueError, r"Found unexpected raw table tag \[file_tag_not_in_config\]"
        ):
            view.build_query(query_structure_config=self.DEFAULT_CONFIG)

    def test_direct_ingest_preprocessed_view_other_materialized_subquery_fails(
        self,
    ) -> None:
        view_query_template = """
CREATE TEMP TABLE my_subquery AS (SELECT * FROM {file_tag_first});
SELECT * FROM my_subquery;"""

        with self.assertRaisesRegex(
            ValueError,
            "^Found CREATE TEMP TABLE clause in this query - ingest views cannot contain CREATE clauses.$",
        ):
            _ = DirectIngestViewQueryBuilder(
                region="us_xx",
                ingest_view_name="ingest_view_tag",
                view_query_template=view_query_template,
                region_module=fake_regions_module,
            )

    def test_direct_ingest_preprocessed_view_materialized_raw_table_views_permanent_expiring_output_table(
        self,
    ) -> None:
        view_query_template = """WITH
foo AS (SELECT * FROM bar)
SELECT * FROM {file_tag_first}
LEFT OUTER JOIN {file_tag_second}
USING (col1);"""

        view = DirectIngestViewQueryBuilder(
            region="us_xx",
            ingest_view_name="ingest_view_tag",
            view_query_template=view_query_template,
            region_module=fake_regions_module,
        )

        self.assertEqual(
            {"file_tag_first", "file_tag_second"},
            view.raw_data_table_dependency_file_tags,
        )

    def test_direct_ingest_preprocessed_view_with_update_datetime(self) -> None:
        view_query_template = f"""SELECT * FROM {{file_tag_first}}
        WHERE col1 <= @{UPDATE_DATETIME_PARAM_NAME}"""

        view = DirectIngestViewQueryBuilder(
            region="us_xx",
            ingest_view_name="ingest_view_tag",
            view_query_template=view_query_template,
            region_module=fake_regions_module,
        )

        expected_parameterized_view_query = """WITH
-- Pulls the latest data from file_tag_first received on or before 2000-01-02 03:04:05.000006
file_tag_first_generated_view AS (
    WITH filtered_rows AS (
        SELECT
            * EXCEPT (recency_rank)
        FROM (
            SELECT
                *,
                ROW_NUMBER() OVER (PARTITION BY col_name_1a, col_name_1b
                                   ORDER BY update_datetime DESC) AS recency_rank
            FROM
                `recidiviz-456.us_xx_raw_data.file_tag_first`
            WHERE update_datetime <= DATETIME "2000-01-02T03:04:05.000006"
        ) a
        WHERE
            recency_rank = 1
            AND is_deleted = False
    )
    SELECT col_name_1a, col_name_1b
    FROM filtered_rows
)
SELECT * FROM file_tag_first_generated_view
        WHERE col1 <= DATETIME "2000-01-02T03:04:05.000006";"""

        self.assertEqual(
            expected_parameterized_view_query,
            view.build_query(query_structure_config=self.DEFAULT_CONFIG),
        )

    def test_direct_ingest_preprocessed_view_with_current_date(self) -> None:
        for current_date_fn in [
            "CURRENT_DATE('US/Eastern')",
            # Split up to avoid the lint check for this function used without a timezone
            "CURRENT_DATE(" + ")",
            "current_date(" + ")",
        ]:
            view_query_template = f"""SELECT * FROM {{file_tag_first}}
            WHERE col1 <= {current_date_fn}"""
            with self.assertRaisesRegex(
                ValueError,
                "Found CURRENT_DATE function in this query - ingest views cannot contain "
                "CURRENT_DATE functions. Consider using @update_timestamp instead.",
            ):
                DirectIngestViewQueryBuilder(
                    region="us_xx",
                    ingest_view_name="ingest_view_tag",
                    view_query_template=view_query_template,
                )

    def test_one_all_rows_dependency(self) -> None:
        view_query_template = """SELECT * FROM {file_tag_first}
LEFT OUTER JOIN {file_tag_second@ALL}
USING (col1);"""

        view = DirectIngestViewQueryBuilder(
            region="us_xx",
            ingest_view_name="ingest_view_tag",
            view_query_template=view_query_template,
            region_module=fake_regions_module,
        )

        self.assertEqual(
            {"file_tag_first", "file_tag_second"},
            view.raw_data_table_dependency_file_tags,
        )
        expected_parameterized_view_query = """WITH
-- Pulls the latest data from file_tag_first received on or before 2000-01-02 03:04:05.000006
file_tag_first_generated_view AS (
    WITH filtered_rows AS (
        SELECT
            * EXCEPT (recency_rank)
        FROM (
            SELECT
                *,
                ROW_NUMBER() OVER (PARTITION BY col_name_1a, col_name_1b
                                   ORDER BY update_datetime DESC) AS recency_rank
            FROM
                `recidiviz-456.us_xx_raw_data.file_tag_first`
            WHERE update_datetime <= DATETIME "2000-01-02T03:04:05.000006"
        ) a
        WHERE
            recency_rank = 1
            AND is_deleted = False
    )
    SELECT col_name_1a, col_name_1b
    FROM filtered_rows
),
-- Pulls all rows from file_tag_second received on or before 2000-01-02 03:04:05.000006
file_tag_second__ALL_generated_view AS (
    WITH filtered_rows AS (
        SELECT *
        FROM `recidiviz-456.us_xx_raw_data.file_tag_second`
        WHERE update_datetime <= DATETIME "2000-01-02T03:04:05.000006"
    )
    SELECT col_name_2a, update_datetime
    FROM filtered_rows
)
SELECT * FROM file_tag_first_generated_view
LEFT OUTER JOIN file_tag_second__ALL_generated_view
USING (col1);"""

        self.assertEqual(
            expected_parameterized_view_query,
            view.build_query(query_structure_config=self.DEFAULT_CONFIG),
        )

    def test_all_rows_dependency_mixed_with_latest_same_table(self) -> None:
        view_query_template = """SELECT * FROM {file_tag_first}
LEFT OUTER JOIN {file_tag_first@ALL}
USING (col1);"""

        view = DirectIngestViewQueryBuilder(
            region="us_xx",
            ingest_view_name="ingest_view_tag",
            view_query_template=view_query_template,
            region_module=fake_regions_module,
        )

        self.assertEqual(
            {"file_tag_first"},
            view.raw_data_table_dependency_file_tags,
        )

        self.assertEqual(
            ["file_tag_first", "file_tag_first@ALL"],
            [
                c.raw_table_dependency_arg_name
                for c in view.raw_table_dependency_configs
            ],
        )

        expected_parameterized_view_query = """WITH
-- Pulls the latest data from file_tag_first received on or before 2000-01-02 03:04:05.000006
file_tag_first_generated_view AS (
    WITH filtered_rows AS (
        SELECT
            * EXCEPT (recency_rank)
        FROM (
            SELECT
                *,
                ROW_NUMBER() OVER (PARTITION BY col_name_1a, col_name_1b
                                   ORDER BY update_datetime DESC) AS recency_rank
            FROM
                `recidiviz-456.us_xx_raw_data.file_tag_first`
            WHERE update_datetime <= DATETIME "2000-01-02T03:04:05.000006"
        ) a
        WHERE
            recency_rank = 1
            AND is_deleted = False
    )
    SELECT col_name_1a, col_name_1b
    FROM filtered_rows
),
-- Pulls all rows from file_tag_first received on or before 2000-01-02 03:04:05.000006
file_tag_first__ALL_generated_view AS (
    WITH filtered_rows AS (
        SELECT *
        FROM `recidiviz-456.us_xx_raw_data.file_tag_first`
        WHERE update_datetime <= DATETIME "2000-01-02T03:04:05.000006"
    )
    SELECT col_name_1a, col_name_1b, update_datetime
    FROM filtered_rows
)
SELECT * FROM file_tag_first_generated_view
LEFT OUTER JOIN file_tag_first__ALL_generated_view
USING (col1);"""

        self.assertEqual(
            expected_parameterized_view_query,
            view.build_query(query_structure_config=self.DEFAULT_CONFIG),
        )

    def test_invalid_raw_table_dependency(self) -> None:
        # This raw table dependency string doesn't match the regext at all
        view_query_template = """SELECT * FROM {file_tag_first#ALL};"""

        view = DirectIngestViewQueryBuilder(
            region="us_xx",
            ingest_view_name="ingest_view_tag",
            view_query_template=view_query_template,
            region_module=fake_regions_module,
        )

        with self.assertRaisesRegex(
            ValueError,
            r"Found raw table dependency format arg \[file_tag_first#ALL\] which does "
            r"not match the expected pattern.",
        ):
            _ = view.raw_table_dependency_configs

    def test_invalid_raw_table_dependency_bad_filter_info(self) -> None:
        # This raw table dependency matches the regex but uses a bad filter string.
        view_query_template = """SELECT * FROM {file_tag_first@ALL_ROWS};"""

        view = DirectIngestViewQueryBuilder(
            region="us_xx",
            ingest_view_name="ingest_view_tag",
            view_query_template=view_query_template,
            region_module=fake_regions_module,
        )

        with self.assertRaisesRegex(
            ValueError,
            r"Found unexpected filter info string \[ALL_ROWS\] on raw table "
            r"dependency \[file_tag_first@ALL_ROWS\]",
        ):
            _ = view.raw_table_dependency_configs
