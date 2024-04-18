# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2023 Recidiviz, Inc.
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
"""Tests for raw_table_relationship_info.py."""
import unittest

import attr

from recidiviz.ingest.direct.raw_data.raw_table_relationship_info import (
    ColumnEqualityJoinBooleanClause,
    ColumnFilterJoinBooleanClause,
    JoinBooleanClause,
    JoinColumn,
    JoinTransform,
    RawDataJoinCardinality,
    RawTableRelationshipInfo,
)


class TestRawDataJoinCardinality(unittest.TestCase):
    def test_invert_cardinalities(self) -> None:
        for cardinality in RawDataJoinCardinality:
            # Test passes if all cardinalities can be inverted
            _ = cardinality.invert()


class TestRawTableColumn(unittest.TestCase):
    def test_parse_from_string(self) -> None:
        self.assertEqual(
            JoinColumn(file_tag="my_table", column="my_col"),
            JoinColumn.parse_from_string("my_table.my_col"),
        )
        self.assertEqual(
            JoinColumn(file_tag="123Table", column="123_col"),
            JoinColumn.parse_from_string("123Table.123_col"),
        )
        self.assertIsNone(
            JoinColumn.parse_from_string("'SOME_VAL'"),
        )
        self.assertIsNone(
            JoinColumn.parse_from_string("this.is.invalid"),
        )


class TestJoinTransform(unittest.TestCase):
    def test_invalid_transform(self) -> None:
        with self.assertRaisesRegex(
            ValueError, r"Transformation .* does not include the string \{col_name\}"
        ):
            _ = JoinTransform(
                column=JoinColumn(file_tag="my_table", column="my_col"),
                transformation="invalid transformation",
            )


class TestJoinBooleanClause(unittest.TestCase):
    """Tests for JoinBooleanClause and subclasses."""

    def test_parse_from_string_column_equality(self) -> None:
        self.assertEqual(
            ColumnEqualityJoinBooleanClause(
                column_1=JoinColumn(file_tag="my_table", column="my_col"),
                column_2=JoinColumn(file_tag="my_table_2", column="my_col"),
            ),
            JoinBooleanClause.parse_from_string("my_table.my_col = my_table_2.my_col"),
        )

        # White space doesn't matter
        self.assertEqual(
            ColumnEqualityJoinBooleanClause(
                column_1=JoinColumn(file_tag="my_table", column="my_col"),
                column_2=JoinColumn(file_tag="my_table_2", column="my_col"),
            ),
            JoinBooleanClause.parse_from_string(" my_table.my_col=my_table_2.my_col  "),
        )

    def test_parse_from_string_column_filter(self) -> None:
        # String quotes preserved for filter value
        self.assertEqual(
            ColumnFilterJoinBooleanClause(
                column=JoinColumn(file_tag="my_table", column="my_col"),
                filter_value='"A"',
            ),
            JoinBooleanClause.parse_from_string('my_table.my_col = "A"'),
        )

        # Type of quotes doesn't matter
        self.assertEqual(
            ColumnFilterJoinBooleanClause(
                column=JoinColumn(file_tag="my_table", column="my_col"),
                filter_value="'A'",
            ),
            JoinBooleanClause.parse_from_string("my_table.my_col = 'A'"),
        )

        # Filters can be numerical values
        self.assertEqual(
            ColumnFilterJoinBooleanClause(
                column=JoinColumn(file_tag="my_table", column="my_col"),
                filter_value="1234",
            ),
            JoinBooleanClause.parse_from_string("my_table.my_col = 1234"),
        )

        # White space doesn't matter
        self.assertEqual(
            ColumnFilterJoinBooleanClause(
                column=JoinColumn(file_tag="my_table", column="my_col"),
                filter_value='"A"',
            ),
            JoinBooleanClause.parse_from_string(' my_table.my_col   ="A"'),
        )

    def test_parse_from_string_no_column_on_left(self) -> None:
        with self.assertRaisesRegex(
            ValueError,
            r"Left side of equals sign in the join clause must always be a column.",
        ):
            JoinBooleanClause.parse_from_string('"A" = my_table.my_col')

    def test_parse_from_string_invalid_filter_value(self) -> None:
        with self.assertRaisesRegex(
            ValueError,
            r"Found invalid filter value \[this_should_be_quoted\] in a join clause.",
        ):
            JoinBooleanClause.parse_from_string(
                "my_table.my_col = this_should_be_quoted"
            )

    def test_parse_from_string_wrong_boolean_operator(self) -> None:
        with self.assertRaisesRegex(
            ValueError,
            r"Found unsupported operator in join clause "
            r"\[my_table\.my_col != my_table_2\.my_col\]: \[!=\]\.",
        ):
            JoinBooleanClause.parse_from_string("my_table.my_col != my_table_2.my_col")

        with self.assertRaisesRegex(
            ValueError,
            r"Found unsupported operator in join clause "
            r"\[my_table\.my_col <= my_table_2\.my_col\]: \[<=\]\.",
        ):
            JoinBooleanClause.parse_from_string("my_table.my_col <= my_table_2.my_col")

        with self.assertRaisesRegex(
            ValueError,
            r"Found unsupported operator in join clause "
            r"\[my_table\.my_col >= my_table_2\.my_col\]: \[>=\]\.",
        ):
            JoinBooleanClause.parse_from_string("my_table.my_col >= my_table_2.my_col")

        with self.assertRaisesRegex(
            ValueError,
            r"Found unsupported operator in join clause "
            r"\[my_table\.my_col == my_table_2\.my_col\]: \[==\]\.",
        ):
            JoinBooleanClause.parse_from_string("my_table.my_col == my_table_2.my_col")

        with self.assertRaisesRegex(
            ValueError,
            r"Found unsupported operator in join clause "
            r"\[my_table.my_col IN \('A', 'B'\)\]: \[IN\]\.",
        ):
            JoinBooleanClause.parse_from_string("my_table.my_col IN ('A', 'B')")

    def test_parse_from_string_no_operators(self) -> None:
        with self.assertRaisesRegex(
            ValueError,
            r"Unable to parse join clause \[my_table.my_col my_table_2.my_col\]",
        ):
            JoinBooleanClause.parse_from_string("my_table.my_col my_table_2.my_col")

        with self.assertRaisesRegex(
            ValueError,
            r"Unable to parse join clause \[my_table.my_col\]",
        ):
            JoinBooleanClause.parse_from_string("my_table.my_col")

    def test_parse_from_string_more_than_one_operator(self) -> None:
        with self.assertRaisesRegex(
            ValueError,
            r"Unable to parse join clause "
            r"\[my_table.my_col = my_table_2.my_col = my_table_3.my_col\]",
        ):
            JoinBooleanClause.parse_from_string(
                "my_table.my_col = my_table_2.my_col = my_table_3.my_col"
            )

    def test_valid_column_equality_clause(self) -> None:
        clause = ColumnEqualityJoinBooleanClause(
            column_1=JoinColumn(file_tag="my_table", column="my_col"),
            column_2=JoinColumn(file_tag="my_table_2", column="my_col"),
        )

        self.assertEqual("my_table.my_col = my_table_2.my_col", clause.to_sql())
        self.assertEqual(
            [
                JoinColumn(file_tag="my_table", column="my_col"),
                JoinColumn(file_tag="my_table_2", column="my_col"),
            ],
            clause.get_referenced_columns(),
        )

        # When column orders are reversed, the SQL statement preserves initialization
        # order, but the referenced columns are sorted.
        reversed_clause = ColumnEqualityJoinBooleanClause(
            column_1=JoinColumn(file_tag="my_table_2", column="my_col"),
            column_2=JoinColumn(file_tag="my_table", column="my_col"),
        )

        self.assertEqual(
            "my_table_2.my_col = my_table.my_col", reversed_clause.to_sql()
        )
        self.assertEqual(
            [
                JoinColumn(file_tag="my_table", column="my_col"),
                JoinColumn(file_tag="my_table_2", column="my_col"),
            ],
            reversed_clause.get_referenced_columns(),
        )

        # Clauses are treated as equal regardless of column order.
        self.assertEqual(clause, reversed_clause)

        # Clauses can be added to sets
        clause_set = {clause}

        # Equality applies when checking set membership
        self.assertIn(reversed_clause, clause_set)

        self.assertNotIn(
            ColumnEqualityJoinBooleanClause(
                column_1=JoinColumn(file_tag="my_table", column="my_col"),
                column_2=JoinColumn(file_tag="my_table_3", column="my_col"),
            ),
            clause_set,
        )

    def test_self_join_equality_clause_allowed(self) -> None:
        clause = ColumnEqualityJoinBooleanClause(
            column_1=JoinColumn(file_tag="my_table", column="my_col"),
            column_2=JoinColumn(file_tag="my_table", column="my_col_2"),
        )
        self.assertEqual(
            [
                JoinColumn(file_tag="my_table", column="my_col"),
                JoinColumn(file_tag="my_table", column="my_col_2"),
            ],
            clause.get_referenced_columns(),
        )

        reversed_clause = ColumnEqualityJoinBooleanClause(
            column_1=JoinColumn(file_tag="my_table", column="my_col"),
            column_2=JoinColumn(file_tag="my_table", column="my_col_2"),
        )

        self.assertEqual(clause, reversed_clause)

        self.assertEqual(
            [
                JoinColumn(file_tag="my_table", column="my_col"),
                JoinColumn(file_tag="my_table", column="my_col_2"),
            ],
            reversed_clause.get_referenced_columns(),
        )

    def test_column_equality_clause_identical_columns(self) -> None:
        with self.assertRaisesRegex(
            ValueError,
            r"Table relationship join clause found between the same column.",
        ):
            ColumnEqualityJoinBooleanClause(
                column_1=JoinColumn(file_tag="my_table", column="my_col"),
                column_2=JoinColumn(file_tag="my_table", column="my_col"),
            )

    def test_valid_column_filter_clause(self) -> None:
        string_filter_clause = ColumnFilterJoinBooleanClause(
            column=JoinColumn(file_tag="my_table", column="my_col"),
            filter_value='"A"',
        )

        self.assertEqual('my_table.my_col = "A"', string_filter_clause.to_sql())
        self.assertEqual(
            [JoinColumn(file_tag="my_table", column="my_col")],
            string_filter_clause.get_referenced_columns(),
        )

        # Clauses can be added to sets
        clause_set = {string_filter_clause}

        # Clause equality works with copies
        self.assertIn(
            ColumnFilterJoinBooleanClause(
                column=JoinColumn(file_tag="my_table", column="my_col"),
                filter_value='"A"',
            ),
            clause_set,
        )

        # Integer filters allowed
        int_filter_clause = ColumnFilterJoinBooleanClause(
            column=JoinColumn(file_tag="my_table", column="my_col"),
            filter_value="1234",
        )

        self.assertEqual("my_table.my_col = 1234", int_filter_clause.to_sql())
        self.assertEqual(
            [JoinColumn(file_tag="my_table", column="my_col")],
            int_filter_clause.get_referenced_columns(),
        )

    def test_column_filter_clause_invalid_filter(self) -> None:
        with self.assertRaisesRegex(
            ValueError,
            r"Found invalid filter value \[1\.234\] in a join clause.",
        ):
            ColumnFilterJoinBooleanClause(
                column=JoinColumn(file_tag="my_table", column="my_col"),
                filter_value="1.234",
            )

        with self.assertRaisesRegex(
            ValueError,
            r"Found invalid filter value \[abcd\] in a join clause.",
        ):
            ColumnFilterJoinBooleanClause(
                column=JoinColumn(file_tag="my_table", column="my_col"),
                filter_value="abcd",
            )


class TestRawTableRelationshipInfo(unittest.TestCase):
    """Tests for RawTableRelationshipInfo"""

    def test_one_join_clause(self) -> None:
        relationship = RawTableRelationshipInfo(
            file_tag="my_table_2",
            foreign_table="my_table",
            cardinality=RawDataJoinCardinality.ONE_TO_MANY,
            join_clauses=[
                ColumnEqualityJoinBooleanClause(
                    column_1=JoinColumn(file_tag="my_table", column="my_col"),
                    column_2=JoinColumn(file_tag="my_table_2", column="my_col"),
                )
            ],
            transforms=[],
        )
        self.assertEqual("my_table.my_col = my_table_2.my_col", relationship.join_sql())
        self.assertEqual(
            ("my_table", "my_table_2"), relationship.get_referenced_tables()
        )

    def test_multiple_join_clauses(self) -> None:
        relationship = RawTableRelationshipInfo(
            file_tag="my_table",
            foreign_table="my_table_2",
            cardinality=RawDataJoinCardinality.MANY_TO_MANY,
            join_clauses=[
                ColumnEqualityJoinBooleanClause(
                    column_1=JoinColumn(file_tag="my_table", column="my_col"),
                    column_2=JoinColumn(file_tag="my_table_2", column="my_col"),
                ),
                ColumnEqualityJoinBooleanClause(
                    column_1=JoinColumn(file_tag="my_table", column="my_col_b"),
                    column_2=JoinColumn(file_tag="my_table_2", column="my_col_b"),
                ),
                ColumnFilterJoinBooleanClause(
                    column=JoinColumn(file_tag="my_table", column="my_col_b"),
                    filter_value='"SOME_VAL"',
                ),
            ],
            transforms=[],
        )
        self.assertEqual(
            'my_table.my_col = my_table_2.my_col AND my_table.my_col_b = my_table_2.my_col_b AND my_table.my_col_b = "SOME_VAL"',
            relationship.join_sql(),
        )
        self.assertEqual(
            ("my_table", "my_table_2"), relationship.get_referenced_tables()
        )

    def test_invert_relationship_info(self) -> None:
        one_to_one_relationship = RawTableRelationshipInfo(
            file_tag="my_table",
            foreign_table="my_table_2",
            cardinality=RawDataJoinCardinality.ONE_TO_ONE,
            join_clauses=[
                ColumnEqualityJoinBooleanClause(
                    column_1=JoinColumn(file_tag="my_table", column="my_col"),
                    column_2=JoinColumn(file_tag="my_table_2", column="my_col"),
                )
            ],
            transforms=[],
        )
        self.assertEqual(
            RawTableRelationshipInfo(
                file_tag="my_table_2",
                foreign_table="my_table",
                cardinality=RawDataJoinCardinality.ONE_TO_ONE,
                join_clauses=[
                    ColumnEqualityJoinBooleanClause(
                        column_1=JoinColumn(file_tag="my_table", column="my_col"),
                        column_2=JoinColumn(file_tag="my_table_2", column="my_col"),
                    )
                ],
                transforms=[],
            ),
            one_to_one_relationship.invert(),
        )

        many_to_one_relationship = RawTableRelationshipInfo(
            file_tag="my_table",
            foreign_table="my_table_2",
            cardinality=RawDataJoinCardinality.MANY_TO_ONE,
            join_clauses=[
                ColumnEqualityJoinBooleanClause(
                    column_1=JoinColumn(file_tag="my_table", column="my_col"),
                    column_2=JoinColumn(file_tag="my_table_2", column="my_col"),
                )
            ],
            transforms=[],
        )
        self.assertEqual(
            RawTableRelationshipInfo(
                file_tag="my_table_2",
                foreign_table="my_table",
                cardinality=RawDataJoinCardinality.ONE_TO_MANY,
                join_clauses=[
                    ColumnEqualityJoinBooleanClause(
                        column_1=JoinColumn(file_tag="my_table", column="my_col"),
                        column_2=JoinColumn(file_tag="my_table_2", column="my_col"),
                    )
                ],
                transforms=[],
            ),
            many_to_one_relationship.invert(),
        )

    def test_relationship_equality_one_to_one(self) -> None:
        # Reversing order of all elements of a one-to-one or many-to-many relationship
        # does not matter - these will still be treated equally
        relationship = RawTableRelationshipInfo(
            file_tag="my_table",
            foreign_table="my_table_2",
            cardinality=RawDataJoinCardinality.ONE_TO_ONE,
            join_clauses=[
                ColumnEqualityJoinBooleanClause(
                    column_1=JoinColumn(file_tag="my_table", column="my_col"),
                    column_2=JoinColumn(file_tag="my_table_2", column="my_col"),
                ),
                ColumnEqualityJoinBooleanClause(
                    column_1=JoinColumn(file_tag="my_table", column="my_col_b"),
                    column_2=JoinColumn(file_tag="my_table_2", column="my_col_b"),
                ),
            ],
            transforms=[],
        )

        relationship_reversed = RawTableRelationshipInfo(
            file_tag="my_table_2",
            foreign_table="my_table",
            cardinality=RawDataJoinCardinality.ONE_TO_ONE,
            join_clauses=[
                ColumnEqualityJoinBooleanClause(
                    column_1=JoinColumn(file_tag="my_table", column="my_col_b"),
                    column_2=JoinColumn(file_tag="my_table_2", column="my_col_b"),
                ),
                ColumnEqualityJoinBooleanClause(
                    column_1=JoinColumn(file_tag="my_table_2", column="my_col"),
                    column_2=JoinColumn(file_tag="my_table", column="my_col"),
                ),
            ],
            transforms=[],
        )
        self.assertEqual(relationship, relationship_reversed)

    def test_relationship_equality_many_to_one(self) -> None:
        # Cannot reverse the order of the tables a many-to-one or one-to-many
        # relationship without also flipping the cardinality.
        relationship = RawTableRelationshipInfo(
            file_tag="my_table",
            foreign_table="my_table_2",
            cardinality=RawDataJoinCardinality.MANY_TO_ONE,
            join_clauses=[
                ColumnEqualityJoinBooleanClause(
                    column_1=JoinColumn(file_tag="my_table", column="my_col"),
                    column_2=JoinColumn(file_tag="my_table_2", column="my_col"),
                ),
                ColumnEqualityJoinBooleanClause(
                    column_1=JoinColumn(file_tag="my_table", column="my_col_b"),
                    column_2=JoinColumn(file_tag="my_table_2", column="my_col_b"),
                ),
            ],
            transforms=[],
        )

        relationship_reversed = RawTableRelationshipInfo(
            file_tag="my_table_2",
            foreign_table="my_table",
            cardinality=RawDataJoinCardinality.MANY_TO_ONE,
            join_clauses=[
                ColumnEqualityJoinBooleanClause(
                    column_1=JoinColumn(file_tag="my_table", column="my_col_b"),
                    column_2=JoinColumn(file_tag="my_table_2", column="my_col_b"),
                ),
                ColumnEqualityJoinBooleanClause(
                    column_1=JoinColumn(file_tag="my_table_2", column="my_col"),
                    column_2=JoinColumn(file_tag="my_table", column="my_col"),
                ),
            ],
            transforms=[],
        )
        self.assertNotEqual(relationship, relationship_reversed)

        # Once we change the cardinality, equality applies
        relationship_reversed = attr.evolve(
            relationship_reversed, cardinality=RawDataJoinCardinality.ONE_TO_MANY
        )
        self.assertEqual(relationship, relationship_reversed)

    def test_joins_reference_other_table_fails(self) -> None:
        with self.assertRaisesRegex(
            ValueError,
            r"Found join clause \[my_table.my_col = my_table_2.my_col\] which involves "
            r"table\(s\) \[\{'my_table_2'\}\] which are not defined as part of the "
            r"relationship. Allowed tables: \('my_table',\).",
        ):
            RawTableRelationshipInfo(
                file_tag="my_table",
                foreign_table="my_table",
                cardinality=RawDataJoinCardinality.ONE_TO_MANY,
                join_clauses=[
                    ColumnEqualityJoinBooleanClause(
                        column_1=JoinColumn(file_tag="my_table", column="my_col"),
                        column_2=JoinColumn(file_tag="my_table_2", column="my_col"),
                    )
                ],
                transforms=[],
            )

    def test_joins_reference_other_table_fails_2(self) -> None:
        with self.assertRaisesRegex(
            ValueError,
            r"Found join clause \[my_table.my_col = my_table_3.my_col\] which involves "
            r"table\(s\) \[\{'my_table_3'\}\] which are not defined as part of the "
            r"relationship. Allowed tables: \('my_table', 'my_table_2'\).",
        ):
            RawTableRelationshipInfo(
                file_tag="my_table",
                foreign_table="my_table_2",
                cardinality=RawDataJoinCardinality.ONE_TO_MANY,
                join_clauses=[
                    ColumnEqualityJoinBooleanClause(
                        column_1=JoinColumn(file_tag="my_table", column="my_col"),
                        column_2=JoinColumn(file_tag="my_table_2", column="my_col"),
                    ),
                    # This join condition references a table that is not one of the
                    # tables in this relationship.
                    ColumnEqualityJoinBooleanClause(
                        column_1=JoinColumn(file_tag="my_table", column="my_col"),
                        column_2=JoinColumn(file_tag="my_table_3", column="my_col"),
                    ),
                ],
                transforms=[],
            )

    def test_no_column_equality_clause_fails(self) -> None:
        with self.assertRaisesRegex(
            ValueError,
            r"Found table relationship defined for table\(s\) "
            r"\('my_table', 'my_table_2'\) which does not have any join_logic "
            r"expressing equality between two different columns.",
        ):
            RawTableRelationshipInfo(
                file_tag="my_table",
                foreign_table="my_table_2",
                cardinality=RawDataJoinCardinality.MANY_TO_MANY,
                join_clauses=[
                    # Must have at least one ColumnEqualityJoinBooleanClause
                    ColumnFilterJoinBooleanClause(
                        column=JoinColumn(file_tag="my_table", column="my_col"),
                        filter_value='"SOME_VAL"',
                    ),
                ],
                transforms=[],
            )

    def test_transform_from_valid_columns(self) -> None:
        # Should lead to no errors
        col1 = JoinColumn(file_tag="my_table", column="my_col")
        col2 = JoinColumn(file_tag="my_table_2", column="my_col")
        _ = RawTableRelationshipInfo(
            file_tag="my_table",
            foreign_table="my_table_2",
            cardinality=RawDataJoinCardinality.ONE_TO_ONE,
            join_clauses=[
                ColumnEqualityJoinBooleanClause(
                    column_1=col1,
                    column_2=col2,
                ),
                ColumnEqualityJoinBooleanClause(
                    column_1=JoinColumn(file_tag="my_table", column="my_col_b"),
                    column_2=JoinColumn(file_tag="my_table_2", column="my_col_b"),
                ),
            ],
            transforms=[
                JoinTransform(col1, "REPLACE({col_name}, 'a', 'b')"),
                JoinTransform(col2, "CAST({col_name} AS INT64"),
            ],
        )

    def test_transform_from_invalid_column_fails(self) -> None:
        with self.assertRaisesRegex(
            ValueError,
            r"...",
        ):
            RawTableRelationshipInfo(
                file_tag="my_table",
                foreign_table="my_table_2",
                cardinality=RawDataJoinCardinality.MANY_TO_MANY,
                join_clauses=[
                    ColumnEqualityJoinBooleanClause(
                        column_1=JoinColumn(file_tag="my_table", column="my_col_b"),
                        column_2=JoinColumn(file_tag="my_table_2", column="my_col_b"),
                    ),
                ],
                transforms=[
                    JoinTransform(
                        JoinColumn(file_tag="my_table", column="column_not_in_table"),
                        "REPLACE({col_name}, 'a', 'b')",
                    )
                ],
            )
