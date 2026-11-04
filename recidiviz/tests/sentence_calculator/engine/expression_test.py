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
"""Tests for recidiviz/sentence_calculator/engine/expression.py."""

import unittest
from datetime import date

from recidiviz.sentence_calculator.engine.expression import (
    evaluate_date_expression,
    evaluate_days_expression,
)
from recidiviz.sentence_calculator.models.tracked import TrackedValue


class TestEvaluateDaysExpression(unittest.TestCase):
    """Tests for evaluate_days_expression."""

    def test_multiplication_truncates_to_int(self) -> None:
        length = TrackedValue.input(1001, "S1.original_min_days")

        self.assertEqual(
            TrackedValue(
                value=580,
                label="S1.credit_cap",
                sources=(length,),
                rule_name="credit_cap",
                formula="sentence_length_days * 0.58",
            ),
            evaluate_days_expression(
                expression="sentence_length_days * 0.58",
                variables={"sentence_length_days": length},
                label="S1.credit_cap",
                rule_name="credit_cap",
            ),
        )

    def test_constant_expression_has_no_sources(self) -> None:
        self.assertEqual(
            TrackedValue(
                value=9125,
                label="S1.credit_cap",
                sources=tuple(),
                rule_name="twenty_five_year_cap",
                formula="25 * 365",
            ),
            evaluate_days_expression(
                expression="25 * 365",
                variables={},
                label="S1.credit_cap",
                rule_name="twenty_five_year_cap",
            ),
        )

    def test_parentheses_and_repeated_variable(self) -> None:
        length = TrackedValue.input(1000, "S1.original_max_days")

        self.assertEqual(
            TrackedValue(
                value=500,
                label="S1.threshold",
                # A variable used twice is listed once.
                sources=(length,),
                rule_name="threshold",
                formula="(sentence_length_days - sentence_length_days // 2) // 1",
            ),
            evaluate_days_expression(
                expression="(sentence_length_days - sentence_length_days // 2) // 1",
                variables={"sentence_length_days": length},
                label="S1.threshold",
                rule_name="threshold",
            ),
        )

    def test_undefined_variable(self) -> None:
        with self.assertRaisesRegex(
            ValueError,
            r"^Expression \[sentence_days \* 0\.58\] in rule \[credit_cap\] "
            r"references undefined variable \[sentence_days\]\. Available variables: "
            r"\['sentence_length_days'\]\.$",
        ):
            evaluate_days_expression(
                expression="sentence_days * 0.58",
                variables={
                    "sentence_length_days": TrackedValue.input(1000, "S1.length")
                },
                label="S1.credit_cap",
                rule_name="credit_cap",
            )

    def test_unsupported_operator(self) -> None:
        with self.assertRaisesRegex(
            ValueError,
            r"^Expression \[2 \*\* 10\] in rule \[credit_cap\] uses unsupported "
            r"operator \[Pow\]\.$",
        ):
            evaluate_days_expression(
                expression="2 ** 10",
                variables={},
                label="S1.credit_cap",
                rule_name="credit_cap",
            )

    def test_modulo_is_unsupported(self) -> None:
        with self.assertRaisesRegex(
            ValueError,
            r"^Expression \[sentence_length_days % 7\] in rule \[credit_cap\] uses "
            r"unsupported operator \[Mod\]\.$",
        ):
            evaluate_days_expression(
                expression="sentence_length_days % 7",
                variables={
                    "sentence_length_days": TrackedValue.input(100, "S1.length")
                },
                label="S1.credit_cap",
                rule_name="credit_cap",
            )

    def test_division_truncates_fractional_result(self) -> None:
        length = TrackedValue.input(7, "S1.original_min_days")

        self.assertEqual(
            TrackedValue(
                value=3,
                label="S1.threshold",
                sources=(length,),
                rule_name="threshold",
                formula="sentence_length_days / 2",
            ),
            evaluate_days_expression(
                expression="sentence_length_days / 2",
                variables={"sentence_length_days": length},
                label="S1.threshold",
                rule_name="threshold",
            ),
        )

    def test_unary_minus_negates_a_number(self) -> None:
        length = TrackedValue.input(100, "S1.original_min_days")

        self.assertEqual(
            TrackedValue(
                value=70,
                label="S1.threshold",
                sources=(length,),
                rule_name="threshold",
                formula="-30 + sentence_length_days",
            ),
            evaluate_days_expression(
                expression="-30 + sentence_length_days",
                variables={"sentence_length_days": length},
                label="S1.threshold",
                rule_name="threshold",
            ),
        )

    def test_duration_result_is_rejected(self) -> None:
        with self.assertRaisesRegex(
            ValueError,
            r"^Expression \[months\(6\)\] in rule \[credit_cap\] produced a "
            r"duration, but this rule must produce a number of days\.$",
        ):
            evaluate_days_expression(
                expression="months(6)",
                variables={},
                label="S1.credit_cap",
                rule_name="credit_cap",
            )

    def test_function_other_than_a_duration_is_unsupported(self) -> None:
        with self.assertRaisesRegex(
            ValueError,
            r"^Expression \[max\(1, 2\)\] in rule \[credit_cap\] calls \[max\], but "
            r"the only functions are \['years', 'months'\]\.$",
        ):
            evaluate_days_expression(
                expression="max(1, 2)",
                variables={},
                label="S1.credit_cap",
                rule_name="credit_cap",
            )

    def test_invalid_syntax(self) -> None:
        with self.assertRaisesRegex(
            ValueError,
            r"^Expression \[sentence_length_days \*\] in rule \[credit_cap\] is not "
            r"valid syntax\.$",
        ):
            evaluate_days_expression(
                expression="sentence_length_days *",
                variables={},
                label="S1.credit_cap",
                rule_name="credit_cap",
            )

    def test_date_result_is_rejected(self) -> None:
        with self.assertRaisesRegex(
            ValueError,
            r"^Expression \[start_date \+ 10\] in rule \[credit_cap\] produced the "
            r"date \[2025-01-11\], but this rule must produce a number of days\.$",
        ):
            evaluate_days_expression(
                expression="start_date + 10",
                variables={
                    "start_date": TrackedValue.input(date(2025, 1, 1), "start_date")
                },
                label="S1.credit_cap",
                rule_name="credit_cap",
            )

    def test_multiplication_is_exact_before_truncating(self) -> None:
        """100 * 0.58 is 57.99999999999999 in floating point; exact arithmetic
        keeps it 58."""
        self.assertEqual(
            58,
            evaluate_days_expression(
                expression="sentence_length_days * 0.58",
                variables={
                    "sentence_length_days": TrackedValue.input(100, "S1.length")
                },
                label="S1.credit_cap",
                rule_name="credit_cap",
            ).value,
        )
        self.assertEqual(
            29,
            evaluate_days_expression(
                expression="sentence_length_days * 0.58",
                variables={"sentence_length_days": TrackedValue.input(50, "S1.length")},
                label="S1.credit_cap",
                rule_name="credit_cap",
            ).value,
        )

    def test_division_of_floor_division_results_is_exact(self) -> None:
        """Each // returns a whole number, and dividing two of them in floating
        point would give 57.99999999999999 here instead of 58."""
        self.assertEqual(
            58,
            evaluate_days_expression(
                expression="(sentence_length_days // 1) / (2 // 2) * 0.58",
                variables={
                    "sentence_length_days": TrackedValue.input(100, "S1.length")
                },
                label="S1.credit_cap",
                rule_name="credit_cap",
            ).value,
        )

    def test_division_by_zero(self) -> None:
        with self.assertRaisesRegex(
            ValueError,
            r"^Expression \[sentence_length_days / 0\] in rule \[credit_cap\] "
            r"divides by zero\.$",
        ):
            evaluate_days_expression(
                expression="sentence_length_days / 0",
                variables={
                    "sentence_length_days": TrackedValue.input(100, "S1.length")
                },
                label="S1.credit_cap",
                rule_name="credit_cap",
            )

    def test_infinite_constant_is_rejected(self) -> None:
        with self.assertRaisesRegex(
            ValueError,
            r"^Expression \[1e999\] in rule \[credit_cap\] has constant \[inf\], "
            r"but expressions only support finite numbers\.$",
        ):
            evaluate_days_expression(
                expression="1e999",
                variables={},
                label="S1.credit_cap",
                rule_name="credit_cap",
            )

    def test_nan_variable_is_rejected(self) -> None:
        with self.assertRaisesRegex(
            ValueError,
            r"^Expression \[sentence_length_days \* 0\.58\] in rule \[credit_cap\] "
            r"reads variable \[sentence_length_days\] as \[nan\], but expressions "
            r"only support finite numbers\.$",
        ):
            evaluate_days_expression(
                expression="sentence_length_days * 0.58",
                variables={
                    "sentence_length_days": TrackedValue.input(
                        float("nan"), "S1.length"
                    )
                },
                label="S1.credit_cap",
                rule_name="credit_cap",
            )


class TestEvaluateDateExpression(unittest.TestCase):
    """Tests for evaluate_date_expression."""

    def test_date_plus_days(self) -> None:
        start_date = TrackedValue.input(date(2025, 1, 1), "start_date")
        adjusted_min_days = TrackedValue.input(365, "adjusted_min_days")

        self.assertEqual(
            TrackedValue(
                value=date(2026, 1, 1),
                label="parole_eligibility_date",
                sources=(start_date, adjusted_min_days),
                rule_name="parole_eligibility_date",
                formula="start_date + adjusted_min_days",
            ),
            evaluate_date_expression(
                expression="start_date + adjusted_min_days",
                variables={
                    "start_date": start_date,
                    "adjusted_min_days": adjusted_min_days,
                },
                label="parole_eligibility_date",
                rule_name="parole_eligibility_date",
            ),
        )

    def test_days_plus_date(self) -> None:
        self.assertEqual(
            date(2025, 1, 11),
            evaluate_date_expression(
                expression="10 + start_date",
                variables={
                    "start_date": TrackedValue.input(date(2025, 1, 1), "start_date")
                },
                label="release_date",
                rule_name="release_date",
            ).value,
        )

    def test_date_minus_fractional_days_truncates_days(self) -> None:
        self.assertEqual(
            date(2025, 6, 1),
            evaluate_date_expression(
                expression="projected_expiration_date - 365.9",
                variables={
                    "projected_expiration_date": TrackedValue.input(
                        date(2026, 6, 1), "projected_expiration_date"
                    )
                },
                label="mandatory_parole_release_date",
                rule_name="mandatory_parole_release_date",
            ).value,
        )

    def test_fractional_days_carry_until_the_date_is_final(self) -> None:
        """182.5 + 1.5 is 184 days. Truncating each before adding it to the date
        would give 182 + 1."""
        start_date = TrackedValue.input(date(2025, 1, 1), "start_date")
        adjusted_min_days = TrackedValue.input(365, "adjusted_min_days")
        jail_credit_days = TrackedValue.input(3, "jail_credit_days")

        self.assertEqual(
            TrackedValue(
                value=date(2025, 7, 4),
                label="parole_eligibility_date",
                sources=(start_date, adjusted_min_days, jail_credit_days),
                rule_name="parole_eligibility_date",
                formula=(
                    "start_date + adjusted_min_days * 0.5 + jail_credit_days * 0.5"
                ),
            ),
            evaluate_date_expression(
                expression=(
                    "start_date + adjusted_min_days * 0.5 + jail_credit_days * 0.5"
                ),
                variables={
                    "start_date": start_date,
                    "adjusted_min_days": adjusted_min_days,
                    "jail_credit_days": jail_credit_days,
                },
                label="parole_eligibility_date",
                rule_name="parole_eligibility_date",
            ),
        )

    def test_whole_days_move_the_date_before_a_duration(self) -> None:
        """The whole day moves 2024-01-30 to 2024-01-31, months(1) moves that to
        2024-02-29, and the two half days carried past months(1) move it to
        2024-03-01. Moving by months(1) first would give 2024-03-02, and
        truncating each half day would give 2024-02-29."""
        self.assertEqual(
            date(2024, 3, 1),
            evaluate_date_expression(
                expression="start_date + 1.5 + months(1) + 0.5",
                variables={
                    "start_date": TrackedValue.input(date(2024, 1, 30), "start_date")
                },
                label="release_date",
                rule_name="release_date",
            ).value,
        )

    def test_date_minus_date_is_rejected(self) -> None:
        with self.assertRaisesRegex(
            ValueError,
            r"^Expression \[end_date - start_date\] in rule \[release_date\] applies "
            r"\[Sub\] to \[2026-01-01\] and \[2025-01-01\]\. Dates only support "
            r"adding or "
            r"subtracting a number of days\.$",
        ):
            evaluate_date_expression(
                expression="end_date - start_date",
                variables={
                    "start_date": TrackedValue.input(date(2025, 1, 1), "start_date"),
                    "end_date": TrackedValue.input(date(2026, 1, 1), "end_date"),
                },
                label="release_date",
                rule_name="release_date",
            )

    def test_number_result_is_rejected(self) -> None:
        with self.assertRaisesRegex(
            ValueError,
            r"^Expression \[365 \* 2\] in rule \[release_date\] produced the number "
            r"\[730\], but this rule must produce a date\.$",
        ):
            evaluate_date_expression(
                expression="365 * 2",
                variables={},
                label="release_date",
                rule_name="release_date",
            )

    def test_date_minus_months_keeps_the_calendar_day(self) -> None:
        """12 months before a leap day is the last day of February, where 365 days
        before it would be March 1."""
        projected_expiration_date = TrackedValue.input(
            date(2028, 2, 29), "projected_expiration_date"
        )

        self.assertEqual(
            TrackedValue(
                value=date(2027, 2, 28),
                label="mandatory_parole_release_date",
                sources=(projected_expiration_date,),
                rule_name="mandatory_parole_release_date",
                formula="projected_expiration_date - months(12)",
            ),
            evaluate_date_expression(
                expression="projected_expiration_date - months(12)",
                variables={"projected_expiration_date": projected_expiration_date},
                label="mandatory_parole_release_date",
                rule_name="mandatory_parole_release_date",
            ),
        )

    def test_years_plus_date(self) -> None:
        self.assertEqual(
            date(2027, 1, 1),
            evaluate_date_expression(
                expression="years(3) + start_date",
                variables={
                    "start_date": TrackedValue.input(date(2024, 1, 1), "start_date")
                },
                label="release_date",
                rule_name="release_date",
            ).value,
        )

    def test_duration_with_non_whole_number_is_rejected(self) -> None:
        with self.assertRaisesRegex(
            ValueError,
            r"^Expression \[start_date \+ months\(1\.5\)\] in rule \[release_date\] "
            r"calls \[months\(1\.5\)\], but months\(\) takes one whole number, e\.g\. "
            r"months\(12\)\.$",
        ):
            evaluate_date_expression(
                expression="start_date + months(1.5)",
                variables={
                    "start_date": TrackedValue.input(date(2024, 1, 1), "start_date")
                },
                label="release_date",
                rule_name="release_date",
            )

    def test_duration_used_with_a_number_is_rejected(self) -> None:
        with self.assertRaisesRegex(
            ValueError,
            r"^Expression \[start_date - 2 \* months\(1\)\] in rule \[release_date\] "
            r"applies \[Mult\] to a duration\. A duration can only be added to or "
            r"subtracted from a date\.$",
        ):
            evaluate_date_expression(
                expression="start_date - 2 * months(1)",
                variables={
                    "start_date": TrackedValue.input(date(2024, 1, 1), "start_date")
                },
                label="release_date",
                rule_name="release_date",
            )

    def test_negated_date_is_rejected(self) -> None:
        with self.assertRaisesRegex(
            ValueError,
            r"^Expression \[-start_date \+ 10\] in rule \[release_date\] negates a "
            r"date\.$",
        ):
            evaluate_date_expression(
                expression="-start_date + 10",
                variables={
                    "start_date": TrackedValue.input(date(2024, 1, 1), "start_date")
                },
                label="release_date",
                rule_name="release_date",
            )

    def test_negated_duration_is_rejected(self) -> None:
        with self.assertRaisesRegex(
            ValueError,
            r"^Expression \[start_date \+ -months\(1\)\] in rule \[release_date\] "
            r"negates a duration\. Subtract it from a date instead\.$",
        ):
            evaluate_date_expression(
                expression="start_date + -months(1)",
                variables={
                    "start_date": TrackedValue.input(date(2024, 1, 1), "start_date")
                },
                label="release_date",
                rule_name="release_date",
            )

    def test_duration_result_is_rejected(self) -> None:
        with self.assertRaisesRegex(
            ValueError,
            r"^Expression \[years\(1\)\] in rule \[release_date\] produced a "
            r"duration, but this rule must produce a date\.$",
        ):
            evaluate_date_expression(
                expression="years(1)",
                variables={},
                label="release_date",
                rule_name="release_date",
            )

    def test_days_past_last_supported_date_are_rejected(self) -> None:
        with self.assertRaisesRegex(
            ValueError,
            r"^Expression \[start_date \+ adjusted_max_days\] in rule "
            r"\[release_date\] moves the date \[2024-01-01 \+ 4000000 days\] outside "
            r"the range dates support, \[0001-01-01\] to \[9999-12-31\]\.$",
        ):
            evaluate_date_expression(
                expression="start_date + adjusted_max_days",
                variables={
                    "start_date": TrackedValue.input(date(2024, 1, 1), "start_date"),
                    "adjusted_max_days": TrackedValue.input(
                        4_000_000, "adjusted_max_days"
                    ),
                },
                label="release_date",
                rule_name="release_date",
            )

    def test_duration_past_last_supported_date_is_rejected(self) -> None:
        with self.assertRaisesRegex(
            ValueError,
            r"^Expression \[start_date \+ years\(8000\)\] in rule \[release_date\] "
            r"moves the date \[2024-01-01\] outside the range dates support, "
            r"\[0001-01-01\] to \[9999-12-31\]\.$",
        ):
            evaluate_date_expression(
                expression="start_date + years(8000)",
                variables={
                    "start_date": TrackedValue.input(date(2024, 1, 1), "start_date")
                },
                label="release_date",
                rule_name="release_date",
            )


if __name__ == "__main__":
    unittest.main()
