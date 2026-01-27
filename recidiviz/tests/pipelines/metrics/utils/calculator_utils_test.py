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
"""Tests for calculator_utils.py."""
import unittest
from datetime import date, datetime

from freezegun import freeze_time

from recidiviz.common.constants.state.state_person import StateEthnicity, StateGender
from recidiviz.persistence.entity.state.normalized_entities import NormalizedStatePerson
from recidiviz.pipelines.metrics.utils import calculator_utils
from recidiviz.pipelines.metrics.utils.calculator_utils import (
    age_at_date,
    person_characteristics,
)


class TestAgeAtDate(unittest.TestCase):
    """Tests the age_at_date function."""

    def test_age_at_date_earlier_month(self) -> None:
        birthdate = date(1989, 6, 17)
        check_date = date(2014, 4, 15)
        person = NormalizedStatePerson(
            state_code="US_XX",
            person_id=12345,
            birthdate=birthdate,
            ethnicity=StateEthnicity.PRESENT_WITHOUT_INFO,
        )

        self.assertEqual(calculator_utils.age_at_date(person, check_date), 24)

    def test_age_at_date_same_month_earlier_date(self) -> None:
        birthdate = date(1989, 6, 17)
        check_date = date(2014, 6, 16)
        person = NormalizedStatePerson(
            state_code="US_XX",
            person_id=12345,
            birthdate=birthdate,
            ethnicity=StateEthnicity.PRESENT_WITHOUT_INFO,
        )

        self.assertEqual(calculator_utils.age_at_date(person, check_date), 24)

    def test_age_at_date_same_month_same_date(self) -> None:
        birthdate = date(1989, 6, 17)
        check_date = date(2014, 6, 17)
        person = NormalizedStatePerson(
            state_code="US_XX",
            person_id=12345,
            birthdate=birthdate,
            ethnicity=StateEthnicity.PRESENT_WITHOUT_INFO,
        )

        self.assertEqual(calculator_utils.age_at_date(person, check_date), 25)

    def test_age_at_date_same_month_later_date(self) -> None:
        birthdate = date(1989, 6, 17)
        check_date = date(2014, 6, 18)
        person = NormalizedStatePerson(
            state_code="US_XX",
            person_id=12345,
            birthdate=birthdate,
            ethnicity=StateEthnicity.PRESENT_WITHOUT_INFO,
        )

        self.assertEqual(calculator_utils.age_at_date(person, check_date), 25)

    def test_age_at_date_later_month(self) -> None:
        birthdate = date(1989, 6, 17)
        check_date = date(2014, 7, 11)
        person = NormalizedStatePerson(
            state_code="US_XX",
            person_id=12345,
            birthdate=birthdate,
            ethnicity=StateEthnicity.PRESENT_WITHOUT_INFO,
        )

        self.assertEqual(calculator_utils.age_at_date(person, check_date), 25)

    def test_age_at_date_birthdate_unknown(self) -> None:
        self.assertIsNone(
            calculator_utils.age_at_date(
                NormalizedStatePerson(
                    state_code="US_XX",
                    person_id=12345,
                    ethnicity=StateEthnicity.PRESENT_WITHOUT_INFO,
                ),
                datetime.today(),
            )
        )


class TestAddPersonCharacteristics(unittest.TestCase):
    """Tests the add_person_characteristics function used by all pipelines."""

    def test_add_person_characteristics(self) -> None:
        person = NormalizedStatePerson(
            state_code="US_XX",
            person_id=12345,
            birthdate=date(1984, 8, 31),
            gender=StateGender.FEMALE,
            ethnicity=StateEthnicity.PRESENT_WITHOUT_INFO,
        )

        event_date = date(2010, 9, 1)

        updated_characteristics = person_characteristics(
            person,
            age_at_date(person, event_date),
        )

        expected_output = {
            "person_id": person.person_id,
            "age": 26,
        }

        self.assertEqual(updated_characteristics, expected_output)

    def test_add_person_characteristics_EmptyRaceEthnicity(self) -> None:
        person = NormalizedStatePerson(
            state_code="US_XX",
            person_id=12345,
            birthdate=date(1984, 8, 31),
            gender=StateGender.FEMALE,
            ethnicity=StateEthnicity.PRESENT_WITHOUT_INFO,
        )

        event_date = date(2010, 9, 1)

        updated_characteristics = person_characteristics(
            person, age_at_date(person, event_date)
        )

        expected_output = {
            "age": 26,
            "person_id": person.person_id,
        }

        self.assertEqual(updated_characteristics, expected_output)

    def test_add_person_characteristics_NoAttributes(self) -> None:
        person = NormalizedStatePerson(
            state_code="US_XX",
            person_id=12345,
            ethnicity=StateEthnicity.PRESENT_WITHOUT_INFO,
        )

        event_date = date(2010, 9, 1)

        updated_characteristics = person_characteristics(
            person, age_at_date(person, event_date)
        )

        expected_output = {"person_id": person.person_id}

        self.assertEqual(updated_characteristics, expected_output)


class TestIncludeInMonthlyMetrics(unittest.TestCase):
    """Tests the include_in_monthly_metrics function."""

    def test_include_in_monthly_metrics(self) -> None:
        calculation_month_upper_bound = date(2000, 1, 31)

        include = calculator_utils.include_in_output(
            year=1999,
            month=11,
            calculation_month_upper_bound=calculation_month_upper_bound,
            calculation_month_lower_bound=None,
        )

        self.assertTrue(include)

    def test_include_in_monthly_metrics_lower_bound(self) -> None:
        calculation_month_upper_bound = date(2000, 1, 31)
        calculation_month_lower_bound = date(1999, 10, 1)

        include = calculator_utils.include_in_output(
            year=1999,
            month=11,
            calculation_month_upper_bound=calculation_month_upper_bound,
            calculation_month_lower_bound=calculation_month_lower_bound,
        )

        self.assertTrue(include)

    def test_include_in_monthly_metrics_month_of_lower_bound(self) -> None:
        calculation_month_upper_bound = date(2000, 1, 31)
        calculation_month_lower_bound = date(1999, 10, 1)

        include = calculator_utils.include_in_output(
            year=calculation_month_lower_bound.year,
            month=calculation_month_lower_bound.month,
            calculation_month_upper_bound=calculation_month_upper_bound,
            calculation_month_lower_bound=calculation_month_lower_bound,
        )

        self.assertTrue(include)

    def test_include_in_monthly_metrics_month_of_end_date(self) -> None:
        calculation_month_upper_bound = date(2000, 1, 31)
        calculation_month_lower_bound = date(1999, 10, 1)

        include = calculator_utils.include_in_output(
            year=calculation_month_upper_bound.year,
            month=calculation_month_upper_bound.month,
            calculation_month_upper_bound=calculation_month_upper_bound,
            calculation_month_lower_bound=calculation_month_lower_bound,
        )

        self.assertTrue(include)

    def test_include_in_monthly_metrics_after_end_date(self) -> None:
        calculation_month_upper_bound = date(2000, 1, 31)

        include = calculator_utils.include_in_output(
            year=2000,
            month=2,
            calculation_month_upper_bound=calculation_month_upper_bound,
            calculation_month_lower_bound=None,
        )

        self.assertFalse(include)

    def test_include_in_monthly_metrics_before_lower_bound(self) -> None:
        calculation_month_upper_bound = date(2000, 1, 31)
        calculation_month_lower_bound = date(1999, 10, 1)

        include = calculator_utils.include_in_output(
            year=1990,
            month=4,
            calculation_month_upper_bound=calculation_month_upper_bound,
            calculation_month_lower_bound=calculation_month_lower_bound,
        )

        self.assertFalse(include)

    def test_include_in_monthly_metrics_one_month_run(self) -> None:
        calculation_month_upper_bound = date(1999, 12, 31)
        calculation_month_lower_bound = date(1999, 12, 1)

        include = calculator_utils.include_in_output(
            year=1999,
            month=12,
            calculation_month_upper_bound=calculation_month_upper_bound,
            calculation_month_lower_bound=calculation_month_lower_bound,
        )

        self.assertTrue(include)

    def test_include_in_monthly_metrics_one_month_run_exclude(self) -> None:
        calculation_month_upper_bound = date(1999, 12, 31)
        calculation_month_lower_bound = date(1999, 12, 1)

        include = calculator_utils.include_in_output(
            year=2000,
            month=1,
            calculation_month_upper_bound=calculation_month_upper_bound,
            calculation_month_lower_bound=calculation_month_lower_bound,
        )

        self.assertFalse(include)


@freeze_time("2009-01-03 00:00:00-05:00")
class TestGetCalculationMonthUpperBoundDate(unittest.TestCase):
    """Tests the get_calculation_month_upper_bound_date function."""

    def test_get_calculation_month_upper_bound_date(self) -> None:
        self.assertEqual(
            date(2009, 1, 31), calculator_utils.get_calculation_month_upper_bound_date()
        )
