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

from recidiviz.common.constants.state.state_person import StateGender
from recidiviz.persistence.entity.state.normalized_entities import (
    NormalizedStatePerson,
    NormalizedStatePersonExternalId,
)
from recidiviz.pipelines.metrics.utils import calculator_utils
from recidiviz.pipelines.metrics.utils.calculator_utils import (
    age_at_date,
    person_characteristics,
)
from recidiviz.pipelines.utils.state_utils.state_calculation_config_manager import (
    get_required_state_specific_metrics_producer_delegates,
)
from recidiviz.pipelines.utils.state_utils.state_specific_incarceration_metrics_producer_delegate import (
    StateSpecificIncarcerationMetricsProducerDelegate,
)
from recidiviz.pipelines.utils.state_utils.state_specific_metrics_producer_delegate import (
    StateSpecificMetricsProducerDelegate,
)
from recidiviz.pipelines.utils.state_utils.state_specific_supervision_metrics_producer_delegate import (
    StateSpecificSupervisionMetricsProducerDelegate,
)
from recidiviz.pipelines.utils.state_utils.templates.us_xx.us_xx_incarceration_metrics_producer_delegate import (
    UsXxIncarcerationMetricsProducerDelegate,
)
from recidiviz.pipelines.utils.state_utils.templates.us_xx.us_xx_supervision_metrics_producer_delegate import (
    UsXxSupervisionMetricsProducerDelegate,
)


class TestUsXxMetricsProducerDelegate(StateSpecificMetricsProducerDelegate):
    def primary_person_external_id_to_include(self) -> str:
        return "US_XX_DOC"


class UsXxIncarcerationMetricsProducerDelegateForTests(
    UsXxIncarcerationMetricsProducerDelegate
):
    def primary_person_external_id_to_include(self) -> str:
        return "US_XX_DOC"


class UsXxSupervisionMetricsProducerDelegateForTests(
    UsXxSupervisionMetricsProducerDelegate
):
    def primary_person_external_id_to_include(self) -> str:
        return "US_XX_SID"


class TestAgeAtDate(unittest.TestCase):
    """Tests the age_at_date function."""

    def test_age_at_date_earlier_month(self) -> None:
        birthdate = date(1989, 6, 17)
        check_date = date(2014, 4, 15)
        person = NormalizedStatePerson(
            state_code="US_XX", person_id=12345, birthdate=birthdate
        )

        self.assertEqual(calculator_utils.age_at_date(person, check_date), 24)

    def test_age_at_date_same_month_earlier_date(self) -> None:
        birthdate = date(1989, 6, 17)
        check_date = date(2014, 6, 16)
        person = NormalizedStatePerson(
            state_code="US_XX", person_id=12345, birthdate=birthdate
        )

        self.assertEqual(calculator_utils.age_at_date(person, check_date), 24)

    def test_age_at_date_same_month_same_date(self) -> None:
        birthdate = date(1989, 6, 17)
        check_date = date(2014, 6, 17)
        person = NormalizedStatePerson(
            state_code="US_XX", person_id=12345, birthdate=birthdate
        )

        self.assertEqual(calculator_utils.age_at_date(person, check_date), 25)

    def test_age_at_date_same_month_later_date(self) -> None:
        birthdate = date(1989, 6, 17)
        check_date = date(2014, 6, 18)
        person = NormalizedStatePerson(
            state_code="US_XX", person_id=12345, birthdate=birthdate
        )

        self.assertEqual(calculator_utils.age_at_date(person, check_date), 25)

    def test_age_at_date_later_month(self) -> None:
        birthdate = date(1989, 6, 17)
        check_date = date(2014, 7, 11)
        person = NormalizedStatePerson(
            state_code="US_XX", person_id=12345, birthdate=birthdate
        )

        self.assertEqual(calculator_utils.age_at_date(person, check_date), 25)

    def test_age_at_date_birthdate_unknown(self) -> None:
        self.assertIsNone(
            calculator_utils.age_at_date(
                NormalizedStatePerson(state_code="US_XX", person_id=12345),
                datetime.today(),
            )
        )


DELEGATE_CLASSES_TO_TEST = [
    StateSpecificIncarcerationMetricsProducerDelegate,
    StateSpecificSupervisionMetricsProducerDelegate,
]


class TestPersonExternalIdToInclude(unittest.TestCase):
    """Tests the person_external_id_to_include function."""

    def test_person_external_id_to_include(self) -> None:
        person = NormalizedStatePerson(
            state_code="US_MO",
            person_id=12345,
            birthdate=date(1984, 8, 31),
            gender=StateGender.FEMALE,
        )

        person_external_id = NormalizedStatePersonExternalId(
            person_external_id_id=12345,
            external_id="SID1341",
            id_type="US_MO_DOC",
            state_code="US_MO",
            is_current_display_id_for_type=True,
            is_stable_id_for_type=True,
            id_active_from_datetime=datetime(2020, 1, 1),
            id_active_to_datetime=None,
        )

        person.external_ids = [person_external_id]

        for metrics_producer_delegate_class in DELEGATE_CLASSES_TO_TEST:
            external_id = calculator_utils.person_external_id_to_include(
                person_external_id.state_code,
                person,
                metrics_producer_delegate=get_required_state_specific_metrics_producer_delegates(
                    "US_MO", {metrics_producer_delegate_class}
                ).get(
                    metrics_producer_delegate_class.__name__
                ),
            )

            self.assertEqual(external_id, person_external_id.external_id)

    def test_person_external_id_to_include_no_delegate(self) -> None:
        person = NormalizedStatePerson(
            state_code="US_XX",
            person_id=12345,
            birthdate=date(1984, 8, 31),
            gender=StateGender.FEMALE,
        )

        person_external_id = NormalizedStatePersonExternalId(
            external_id="SID10928",
            id_type="US_XX_SID",
            state_code="US_XX",
            person_external_id_id=12345,
            is_current_display_id_for_type=True,
            is_stable_id_for_type=True,
            id_active_from_datetime=datetime(2020, 1, 1),
            id_active_to_datetime=None,
        )

        person.external_ids = [person_external_id]

        external_id = calculator_utils.person_external_id_to_include(
            person_external_id.state_code,
            person,
        )
        self.assertIsNone(external_id)

    def test_person_has_external_ids_from_multiple_states(self) -> None:
        person = NormalizedStatePerson(
            state_code="US_XX",
            person_id=12345,
            birthdate=date(1984, 8, 31),
            gender=StateGender.FEMALE,
        )

        person_external_id_1 = NormalizedStatePersonExternalId(
            external_id="SID10928",
            id_type="US_ND_SID",
            state_code="US_ND",
            person_external_id_id=12345,
            is_current_display_id_for_type=True,
            is_stable_id_for_type=True,
            id_active_from_datetime=None,
            id_active_to_datetime=None,
        )

        person_external_id_2 = NormalizedStatePersonExternalId(
            external_id="SID1341",
            id_type="US_MO_DOC",
            state_code="US_MO",
            person_external_id_id=12345,
            is_current_display_id_for_type=True,
            is_stable_id_for_type=True,
            id_active_from_datetime=None,
            id_active_to_datetime=None,
        )

        person.external_ids = [person_external_id_1, person_external_id_2]

        with self.assertRaises(ValueError):
            _ = calculator_utils.person_external_id_to_include(
                person_external_id_2.state_code,
                person,
                metrics_producer_delegate=UsXxIncarcerationMetricsProducerDelegateForTests(),
            )

    def test_person_has_multiple_external_ids_of_the_same_type(self) -> None:
        person = NormalizedStatePerson(
            state_code="US_XX",
            person_id=12345,
            birthdate=date(1984, 8, 31),
            gender=StateGender.FEMALE,
        )

        # Wrong ID type
        person_external_id_exclude = NormalizedStatePersonExternalId(
            external_id="0123",
            id_type="US_PA_PBPP",
            state_code="US_PA",
            person_external_id_id=12345,
            is_current_display_id_for_type=True,
            is_stable_id_for_type=True,
            id_active_from_datetime=datetime(2020, 1, 1),
            id_active_to_datetime=None,
        )

        # Lowest value of the two 'US_PA_CONT' ids - pick this one
        person_external_id_include = NormalizedStatePersonExternalId(
            external_id="1234",
            id_type="US_PA_CONT",
            state_code="US_PA",
            person_external_id_id=12345,
            is_current_display_id_for_type=False,
            is_stable_id_for_type=False,
            id_active_from_datetime=datetime(2020, 1, 1),
            id_active_to_datetime=datetime(2021, 1, 1),
        )

        # Other 'US_PA_CONT' should be picked
        person_external_id_exclude_2 = NormalizedStatePersonExternalId(
            external_id="2345",
            id_type="US_PA_CONT",
            state_code="US_PA",
            person_external_id_id=12345,
            is_current_display_id_for_type=True,
            is_stable_id_for_type=True,
            id_active_from_datetime=datetime(2021, 1, 1),
            id_active_to_datetime=None,
        )

        person.external_ids = [
            person_external_id_exclude,
            person_external_id_include,
            person_external_id_exclude_2,
        ]

        delegate_class = StateSpecificIncarcerationMetricsProducerDelegate
        external_id = calculator_utils.person_external_id_to_include(
            person_external_id_include.state_code,
            person,
            metrics_producer_delegate=get_required_state_specific_metrics_producer_delegates(
                "US_PA", {delegate_class}
            ).get(
                delegate_class.__name__
            ),
        )

        self.assertEqual(external_id, person_external_id_include.external_id)


class TestAddPersonCharacteristics(unittest.TestCase):
    """Tests the add_person_characteristics function used by all pipelines."""

    def test_add_person_characteristics(self) -> None:
        person = NormalizedStatePerson(
            state_code="US_XX",
            person_id=12345,
            birthdate=date(1984, 8, 31),
            gender=StateGender.FEMALE,
        )

        event_date = date(2010, 9, 1)

        updated_characteristics = person_characteristics(
            person,
            age_at_date(person, event_date),
        )

        expected_output = {
            "person_id": person.person_id,
            "age": 26,
            "gender": StateGender.FEMALE,
        }

        self.assertEqual(updated_characteristics, expected_output)

    def test_add_person_characteristics_EmptyRaceEthnicity(self) -> None:
        person = NormalizedStatePerson(
            state_code="US_XX",
            person_id=12345,
            birthdate=date(1984, 8, 31),
            gender=StateGender.FEMALE,
        )

        event_date = date(2010, 9, 1)

        updated_characteristics = person_characteristics(
            person, age_at_date(person, event_date)
        )

        expected_output = {
            "age": 26,
            "gender": StateGender.FEMALE,
            "person_id": person.person_id,
        }

        self.assertEqual(updated_characteristics, expected_output)

    def test_add_person_characteristics_NoAttributes(self) -> None:
        person = NormalizedStatePerson(state_code="US_XX", person_id=12345)

        event_date = date(2010, 9, 1)

        updated_characteristics = person_characteristics(
            person, age_at_date(person, event_date)
        )

        expected_output = {"person_id": person.person_id}

        self.assertEqual(updated_characteristics, expected_output)

    def test_add_person_characteristics_include_external_id(self) -> None:
        person = NormalizedStatePerson(
            state_code="US_XX",
            person_id=12345,
            birthdate=date(1984, 8, 31),
            gender=StateGender.FEMALE,
            races=[],
            ethnicities=[],
            external_ids=[
                NormalizedStatePersonExternalId(
                    external_id="DOC1341",
                    id_type="US_XX_DOC",
                    state_code="US_XX",
                    person_external_id_id=12345,
                    is_current_display_id_for_type=True,
                    is_stable_id_for_type=True,
                    id_active_from_datetime=datetime(2020, 1, 1),
                    id_active_to_datetime=None,
                ),
                NormalizedStatePersonExternalId(
                    external_id="SID9889",
                    id_type="US_XX_SID",
                    state_code="US_XX",
                    person_external_id_id=12345,
                    is_current_display_id_for_type=True,
                    is_stable_id_for_type=True,
                    id_active_from_datetime=datetime(2020, 1, 1),
                    id_active_to_datetime=None,
                ),
            ],
        )

        event_date = date(2010, 9, 1)

        updated_characteristics = person_characteristics(
            person,
            age_at_date(person, event_date),
            metrics_producer_delegate=TestUsXxMetricsProducerDelegate(),
        )

        expected_output = {
            "age": 26,
            "gender": StateGender.FEMALE,
            "person_id": person.person_id,
            "person_external_id": "DOC1341",
        }

        self.assertEqual(updated_characteristics, expected_output)

    def test_add_person_characteristics_include_secondary_external_id(self) -> None:
        person = NormalizedStatePerson(
            state_code="US_XX",
            person_id=12345,
            birthdate=date(1984, 8, 31),
            gender=StateGender.FEMALE,
            races=[],
            ethnicities=[],
            external_ids=[
                NormalizedStatePersonExternalId(
                    external_id="DOC1341",
                    id_type="US_XX_DOC",
                    state_code="US_XX",
                    person_external_id_id=12345,
                    is_current_display_id_for_type=True,
                    is_stable_id_for_type=True,
                    id_active_from_datetime=datetime(2020, 1, 1),
                    id_active_to_datetime=None,
                ),
                NormalizedStatePersonExternalId(
                    external_id="SID9889",
                    id_type="US_XX_SID",
                    state_code="US_XX",
                    person_external_id_id=12345,
                    is_current_display_id_for_type=True,
                    is_stable_id_for_type=True,
                    id_active_from_datetime=datetime(2020, 1, 1),
                    id_active_to_datetime=None,
                ),
            ],
        )

        event_date = date(2010, 9, 1)

        updated_characteristics = person_characteristics(
            person,
            age_at_date(person, event_date),
            metrics_producer_delegate=UsXxIncarcerationMetricsProducerDelegateForTests(),
        )

        expected_output = {
            "age": 26,
            "gender": StateGender.FEMALE,
            "person_id": person.person_id,
            "person_external_id": "DOC1341",
        }

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
