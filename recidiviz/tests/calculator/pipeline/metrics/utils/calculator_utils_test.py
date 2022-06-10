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
from typing import Optional

from recidiviz.calculator.pipeline.metrics.utils import calculator_utils
from recidiviz.calculator.pipeline.metrics.utils.calculator_utils import (
    person_characteristics,
)
from recidiviz.calculator.pipeline.metrics.utils.metric_utils import PersonMetadata
from recidiviz.calculator.pipeline.pipeline_type import (
    INCARCERATION_METRICS_PIPELINE_NAME,
    SUPERVISION_METRICS_PIPELINE_NAME,
)
from recidiviz.calculator.pipeline.utils.state_utils.state_calculation_config_manager import (
    get_required_state_specific_metrics_producer_delegate,
)
from recidiviz.calculator.pipeline.utils.state_utils.state_specific_incarceration_metrics_producer_delegate import (
    StateSpecificIncarcerationMetricsProducerDelegate,
)
from recidiviz.calculator.pipeline.utils.state_utils.state_specific_metrics_producer_delegate import (
    StateSpecificMetricsProducerDelegate,
)
from recidiviz.calculator.pipeline.utils.state_utils.state_specific_supervision_metrics_producer_delegate import (
    StateSpecificSupervisionMetricsProducerDelegate,
)
from recidiviz.calculator.pipeline.utils.state_utils.templates.us_xx.us_xx_incarceration_metrics_producer_delegate import (
    UsXxIncarcerationMetricsProducerDelegate,
)
from recidiviz.common.constants.state.state_person import StateGender
from recidiviz.persistence.entity.state.entities import (
    StatePerson,
    StatePersonEthnicity,
    StatePersonExternalId,
    StatePersonRace,
)


class TestAgeAtDate(unittest.TestCase):
    """Tests the age_at_date function."""

    def test_age_at_date_earlier_month(self) -> None:
        birthdate = date(1989, 6, 17)
        check_date = date(2014, 4, 15)
        person = StatePerson.new_with_defaults(state_code="US_XX", birthdate=birthdate)

        self.assertEqual(calculator_utils.age_at_date(person, check_date), 24)

    def test_age_at_date_same_month_earlier_date(self) -> None:
        birthdate = date(1989, 6, 17)
        check_date = date(2014, 6, 16)
        person = StatePerson.new_with_defaults(state_code="US_XX", birthdate=birthdate)

        self.assertEqual(calculator_utils.age_at_date(person, check_date), 24)

    def test_age_at_date_same_month_same_date(self) -> None:
        birthdate = date(1989, 6, 17)
        check_date = date(2014, 6, 17)
        person = StatePerson.new_with_defaults(state_code="US_XX", birthdate=birthdate)

        self.assertEqual(calculator_utils.age_at_date(person, check_date), 25)

    def test_age_at_date_same_month_later_date(self) -> None:
        birthdate = date(1989, 6, 17)
        check_date = date(2014, 6, 18)
        person = StatePerson.new_with_defaults(state_code="US_XX", birthdate=birthdate)

        self.assertEqual(calculator_utils.age_at_date(person, check_date), 25)

    def test_age_at_date_later_month(self) -> None:
        birthdate = date(1989, 6, 17)
        check_date = date(2014, 7, 11)
        person = StatePerson.new_with_defaults(state_code="US_XX", birthdate=birthdate)

        self.assertEqual(calculator_utils.age_at_date(person, check_date), 25)

    def test_age_at_date_birthdate_unknown(self) -> None:
        self.assertIsNone(
            calculator_utils.age_at_date(
                StatePerson.new_with_defaults(state_code="US_XX"), datetime.today()
            )
        )


INCLUDED_PIPELINES = {
    INCARCERATION_METRICS_PIPELINE_NAME: StateSpecificIncarcerationMetricsProducerDelegate,
    SUPERVISION_METRICS_PIPELINE_NAME: StateSpecificSupervisionMetricsProducerDelegate,
}


class TestPersonExternalIdToInclude(unittest.TestCase):
    """Tests the person_external_id_to_include function."""

    def test_person_external_id_to_include(self) -> None:
        person = StatePerson.new_with_defaults(
            state_code="US_MO",
            person_id=12345,
            birthdate=date(1984, 8, 31),
            gender=StateGender.FEMALE,
        )

        person_external_id = StatePersonExternalId.new_with_defaults(
            external_id="SID1341",
            id_type="US_MO_DOC",
            state_code="US_MO",
        )

        person.external_ids = [person_external_id]

        for _, metrics_producer_delegate_class in INCLUDED_PIPELINES.items():
            external_id = calculator_utils.person_external_id_to_include(
                person_external_id.state_code,
                person,
                metrics_producer_delegate=get_required_state_specific_metrics_producer_delegate(
                    "US_MO", metrics_producer_delegate_class
                ),
            )

            self.assertEqual(external_id, person_external_id.external_id)

    def test_person_external_id_to_include_no_results(self) -> None:
        person = StatePerson.new_with_defaults(
            state_code="US_XX",
            person_id=12345,
            birthdate=date(1984, 8, 31),
            gender=StateGender.FEMALE,
        )

        person_external_id = StatePersonExternalId.new_with_defaults(
            external_id="SID10928", id_type="US_XX_SID", state_code="US_XX"
        )

        person.external_ids = [person_external_id]

        for _, _ in INCLUDED_PIPELINES.items():
            external_id = calculator_utils.person_external_id_to_include(
                person_external_id.state_code,
                person,
            )
            self.assertIsNone(external_id)

    def test_person_has_external_ids_from_multiple_states(self) -> None:
        person = StatePerson.new_with_defaults(
            state_code="US_XX",
            person_id=12345,
            birthdate=date(1984, 8, 31),
            gender=StateGender.FEMALE,
        )

        person_external_id_1 = StatePersonExternalId.new_with_defaults(
            external_id="SID10928", id_type="US_ND_SID", state_code="US_ND"
        )

        person_external_id_2 = StatePersonExternalId.new_with_defaults(
            external_id="SID1341", id_type="US_MO_DOC", state_code="US_MO"
        )

        person.external_ids = [person_external_id_1, person_external_id_2]

        with self.assertRaises(ValueError):
            _ = calculator_utils.person_external_id_to_include(
                person_external_id_2.state_code,
                person,
                metrics_producer_delegate=UsXxIncarcerationMetricsProducerDelegate(),
            )

    def test_person_has_multiple_external_ids_of_the_same_type(self) -> None:
        person = StatePerson.new_with_defaults(
            state_code="US_XX",
            person_id=12345,
            birthdate=date(1984, 8, 31),
            gender=StateGender.FEMALE,
        )

        # Wrong ID type
        person_external_id_exclude = StatePersonExternalId.new_with_defaults(
            external_id="0123", id_type="US_PA_PBPP", state_code="US_PA"
        )

        # Lowest value of the two 'US_PA_CONT' ids - pick this one
        person_external_id_include = StatePersonExternalId.new_with_defaults(
            external_id="1234", id_type="US_PA_CONT", state_code="US_PA"
        )

        # Other 'US_PA_CONT' should be picked
        person_external_id_exclude_2 = StatePersonExternalId.new_with_defaults(
            external_id="2345", id_type="US_PA_CONT", state_code="US_PA"
        )

        person.external_ids = [
            person_external_id_exclude,
            person_external_id_include,
            person_external_id_exclude_2,
        ]

        external_id = calculator_utils.person_external_id_to_include(
            person_external_id_include.state_code,
            person,
            metrics_producer_delegate=get_required_state_specific_metrics_producer_delegate(
                "US_PA", INCLUDED_PIPELINES[INCARCERATION_METRICS_PIPELINE_NAME]
            ),
        )

        self.assertEqual(external_id, person_external_id_include.external_id)


class TestAddPersonCharacteristics(unittest.TestCase):
    """Tests the add_person_characteristics function used by all pipelines."""

    def test_add_person_characteristics(self) -> None:
        person = StatePerson.new_with_defaults(
            state_code="US_XX",
            person_id=12345,
            birthdate=date(1984, 8, 31),
            gender=StateGender.FEMALE,
        )

        event_date = date(2010, 9, 1)

        person_metadata = PersonMetadata(prioritized_race_or_ethnicity="ASIAN")

        updated_characteristics = person_characteristics(
            person,
            event_date,
            person_metadata,
        )

        expected_output = {
            "person_id": person.person_id,
            "age": 26,
            "prioritized_race_or_ethnicity": "ASIAN",
            "gender": StateGender.FEMALE,
        }

        self.assertEqual(updated_characteristics, expected_output)

    def test_add_person_characteristics_EmptyRaceEthnicity(self) -> None:
        person = StatePerson.new_with_defaults(
            state_code="US_XX",
            person_id=12345,
            birthdate=date(1984, 8, 31),
            gender=StateGender.FEMALE,
        )

        event_date = date(2010, 9, 1)

        person_metadata = PersonMetadata()

        updated_characteristics = person_characteristics(
            person, event_date, person_metadata
        )

        expected_output = {
            "age": 26,
            "gender": StateGender.FEMALE,
            "person_id": person.person_id,
        }

        self.assertEqual(updated_characteristics, expected_output)

    def test_add_person_characteristics_NoAttributes(self) -> None:
        person = StatePerson.new_with_defaults(state_code="US_XX", person_id=12345)

        event_date = date(2010, 9, 1)

        person_metadata = PersonMetadata()

        updated_characteristics = person_characteristics(
            person, event_date, person_metadata
        )

        expected_output = {"person_id": person.person_id}

        self.assertEqual(updated_characteristics, expected_output)

    class TestUsXxMetricsProducerDelegate(StateSpecificMetricsProducerDelegate):
        def primary_person_external_id_to_include(self) -> Optional[str]:
            return "US_XX_DOC"

    def test_add_person_characteristics_include_external_id(self) -> None:
        person = StatePerson.new_with_defaults(
            state_code="US_XX",
            person_id=12345,
            birthdate=date(1984, 8, 31),
            gender=StateGender.FEMALE,
            races=[
                StatePersonRace.new_with_defaults(
                    state_code="US_XX",
                )
            ],
            ethnicities=[
                StatePersonEthnicity.new_with_defaults(
                    state_code="US_XX",
                )
            ],
            external_ids=[
                StatePersonExternalId.new_with_defaults(
                    external_id="DOC1341", id_type="US_XX_DOC", state_code="US_XX"
                ),
                StatePersonExternalId.new_with_defaults(
                    external_id="SID9889", id_type="US_XX_SID", state_code="US_XX"
                ),
            ],
        )

        event_date = date(2010, 9, 1)

        person_metadata = PersonMetadata()

        updated_characteristics = person_characteristics(
            person,
            event_date,
            person_metadata,
            metrics_producer_delegate=self.TestUsXxMetricsProducerDelegate(),
        )

        expected_output = {
            "age": 26,
            "gender": StateGender.FEMALE,
            "person_id": person.person_id,
            "person_external_id": "DOC1341",
        }

        self.assertEqual(updated_characteristics, expected_output)

    def test_add_person_characteristics_include_secondary_external_id(self) -> None:
        person = StatePerson.new_with_defaults(
            state_code="US_XX",
            person_id=12345,
            birthdate=date(1984, 8, 31),
            gender=StateGender.FEMALE,
            races=[
                StatePersonRace.new_with_defaults(
                    state_code="US_XX",
                )
            ],
            ethnicities=[
                StatePersonEthnicity.new_with_defaults(
                    state_code="US_XX",
                )
            ],
            external_ids=[
                StatePersonExternalId.new_with_defaults(
                    external_id="DOC1341", id_type="US_XX_DOC", state_code="US_XX"
                ),
                StatePersonExternalId.new_with_defaults(
                    external_id="SID9889", id_type="US_XX_SID", state_code="US_XX"
                ),
            ],
        )

        event_date = date(2010, 9, 1)

        person_metadata = PersonMetadata()

        updated_characteristics = person_characteristics(
            person,
            event_date,
            person_metadata,
            metrics_producer_delegate=UsXxIncarcerationMetricsProducerDelegate(),
        )

        expected_output = {
            "age": 26,
            "gender": StateGender.FEMALE,
            "person_id": person.person_id,
            "person_external_id": "DOC1341",
            "secondary_person_external_id": "SID9889",
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


class TestGetCalculationMonthUpperBoundDate(unittest.TestCase):
    """Tests the get_calculation_month_upper_bound_date function."""

    def test_get_calculation_month_upper_bound_date(self) -> None:
        value = "2009-01"

        calculation_month_upper_bound = (
            calculator_utils.get_calculation_month_upper_bound_date(value)
        )

        self.assertEqual(date(2009, 1, 31), calculation_month_upper_bound)

    def test_get_calculation_month_upper_bound_date_bad_month(self) -> None:
        value = "2009-31"

        with self.assertRaisesRegex(
            ValueError, "Invalid value for calculation_end_month"
        ):
            _ = calculator_utils.get_calculation_month_upper_bound_date(value)

    def test_get_calculation_month_upper_bound_date_bad_year(self) -> None:
        value = "0001-31"

        with self.assertRaisesRegex(
            ValueError, "Invalid value for calculation_end_month"
        ):
            _ = calculator_utils.get_calculation_month_upper_bound_date(value)
