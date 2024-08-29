# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2024 Recidiviz, Inc.
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
"""Tests for OutliersActionStrategyQualifier"""

import datetime
from unittest import TestCase

import freezegun

from recidiviz.outliers.outliers_action_strategy_qualifier import (
    OutliersActionStrategyQualifier,
)
from recidiviz.outliers.types import (
    ActionStrategySurfacedEvent,
    ActionStrategyType,
    PersonName,
    SupervisionOfficerEntity,
)

EVENT_OUTLIER = ActionStrategySurfacedEvent(
    state_code="US_PA",
    user_pseudonymized_id="user_pseudo_id",
    officer_pseudonymized_id="officer_hash",
    action_strategy=ActionStrategyType.ACTION_STRATEGY_OUTLIER.value,
    timestamp=datetime.datetime.now().date(),
)


class TestOutliersActionStrategyQualifier(TestCase):
    """Class for OutliersActionStrategyQualifier tests"""

    @freezegun.freeze_time(datetime.datetime(2023, 8, 24, 0, 0, 0, 0))
    def test_check_date_in_prior_month(self) -> None:
        qualifier = OutliersActionStrategyQualifier(events=[])
        result = qualifier.check_date_in_prior_month(
            timestamp=datetime.date(2023, 7, 30)
        )
        self.assertTrue(result)
        result = qualifier.check_date_in_prior_month(
            timestamp=datetime.date(2023, 8, 1)
        )
        self.assertFalse(result)

    def test_action_strategy_outlier_eligible_eligible(self) -> None:
        qualifier = OutliersActionStrategyQualifier(events=[])
        result = qualifier.action_strategy_outlier_eligible(
            officer_pseudo_id="officer_hash", is_outlier=True
        )
        self.assertTrue(result)

    def test_action_strategy_outlier_eligible_ineligible_event(self) -> None:
        qualifier = OutliersActionStrategyQualifier(events=[EVENT_OUTLIER])
        result = qualifier.action_strategy_outlier_eligible(
            officer_pseudo_id="officer_hash", is_outlier=True
        )
        self.assertFalse(result)

    def test_check_for_consecutive_3_months_true(self) -> None:
        qualifier = OutliersActionStrategyQualifier(events=[])
        outlier_metrics = [
            {
                "metric_id": "metric_one",
                "statuses_over_time": [
                    {
                        "end_date": "2023-05-01",
                        "metric_rate": 0.1,
                        "status": "FAR",
                    },
                    {
                        "end_date": "2023-04-01",
                        "metric_rate": 0.1,
                        "status": "FAR",
                    },
                    {
                        "end_date": "2023-03-01",
                        "metric_rate": 0.1,
                        "status": "FAR",
                    },
                    {
                        "end_date": "2023-01-01",
                        "metric_rate": 0.1,
                        "status": "FAR",
                    },
                ],
            }
        ]
        result = qualifier.check_for_consecutive_3_months(outlier_metrics)
        self.assertTrue(result)

    def test_check_for_consecutive_3_months_consecutive_after_first_status(
        self,
    ) -> None:
        qualifier = OutliersActionStrategyQualifier(events=[])
        outlier_metrics = [
            {
                "metric_id": "metric_one",
                "statuses_over_time": [
                    {
                        "end_date": "2023-05-01",
                        "metric_rate": 0.1,
                        "status": "FAR",
                    },
                    {
                        "end_date": "2023-03-01",
                        "metric_rate": 0.1,
                        "status": "FAR",
                    },
                    {
                        "end_date": "2023-02-01",
                        "metric_rate": 0.1,
                        "status": "FAR",
                    },
                    {
                        "end_date": "2023-01-01",
                        "metric_rate": 0.1,
                        "status": "FAR",
                    },
                ],
            }
        ]
        result = qualifier.check_for_consecutive_3_months(outlier_metrics)
        self.assertTrue(result)

    def test_check_for_consecutive_3_months_not_consecutive(self) -> None:
        qualifier = OutliersActionStrategyQualifier(events=[])
        outlier_metrics = [
            {
                "metric_id": "metric_one",
                "statuses_over_time": [
                    {
                        "end_date": "2023-04-01",
                        "metric_rate": 0.1,
                        "status": "FAR",
                    },
                    {
                        "end_date": "2023-03-01",
                        "metric_rate": 0.1,
                        "status": "FAR",
                    },
                    {
                        "end_date": "2023-01-01",
                        "metric_rate": 0.1,
                        "status": "FAR",
                    },
                ],
            }
        ]
        result = qualifier.check_for_consecutive_3_months(outlier_metrics)
        self.assertFalse(result)

    def test_check_for_consecutive_3_months_not_outlier(self) -> None:
        qualifier = OutliersActionStrategyQualifier(events=[])
        outlier_metrics = [
            {
                "metric_id": "metric_one",
                "statuses_over_time": [
                    {
                        "end_date": "2023-05-01",
                        "metric_rate": 0.1,
                        "status": "FAR",
                    },
                    {
                        "end_date": "2023-04-01",
                        "metric_rate": 0.1,
                        "status": "NEAR",
                    },
                    {
                        "end_date": "2023-03-01",
                        "metric_rate": 0.1,
                        "status": "FAR",
                    },
                ],
            }
        ]
        result = qualifier.check_for_consecutive_3_months(outlier_metrics)
        self.assertFalse(result)

    @freezegun.freeze_time(datetime.datetime(2023, 8, 24, 0, 0, 0, 0))
    def test_action_strategy_outlier_3_months_eligible_true(self) -> None:
        officer = SupervisionOfficerEntity(
            full_name=PersonName(**{"given_names": "DRACO", "surname": "MALFOY"}),
            external_id="789",
            pseudonymized_id="hash5",
            supervisor_external_id="102",
            supervisor_external_ids=["102"],
            district="Hogwarts",
            caseload_type=None,
            caseload_category="ALL",
            outlier_metrics=[
                {
                    "metric_id": "metric_one",
                    "statuses_over_time": [
                        {
                            "end_date": "2023-05-01",
                            "metric_rate": 0.1,
                            "status": "FAR",
                        },
                        {
                            "end_date": "2023-04-01",
                            "metric_rate": 0.1,
                            "status": "FAR",
                        },
                        {
                            "end_date": "2023-03-01",
                            "metric_rate": 0.1,
                            "status": "FAR",
                        },
                    ],
                }
            ],
            top_x_pct_metrics=[
                {
                    "metric_id": "incarceration_starts_and_inferred",
                    "top_x_pct": 10,
                }
            ],
            avg_daily_population=10.0,
        )
        pseudo_id = "supervisorHash"
        events = [
            ActionStrategySurfacedEvent(
                state_code="us_pa",
                user_pseudonymized_id=pseudo_id,
                officer_pseudonymized_id="hash5",
                action_strategy=ActionStrategyType.ACTION_STRATEGY_OUTLIER.value,
                timestamp=datetime.date(2023, 4, 1),
            )
        ]
        qualifier = OutliersActionStrategyQualifier(events=events)
        self.assertTrue(qualifier.action_strategy_outlier_3_months_eligible(officer))

    @freezegun.freeze_time(datetime.datetime(2023, 8, 24, 0, 0, 0, 0))
    def test_action_strategy_outlier_3_months_eligible_ineligible_disqualifying_event(
        self,
    ) -> None:
        officer = SupervisionOfficerEntity(
            full_name=PersonName(**{"given_names": "DRACO", "surname": "MALFOY"}),
            external_id="789",
            pseudonymized_id="hash5",
            supervisor_external_id="102",
            supervisor_external_ids=["102"],
            district="Hogwarts",
            caseload_type=None,
            caseload_category="ALL",
            outlier_metrics=[
                {
                    "metric_id": "metric_one",
                    "statuses_over_time": [
                        {
                            "end_date": "2023-05-01",
                            "metric_rate": 0.1,
                            "status": "FAR",
                        },
                        {
                            "end_date": "2023-04-01",
                            "metric_rate": 0.1,
                            "status": "FAR",
                        },
                        {
                            "end_date": "2023-03-01",
                            "metric_rate": 0.1,
                            "status": "FAR",
                        },
                    ],
                }
            ],
            top_x_pct_metrics=[
                {
                    "metric_id": "incarceration_starts_and_inferred",
                    "top_x_pct": 10,
                }
            ],
            avg_daily_population=10.0,
        )
        pseudo_id = "supervisorHash"
        events = [
            # Disqualifying due to the ActionStrategyType
            ActionStrategySurfacedEvent(
                state_code="us_pa",
                user_pseudonymized_id=pseudo_id,
                officer_pseudonymized_id="hash5",
                action_strategy=ActionStrategyType.ACTION_STRATEGY_OUTLIER_3_MONTHS.value,
                timestamp=datetime.date(2023, 4, 1),
            )
        ]
        qualifier = OutliersActionStrategyQualifier(events=events)
        self.assertFalse(qualifier.action_strategy_outlier_3_months_eligible(officer))

    @freezegun.freeze_time(datetime.datetime(2023, 8, 24, 0, 0, 0, 0))
    def test_action_strategy_outlier_3_months_eligible_ineligible_qualifying_event_same_month(
        self,
    ) -> None:
        officer = SupervisionOfficerEntity(
            full_name=PersonName(**{"given_names": "DRACO", "surname": "MALFOY"}),
            external_id="789",
            pseudonymized_id="hash5",
            supervisor_external_id="102",
            supervisor_external_ids=["102"],
            district="Hogwarts",
            caseload_type=None,
            caseload_category="ALL",
            outlier_metrics=[
                {
                    "metric_id": "metric_one",
                    "statuses_over_time": [
                        {
                            "end_date": "2023-05-01",
                            "metric_rate": 0.1,
                            "status": "FAR",
                        },
                        {
                            "end_date": "2023-04-01",
                            "metric_rate": 0.1,
                            "status": "FAR",
                        },
                        {
                            "end_date": "2023-03-01",
                            "metric_rate": 0.1,
                            "status": "FAR",
                        },
                    ],
                }
            ],
            top_x_pct_metrics=[
                {
                    "metric_id": "incarceration_starts_and_inferred",
                    "top_x_pct": 10,
                }
            ],
            avg_daily_population=10.0,
        )
        pseudo_id = "supervisorHash"
        events = [
            # Disqualifying due to the the timestamp in the same month as today
            ActionStrategySurfacedEvent(
                state_code="us_pa",
                user_pseudonymized_id=pseudo_id,
                officer_pseudonymized_id="hash5",
                action_strategy=ActionStrategyType.ACTION_STRATEGY_OUTLIER.value,
                timestamp=datetime.date(2023, 8, 1),
            )
        ]
        qualifier = OutliersActionStrategyQualifier(events=events)
        self.assertFalse(qualifier.action_strategy_outlier_3_months_eligible(officer))

    @freezegun.freeze_time(datetime.datetime(2023, 8, 24, 0, 0, 0, 0))
    def test_action_strategy_outlier_3_months_eligible_ineligible_qualifying_event_same_month_supervisor(
        self,
    ) -> None:
        officer = SupervisionOfficerEntity(
            full_name=PersonName(**{"given_names": "DRACO", "surname": "MALFOY"}),
            external_id="789",
            pseudonymized_id="hash5",
            supervisor_external_id="102",
            supervisor_external_ids=["102"],
            district="Hogwarts",
            caseload_type=None,
            caseload_category="ALL",
            outlier_metrics=[
                {
                    "metric_id": "metric_one",
                    "statuses_over_time": [
                        {
                            "end_date": "2023-05-01",
                            "metric_rate": 0.1,
                            "status": "FAR",
                        },
                        {
                            "end_date": "2023-04-01",
                            "metric_rate": 0.1,
                            "status": "FAR",
                        },
                        {
                            "end_date": "2023-03-01",
                            "metric_rate": 0.1,
                            "status": "FAR",
                        },
                    ],
                }
            ],
            top_x_pct_metrics=[
                {
                    "metric_id": "incarceration_starts_and_inferred",
                    "top_x_pct": 10,
                }
            ],
            avg_daily_population=10.0,
        )
        pseudo_id = "supervisorHash"
        events = [
            # Disqualifying due to the the timestamp in the same month as today
            ActionStrategySurfacedEvent(
                state_code="us_pa",
                user_pseudonymized_id=pseudo_id,
                officer_pseudonymized_id="hash5",
                action_strategy=ActionStrategyType.ACTION_STRATEGY_60_PERC_OUTLIERS.value,
                timestamp=datetime.date(2023, 8, 1),
            )
        ]
        qualifier = OutliersActionStrategyQualifier(events=events)
        self.assertFalse(qualifier.action_strategy_outlier_3_months_eligible(officer))

    def test_action_strategy_60_perc_outliers_eligible_eligible(
        self,
    ) -> None:
        # 3 of the 4 officers are outliers - meets eligibility criteria
        officers = [
            SupervisionOfficerEntity(
                full_name=PersonName(**{"given_names": "DRACO", "surname": "MALFOY"}),
                external_id="1",
                pseudonymized_id="hash1",
                supervisor_external_id="102",
                supervisor_external_ids=["102"],
                district="Hogwarts",
                caseload_type=None,
                caseload_category="ALL",
                outlier_metrics=[
                    {
                        "metric_id": "metric_one",
                        "statuses_over_time": [
                            {
                                "end_date": "2023-05-01",
                                "metric_rate": 0.1,
                                "status": "FAR",
                            },
                        ],
                    }
                ],
                top_x_pct_metrics=[
                    {
                        "metric_id": "incarceration_starts_and_inferred",
                        "top_x_pct": 10,
                    }
                ],
                avg_daily_population=10.0,
            ),
            SupervisionOfficerEntity(
                full_name=PersonName(**{"given_names": "HARRY", "surname": "POTTER"}),
                external_id="2",
                pseudonymized_id="hash2",
                supervisor_external_id="102",
                supervisor_external_ids=["102"],
                district="Hogwarts",
                caseload_type=None,
                caseload_category="ALL",
                outlier_metrics=[
                    {
                        "metric_id": "metric_one",
                        "statuses_over_time": [
                            {
                                "end_date": "2023-05-01",
                                "metric_rate": 0.1,
                                "status": "FAR",
                            },
                        ],
                    }
                ],
                top_x_pct_metrics=[
                    {
                        "metric_id": "incarceration_starts_and_inferred",
                        "top_x_pct": 10,
                    }
                ],
                avg_daily_population=10.0,
            ),
            SupervisionOfficerEntity(
                full_name=PersonName(
                    **{"given_names": "HERMOINE", "surname": "GRANGER"}
                ),
                external_id="3",
                pseudonymized_id="hash3",
                supervisor_external_id="102",
                supervisor_external_ids=["102"],
                district="Hogwarts",
                caseload_type=None,
                caseload_category="ALL",
                outlier_metrics=[
                    {
                        "metric_id": "metric_one",
                        "statuses_over_time": [
                            {
                                "end_date": "2023-05-01",
                                "metric_rate": 0.1,
                                "status": "FAR",
                            },
                        ],
                    }
                ],
                top_x_pct_metrics=[
                    {
                        "metric_id": "incarceration_starts_and_inferred",
                        "top_x_pct": 10,
                    }
                ],
                avg_daily_population=10.0,
            ),
            SupervisionOfficerEntity(
                full_name=PersonName(
                    **{"given_names": "RON", "surname": "WHATSHISNAME"}
                ),
                external_id="4",
                pseudonymized_id="hash4",
                supervisor_external_id="102",
                supervisor_external_ids=["102"],
                district="Hogwarts",
                caseload_type=None,
                caseload_category="ALL",
                outlier_metrics=[],
                top_x_pct_metrics=[
                    {
                        "metric_id": "incarceration_starts_and_inferred",
                        "top_x_pct": 10,
                    }
                ],
                avg_daily_population=10.0,
            ),
        ]
        events: list[ActionStrategySurfacedEvent] = []
        qualifier = OutliersActionStrategyQualifier(events=events)
        expected = ActionStrategyType.ACTION_STRATEGY_60_PERC_OUTLIERS.value
        self.assertEqual(
            expected, qualifier.get_eligible_action_strategy_for_supervisor(officers)
        )

    def test_action_strategy_60_perc_outliers_eligible_ineligible_number(
        self,
    ) -> None:
        # only 2 officers - does not meet eligibility criteria
        officers = [
            SupervisionOfficerEntity(
                full_name=PersonName(**{"given_names": "DRACO", "surname": "MALFOY"}),
                external_id="1",
                pseudonymized_id="hash1",
                supervisor_external_id="102",
                supervisor_external_ids=["102"],
                district="Hogwarts",
                caseload_type=None,
                caseload_category="ALL",
                outlier_metrics=[
                    {
                        "metric_id": "metric_one",
                        "statuses_over_time": [
                            {
                                "end_date": "2023-05-01",
                                "metric_rate": 0.1,
                                "status": "FAR",
                            },
                        ],
                    }
                ],
                top_x_pct_metrics=[
                    {
                        "metric_id": "incarceration_starts_and_inferred",
                        "top_x_pct": 10,
                    }
                ],
                avg_daily_population=10.0,
            ),
            SupervisionOfficerEntity(
                full_name=PersonName(**{"given_names": "HARRY", "surname": "POTTER"}),
                external_id="2",
                pseudonymized_id="hash2",
                supervisor_external_id="102",
                supervisor_external_ids=["102"],
                district="Hogwarts",
                caseload_type=None,
                caseload_category="ALL",
                outlier_metrics=[],
                top_x_pct_metrics=[
                    {
                        "metric_id": "incarceration_starts_and_inferred",
                        "top_x_pct": 10,
                    }
                ],
                avg_daily_population=10.0,
            ),
        ]
        events: list[ActionStrategySurfacedEvent] = []
        qualifier = OutliersActionStrategyQualifier(events=events)
        self.assertEqual(
            None, qualifier.get_eligible_action_strategy_for_supervisor(officers)
        )

    def test_action_strategy_60_perc_outliers_eligible_ineligible_percentage(
        self,
    ) -> None:
        # 1 of the 4 officers are outliers - does not meet eligibility criteria
        officers = [
            SupervisionOfficerEntity(
                full_name=PersonName(**{"given_names": "DRACO", "surname": "MALFOY"}),
                external_id="1",
                pseudonymized_id="hash1",
                supervisor_external_id="102",
                supervisor_external_ids=["102"],
                district="Hogwarts",
                caseload_type=None,
                caseload_category="ALL",
                outlier_metrics=[
                    {
                        "metric_id": "metric_one",
                        "statuses_over_time": [
                            {
                                "end_date": "2023-05-01",
                                "metric_rate": 0.1,
                                "status": "FAR",
                            },
                        ],
                    }
                ],
                top_x_pct_metrics=[
                    {
                        "metric_id": "incarceration_starts_and_inferred",
                        "top_x_pct": 10,
                    }
                ],
                avg_daily_population=10.0,
            ),
            SupervisionOfficerEntity(
                full_name=PersonName(**{"given_names": "HARRY", "surname": "POTTER"}),
                external_id="2",
                pseudonymized_id="hash2",
                supervisor_external_id="102",
                supervisor_external_ids=["102"],
                district="Hogwarts",
                caseload_type=None,
                caseload_category="ALL",
                outlier_metrics=[],
                top_x_pct_metrics=[
                    {
                        "metric_id": "incarceration_starts_and_inferred",
                        "top_x_pct": 10,
                    }
                ],
                avg_daily_population=10.0,
            ),
            SupervisionOfficerEntity(
                full_name=PersonName(
                    **{"given_names": "HERMOINE", "surname": "GRANGER"}
                ),
                external_id="3",
                pseudonymized_id="hash3",
                supervisor_external_id="102",
                supervisor_external_ids=["102"],
                district="Hogwarts",
                caseload_type=None,
                caseload_category="ALL",
                outlier_metrics=[],
                top_x_pct_metrics=[
                    {
                        "metric_id": "incarceration_starts_and_inferred",
                        "top_x_pct": 10,
                    }
                ],
                avg_daily_population=10.0,
            ),
            SupervisionOfficerEntity(
                full_name=PersonName(
                    **{"given_names": "RON", "surname": "WHATSHISNAME"}
                ),
                external_id="4",
                pseudonymized_id="hash4",
                supervisor_external_id="102",
                supervisor_external_ids=["102"],
                district="Hogwarts",
                caseload_type=None,
                caseload_category="ALL",
                outlier_metrics=[],
                top_x_pct_metrics=[
                    {
                        "metric_id": "incarceration_starts_and_inferred",
                        "top_x_pct": 10,
                    }
                ],
                avg_daily_population=10.0,
            ),
        ]
        events: list[ActionStrategySurfacedEvent] = []
        qualifier = OutliersActionStrategyQualifier(events=events)
        self.assertEqual(
            None, qualifier.get_eligible_action_strategy_for_supervisor(officers)
        )

    def test_action_strategy_60_perc_outliers_eligible_ineligible_event(
        self,
    ) -> None:
        # 3 of the 4 officers are outliers - meets eligibility criteria
        officers = [
            SupervisionOfficerEntity(
                full_name=PersonName(**{"given_names": "DRACO", "surname": "MALFOY"}),
                external_id="1",
                pseudonymized_id="hash1",
                supervisor_external_id="102",
                supervisor_external_ids=["102"],
                district="Hogwarts",
                caseload_type=None,
                caseload_category="ALL",
                outlier_metrics=[
                    {
                        "metric_id": "metric_one",
                        "statuses_over_time": [
                            {
                                "end_date": "2023-05-01",
                                "metric_rate": 0.1,
                                "status": "FAR",
                            },
                        ],
                    }
                ],
                top_x_pct_metrics=[
                    {
                        "metric_id": "incarceration_starts_and_inferred",
                        "top_x_pct": 10,
                    }
                ],
                avg_daily_population=10.0,
            ),
            SupervisionOfficerEntity(
                full_name=PersonName(**{"given_names": "HARRY", "surname": "POTTER"}),
                external_id="2",
                pseudonymized_id="hash2",
                supervisor_external_id="102",
                supervisor_external_ids=["102"],
                district="Hogwarts",
                caseload_type=None,
                caseload_category="ALL",
                outlier_metrics=[
                    {
                        "metric_id": "metric_one",
                        "statuses_over_time": [
                            {
                                "end_date": "2023-05-01",
                                "metric_rate": 0.1,
                                "status": "FAR",
                            },
                        ],
                    }
                ],
                top_x_pct_metrics=[
                    {
                        "metric_id": "incarceration_starts_and_inferred",
                        "top_x_pct": 10,
                    }
                ],
                avg_daily_population=10.0,
            ),
            SupervisionOfficerEntity(
                full_name=PersonName(
                    **{"given_names": "HERMOINE", "surname": "GRANGER"}
                ),
                external_id="3",
                pseudonymized_id="hash3",
                supervisor_external_id="102",
                supervisor_external_ids=["102"],
                district="Hogwarts",
                caseload_type=None,
                caseload_category="ALL",
                outlier_metrics=[
                    {
                        "metric_id": "metric_one",
                        "statuses_over_time": [
                            {
                                "end_date": "2023-05-01",
                                "metric_rate": 0.1,
                                "status": "FAR",
                            },
                        ],
                    }
                ],
                top_x_pct_metrics=[
                    {
                        "metric_id": "incarceration_starts_and_inferred",
                        "top_x_pct": 10,
                    }
                ],
                avg_daily_population=10.0,
            ),
            SupervisionOfficerEntity(
                full_name=PersonName(
                    **{"given_names": "RON", "surname": "WHATSHISNAME"}
                ),
                external_id="4",
                pseudonymized_id="hash4",
                supervisor_external_id="102",
                supervisor_external_ids=["102"],
                district="Hogwarts",
                caseload_type=None,
                caseload_category="ALL",
                outlier_metrics=[],
                top_x_pct_metrics=[
                    {
                        "metric_id": "incarceration_starts_and_inferred",
                        "top_x_pct": 10,
                    }
                ],
                avg_daily_population=10.0,
            ),
        ]
        # disqualifying event - does not meet eligiblity criteria
        events = [
            ActionStrategySurfacedEvent(
                state_code="us_pa",
                user_pseudonymized_id="any_hash",
                officer_pseudonymized_id=None,
                action_strategy=ActionStrategyType.ACTION_STRATEGY_60_PERC_OUTLIERS.value,
                timestamp=datetime.date(2023, 8, 1),
            )
        ]
        qualifier = OutliersActionStrategyQualifier(events=events)
        self.assertEqual(
            None, qualifier.get_eligible_action_strategy_for_supervisor(officers)
        )
