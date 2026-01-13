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

from recidiviz.common.constants.states import StateCode
from recidiviz.outliers.constants import (
    INCARCERATION_STARTS_AND_INFERRED_TECHNICAL_VIOLATION,
)
from recidiviz.outliers.outliers_action_strategy_qualifier import (
    OutliersActionStrategyQualifier,
)
from recidiviz.outliers.outliers_configs import get_outliers_backend_config
from recidiviz.outliers.types import (
    ActionStrategySurfacedEvent,
    ActionStrategyType,
    OutliersBackendConfig,
    OutliersMetricConfig,
    PersonName,
    SupervisionOfficerEntity,
    SupervisionOfficerOutcomes,
)

EVENT_OUTLIER = ActionStrategySurfacedEvent(
    state_code="US_PA",
    user_pseudonymized_id="user_pseudo_id",
    officer_pseudonymized_id="officer_hash",
    action_strategy=ActionStrategyType.ACTION_STRATEGY_OUTLIER.value,
    timestamp=datetime.datetime.now().date(),
)
METRIC_CONFIG = get_outliers_backend_config("US_PA")


class TestOutliersActionStrategyQualifier(TestCase):
    """Class for OutliersActionStrategyQualifier tests"""

    @freezegun.freeze_time(datetime.datetime(2023, 8, 24, 0, 0, 0, 0))
    def test_date_is_period_prior_than_current(self) -> None:
        qualifier = OutliersActionStrategyQualifier(events=[], config=METRIC_CONFIG)
        result = qualifier.date_is_period_prior_than_current(
            timestamp=datetime.date(2023, 7, 30)
        )
        self.assertTrue(result)
        result = qualifier.date_is_period_prior_than_current(
            timestamp=datetime.date(2023, 8, 1)
        )
        self.assertFalse(result)

    @freezegun.freeze_time(datetime.datetime(2023, 8, 24, 0, 0, 0, 0))
    def test_date_is_within_current_or_prior_period(self) -> None:
        qualifier = OutliersActionStrategyQualifier(events=[], config=METRIC_CONFIG)
        result = qualifier.date_is_within_current_or_prior_period(
            timestamp=datetime.date(2023, 6, 30)
        )
        self.assertFalse(result)
        result = qualifier.date_is_within_current_or_prior_period(
            timestamp=datetime.date(2023, 7, 1)
        )
        self.assertTrue(result)

    def test_action_strategy_outlier_eligible_eligible(self) -> None:
        qualifier = OutliersActionStrategyQualifier(events=[], config=METRIC_CONFIG)
        result = qualifier.action_strategy_outlier_eligible(
            officer_pseudo_id="officer_hash", is_outlier=True
        )
        self.assertTrue(result)

    def test_action_strategy_outlier_eligible_ineligible_event(self) -> None:
        qualifier = OutliersActionStrategyQualifier(
            events=[EVENT_OUTLIER], config=METRIC_CONFIG
        )
        result = qualifier.action_strategy_outlier_eligible(
            officer_pseudo_id="officer_hash", is_outlier=True
        )
        self.assertFalse(result)

    def test_check_for_consecutive_3_months_true(self) -> None:
        qualifier = OutliersActionStrategyQualifier(events=[], config=METRIC_CONFIG)
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
        qualifier = OutliersActionStrategyQualifier(events=[], config=METRIC_CONFIG)
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
        qualifier = OutliersActionStrategyQualifier(events=[], config=METRIC_CONFIG)
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
        qualifier = OutliersActionStrategyQualifier(events=[], config=METRIC_CONFIG)
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

        outcomes = SupervisionOfficerOutcomes(
            external_id="789",
            pseudonymized_id="hash5",
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
        qualifier = OutliersActionStrategyQualifier(events=events, config=METRIC_CONFIG)
        self.assertTrue(qualifier.action_strategy_outlier_3_months_eligible(outcomes))

    @freezegun.freeze_time(datetime.datetime(2023, 8, 24, 0, 0, 0, 0))
    def test_action_strategy_outlier_3_months_eligible_ineligible_disqualifying_event(
        self,
    ) -> None:

        outcomes = SupervisionOfficerOutcomes(
            external_id="789",
            pseudonymized_id="hash5",
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
        qualifier = OutliersActionStrategyQualifier(events=events, config=METRIC_CONFIG)
        self.assertFalse(qualifier.action_strategy_outlier_3_months_eligible(outcomes))

    @freezegun.freeze_time(datetime.datetime(2023, 8, 24, 0, 0, 0, 0))
    def test_action_strategy_outlier_3_months_eligible_ineligible_qualifying_event_same_month(
        self,
    ) -> None:
        outcomes = SupervisionOfficerOutcomes(
            external_id="789",
            pseudonymized_id="hash5",
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
        qualifier = OutliersActionStrategyQualifier(events=events, config=METRIC_CONFIG)
        self.assertFalse(qualifier.action_strategy_outlier_3_months_eligible(outcomes))

    @freezegun.freeze_time(datetime.datetime(2023, 8, 24, 0, 0, 0, 0))
    def test_action_strategy_outlier_3_months_eligible_ineligible_qualifying_event_same_month_supervisor(
        self,
    ) -> None:
        outcomes = SupervisionOfficerOutcomes(
            external_id="789",
            pseudonymized_id="hash5",
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
        qualifier = OutliersActionStrategyQualifier(events=events, config=METRIC_CONFIG)
        self.assertFalse(qualifier.action_strategy_outlier_3_months_eligible(outcomes))

    def test_action_strategy_60_perc_outliers_eligible_eligible(
        self,
    ) -> None:
        # 3 of the 4 officers are outliers - meets eligibility criteria
        outcomes_list = [
            SupervisionOfficerOutcomes(
                external_id="1",
                pseudonymized_id="hash1",
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
            ),
            SupervisionOfficerOutcomes(
                external_id="2",
                pseudonymized_id="hash2",
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
            ),
            SupervisionOfficerOutcomes(
                external_id="3",
                pseudonymized_id="hash3",
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
            ),
            SupervisionOfficerOutcomes(
                external_id="4",
                pseudonymized_id="hash4",
                caseload_category="ALL",
                outlier_metrics=[],
                top_x_pct_metrics=[
                    {
                        "metric_id": "incarceration_starts_and_inferred",
                        "top_x_pct": 10,
                    }
                ],
            ),
        ]
        events: list[ActionStrategySurfacedEvent] = []
        qualifier = OutliersActionStrategyQualifier(events=events, config=METRIC_CONFIG)
        expected = ActionStrategyType.ACTION_STRATEGY_60_PERC_OUTLIERS.value
        self.assertEqual(
            expected,
            qualifier.get_eligible_action_strategy_for_supervisor(outcomes_list),
        )

    def test_action_strategy_60_perc_outliers_eligible_ineligible_number(
        self,
    ) -> None:
        # only 2 officers - does not meet eligibility criteria
        outcomes_list = [
            SupervisionOfficerOutcomes(
                external_id="1",
                pseudonymized_id="hash1",
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
            ),
            SupervisionOfficerOutcomes(
                external_id="2",
                pseudonymized_id="hash2",
                caseload_category="ALL",
                outlier_metrics=[],
                top_x_pct_metrics=[
                    {
                        "metric_id": "incarceration_starts_and_inferred",
                        "top_x_pct": 10,
                    }
                ],
            ),
        ]
        events: list[ActionStrategySurfacedEvent] = []
        qualifier = OutliersActionStrategyQualifier(events=events, config=METRIC_CONFIG)
        self.assertEqual(
            None, qualifier.get_eligible_action_strategy_for_supervisor(outcomes_list)
        )

    def test_action_strategy_60_perc_outliers_eligible_ineligible_percentage(
        self,
    ) -> None:
        # 1 of the 4 officers are outliers - does not meet eligibility criteria
        outcomes_list = [
            SupervisionOfficerOutcomes(
                external_id="1",
                pseudonymized_id="hash1",
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
            ),
            SupervisionOfficerOutcomes(
                external_id="2",
                pseudonymized_id="hash2",
                caseload_category="ALL",
                outlier_metrics=[],
                top_x_pct_metrics=[
                    {
                        "metric_id": "incarceration_starts_and_inferred",
                        "top_x_pct": 10,
                    }
                ],
            ),
            SupervisionOfficerOutcomes(
                external_id="3",
                pseudonymized_id="hash3",
                caseload_category="ALL",
                outlier_metrics=[],
                top_x_pct_metrics=[
                    {
                        "metric_id": "incarceration_starts_and_inferred",
                        "top_x_pct": 10,
                    }
                ],
            ),
            SupervisionOfficerOutcomes(
                external_id="4",
                pseudonymized_id="hash4",
                caseload_category="ALL",
                outlier_metrics=[],
                top_x_pct_metrics=[
                    {
                        "metric_id": "incarceration_starts_and_inferred",
                        "top_x_pct": 10,
                    }
                ],
            ),
        ]
        events: list[ActionStrategySurfacedEvent] = []
        qualifier = OutliersActionStrategyQualifier(events=events, config=METRIC_CONFIG)
        self.assertEqual(
            None, qualifier.get_eligible_action_strategy_for_supervisor(outcomes_list)
        )

    def test_action_strategy_60_perc_outliers_eligible_ineligible_event(
        self,
    ) -> None:
        # 3 of the 4 officers are outliers - meets eligibility criteria
        outcomes_list = [
            SupervisionOfficerOutcomes(
                external_id="1",
                pseudonymized_id="hash1",
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
            ),
            SupervisionOfficerOutcomes(
                external_id="2",
                pseudonymized_id="hash2",
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
            ),
            SupervisionOfficerOutcomes(
                external_id="3",
                pseudonymized_id="hash3",
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
            ),
            SupervisionOfficerOutcomes(
                external_id="4",
                pseudonymized_id="hash4",
                caseload_category="ALL",
                outlier_metrics=[],
                top_x_pct_metrics=[
                    {
                        "metric_id": "incarceration_starts_and_inferred",
                        "top_x_pct": 10,
                    }
                ],
            ),
        ]

        # disqualifying event - does not meet eligibility criteria
        events = [
            ActionStrategySurfacedEvent(
                state_code="us_pa",
                user_pseudonymized_id="any_hash",
                officer_pseudonymized_id=None,
                action_strategy=ActionStrategyType.ACTION_STRATEGY_60_PERC_OUTLIERS.value,
                timestamp=datetime.date(2023, 8, 1),
            )
        ]
        qualifier = OutliersActionStrategyQualifier(events=events, config=METRIC_CONFIG)
        self.assertEqual(
            None, qualifier.get_eligible_action_strategy_for_supervisor(outcomes_list)
        )

    @freezegun.freeze_time(datetime.datetime(2023, 8, 24, 0, 0, 0, 0))
    def test_action_strategy_outlier_absconsion_eligible_eligible(
        self,
    ) -> None:
        outcomes = SupervisionOfficerOutcomes(
            external_id="1",
            pseudonymized_id="hash1",
            caseload_category="ALL",
            outlier_metrics=[
                {
                    "metric_id": "absconsions_bench_warrants",
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
        )

        events = [
            ActionStrategySurfacedEvent(
                state_code="us_pa",
                user_pseudonymized_id="any_hash",
                officer_pseudonymized_id="hash1",
                action_strategy=ActionStrategyType.ACTION_STRATEGY_OUTLIER_ABSCONSION.value,
                timestamp=datetime.date(2023, 6, 1),
            )
        ]
        qualifier = OutliersActionStrategyQualifier(events=events, config=METRIC_CONFIG)
        self.assertTrue(qualifier.action_strategy_outlier_absconsion_eligible(outcomes))

    @freezegun.freeze_time(datetime.datetime(2023, 8, 24, 0, 0, 0, 0))
    def test_action_strategy_outlier_absconsion_eligible_ineligible_date(
        self,
    ) -> None:
        outcomes = SupervisionOfficerOutcomes(
            external_id="1",
            pseudonymized_id="hash1",
            caseload_category="ALL",
            outlier_metrics=[
                {
                    "metric_id": "absconsions_bench_warrants",
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
        )

        events = [
            ActionStrategySurfacedEvent(
                state_code="us_pa",
                user_pseudonymized_id="any_hash",
                officer_pseudonymized_id="hash1",
                action_strategy=ActionStrategyType.ACTION_STRATEGY_OUTLIER_ABSCONSION.value,
                timestamp=datetime.date(2023, 8, 1),
            )
        ]
        qualifier = OutliersActionStrategyQualifier(events=events, config=METRIC_CONFIG)
        self.assertFalse(
            qualifier.action_strategy_outlier_absconsion_eligible(outcomes)
        )

    @freezegun.freeze_time(datetime.datetime(2023, 8, 24, 0, 0, 0, 0))
    def test_action_strategy_outlier_absconsion_eligible_ineligible_metric_type(
        self,
    ) -> None:

        outcomes = SupervisionOfficerOutcomes(
            external_id="1",
            pseudonymized_id="hash1",
            caseload_category="ALL",
            outlier_metrics=[
                {
                    "metric_id": "incarceration_starts_and_inferred",
                    "statuses_over_time": [
                        {
                            "end_date": "2023-08-01",
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
        )

        events: list[ActionStrategySurfacedEvent] = []
        config = OutliersBackendConfig(
            metrics=[
                OutliersMetricConfig.build_from_metric(
                    state_code=StateCode.US_XX,
                    metric=INCARCERATION_STARTS_AND_INFERRED_TECHNICAL_VIOLATION,
                    title_display_name="Technical Incarceration Rate (TPVs)",
                    body_display_name="technical incarceration rate",
                    event_name="technical incarcerations",
                    event_name_singular="technical incarceration",
                    event_name_past_tense="had a technical incarceration",
                ),
            ],
        )
        qualifier = OutliersActionStrategyQualifier(events=events, config=config)
        self.assertFalse(
            qualifier.action_strategy_outlier_absconsion_eligible(outcomes)
        )

    @freezegun.freeze_time(datetime.datetime(2023, 8, 24, 0, 0, 0, 0))
    def test_action_strategy_outlier_absconsion_eligible_ineligible_no_matching_metric(
        self,
    ) -> None:
        outcomes = SupervisionOfficerOutcomes(
            external_id="1",
            pseudonymized_id="hash1",
            caseload_category="ALL",
            outlier_metrics=[
                {
                    "metric_id": "non_matching_metric",
                    "statuses_over_time": [
                        {
                            "end_date": "2023-08-01",
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
        )

        events: list[ActionStrategySurfacedEvent] = []
        qualifier = OutliersActionStrategyQualifier(events=events, config=METRIC_CONFIG)
        self.assertFalse(
            qualifier.action_strategy_outlier_absconsion_eligible(outcomes)
        )

    def test_get_eligible_action_strategy_for_officer_absconsion(self) -> None:
        officer = SupervisionOfficerEntity(
            full_name=PersonName(**{"given_names": "DRACO", "surname": "MALFOY"}),
            external_id="1",
            pseudonymized_id="hash1",
            supervisor_external_id="102",
            supervisor_external_ids=["102"],
            district="Hogwarts",
            email="officer1@recidiviz.org",
            avg_daily_population=10.0,
            include_in_outcomes=True,
        )

        outcomes = SupervisionOfficerOutcomes(
            pseudonymized_id="hash1",
            external_id="1",
            caseload_category="ALL",
            outlier_metrics=[
                {
                    "metric_id": "absconsions_bench_warrants",
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
        )

        events = [
            ActionStrategySurfacedEvent(
                state_code="us_pa",
                user_pseudonymized_id="any_hash",
                officer_pseudonymized_id="hash1",
                action_strategy=ActionStrategyType.ACTION_STRATEGY_OUTLIER.value,
                timestamp=datetime.date(2023, 8, 1),
            ),
            ActionStrategySurfacedEvent(
                state_code="us_pa",
                user_pseudonymized_id="any_hash",
                officer_pseudonymized_id="hash1",
                action_strategy=ActionStrategyType.ACTION_STRATEGY_OUTLIER_3_MONTHS.value,
                timestamp=datetime.date(2023, 8, 1),
            ),
            ActionStrategySurfacedEvent(
                state_code="us_pa",
                user_pseudonymized_id="any_hash",
                officer_pseudonymized_id="hash1",
                action_strategy=ActionStrategyType.ACTION_STRATEGY_OUTLIER_ABSCONSION.value,
                timestamp=datetime.date(2023, 6, 1),
            ),
        ]

        qualifier = OutliersActionStrategyQualifier(events=events, config=METRIC_CONFIG)
        self.assertEqual(
            ActionStrategyType.ACTION_STRATEGY_OUTLIER_ABSCONSION.value,
            qualifier.get_eligible_action_strategy_for_officer(officer, outcomes),
        )

    @freezegun.freeze_time(datetime.datetime(2023, 8, 24, 0, 0, 0, 0))
    def test_action_strategy_outlier_new_officer_eligible_eligible(
        self,
    ) -> None:
        officer = SupervisionOfficerEntity(
            full_name=PersonName(**{"given_names": "DRACO", "surname": "MALFOY"}),
            external_id="1",
            pseudonymized_id="hash1",
            supervisor_external_id="102",
            supervisor_external_ids=["102"],
            district="Hogwarts",
            email="officer1@recidiviz.org",
            avg_daily_population=10.0,
            include_in_outcomes=True,
            # Within 15 months of today
            earliest_person_assignment_date=datetime.date(2022, 7, 30),
        )
        outcomes = SupervisionOfficerOutcomes(
            external_id="1",
            pseudonymized_id="hash1",
            caseload_category="ALL",
            outlier_metrics=[
                {
                    "metric_id": "absconsions_bench_warrants",
                    "statuses_over_time": [
                        {
                            "end_date": "2023-05-01",
                            "metric_rate": 0.1,
                            "status": "FAR",
                        },
                    ],
                }
            ],
            top_x_pct_metrics=[],
        )

        events: list[ActionStrategySurfacedEvent] = []
        qualifier = OutliersActionStrategyQualifier(events=events, config=METRIC_CONFIG)
        self.assertTrue(
            qualifier.action_strategy_outlier_new_officer_eligible(officer, outcomes)
        )

    @freezegun.freeze_time(datetime.datetime(2023, 8, 24, 0, 0, 0, 0))
    def test_action_strategy_outlier_new_officer_eligible_ineligible_date(
        self,
    ) -> None:
        officer = SupervisionOfficerEntity(
            full_name=PersonName(**{"given_names": "DRACO", "surname": "MALFOY"}),
            external_id="1",
            pseudonymized_id="hash1",
            supervisor_external_id="102",
            supervisor_external_ids=["102"],
            district="Hogwarts",
            email="officer1@recidiviz.org",
            avg_daily_population=10.0,
            include_in_outcomes=True,
            # More than 15 months from today
            earliest_person_assignment_date=datetime.date(2022, 5, 23),
        )
        outcomes = SupervisionOfficerOutcomes(
            external_id="1",
            pseudonymized_id="hash1",
            caseload_category="ALL",
            outlier_metrics=[
                {
                    "metric_id": "absconsions_bench_warrants",
                    "statuses_over_time": [
                        {
                            "end_date": "2023-05-01",
                            "metric_rate": 0.1,
                            "status": "FAR",
                        },
                    ],
                }
            ],
            top_x_pct_metrics=[],
        )

        events: list[ActionStrategySurfacedEvent] = []
        qualifier = OutliersActionStrategyQualifier(events=events, config=METRIC_CONFIG)
        self.assertFalse(
            qualifier.action_strategy_outlier_new_officer_eligible(officer, outcomes)
        )

    @freezegun.freeze_time(datetime.datetime(2023, 8, 24, 0, 0, 0, 0))
    def test_action_strategy_outlier_new_officer_eligible_ineligible_missing_date(
        self,
    ) -> None:
        officer = SupervisionOfficerEntity(
            full_name=PersonName(**{"given_names": "DRACO", "surname": "MALFOY"}),
            external_id="1",
            pseudonymized_id="hash1",
            supervisor_external_id="102",
            supervisor_external_ids=["102"],
            district="Hogwarts",
            email="officer1@recidiviz.org",
            avg_daily_population=10.0,
            include_in_outcomes=True,
        )
        outcomes = SupervisionOfficerOutcomes(
            external_id="1",
            pseudonymized_id="hash1",
            caseload_category="ALL",
            outlier_metrics=[
                {
                    "metric_id": "absconsions_bench_warrants",
                    "statuses_over_time": [
                        {
                            "end_date": "2023-05-01",
                            "metric_rate": 0.1,
                            "status": "FAR",
                        },
                    ],
                }
            ],
            top_x_pct_metrics=[],
        )

        events: list[ActionStrategySurfacedEvent] = []
        qualifier = OutliersActionStrategyQualifier(events=events, config=METRIC_CONFIG)
        self.assertFalse(
            qualifier.action_strategy_outlier_new_officer_eligible(officer, outcomes)
        )

    @freezegun.freeze_time(datetime.datetime(2023, 8, 24, 0, 0, 0, 0))
    def test_action_strategy_outlier_new_officer_eligible_ineligible_event(
        self,
    ) -> None:
        officer = SupervisionOfficerEntity(
            full_name=PersonName(**{"given_names": "DRACO", "surname": "MALFOY"}),
            external_id="1",
            pseudonymized_id="hash1",
            supervisor_external_id="102",
            supervisor_external_ids=["102"],
            district="Hogwarts",
            email="officer1@recidiviz.org",
            avg_daily_population=10.0,
            include_in_outcomes=True,
            earliest_person_assignment_date=datetime.date(2022, 9, 30),
        )
        outcomes = SupervisionOfficerOutcomes(
            external_id="1",
            pseudonymized_id="hash1",
            caseload_category="ALL",
            outlier_metrics=[
                {
                    "metric_id": "absconsions_bench_warrants",
                    "statuses_over_time": [
                        {
                            "end_date": "2023-05-01",
                            "metric_rate": 0.1,
                            "status": "FAR",
                        },
                    ],
                }
            ],
            top_x_pct_metrics=[],
        )

        events = [
            ActionStrategySurfacedEvent(
                state_code="us_pa",
                user_pseudonymized_id="any_hash",
                officer_pseudonymized_id="hash1",
                action_strategy=ActionStrategyType.ACTION_STRATEGY_OUTLIER_NEW_OFFICER.value,
                timestamp=datetime.date(2020, 8, 1),
            )
        ]
        qualifier = OutliersActionStrategyQualifier(events=events, config=METRIC_CONFIG)
        self.assertFalse(
            qualifier.action_strategy_outlier_new_officer_eligible(officer, outcomes)
        )

    @freezegun.freeze_time(datetime.datetime(2023, 8, 24, 0, 0, 0, 0))
    def test_get_eligible_action_strategy_for_officer_new_officer(self) -> None:
        officer = SupervisionOfficerEntity(
            full_name=PersonName(**{"given_names": "DRACO", "surname": "MALFOY"}),
            external_id="1",
            pseudonymized_id="hash1",
            supervisor_external_id="102",
            supervisor_external_ids=["102"],
            district="Hogwarts",
            email="officer1@recidiviz.org",
            avg_daily_population=10.0,
            include_in_outcomes=True,
            earliest_person_assignment_date=datetime.date(2022, 9, 30),
        )
        outcomes = SupervisionOfficerOutcomes(
            external_id="1",
            pseudonymized_id="hash1",
            caseload_category="ALL",
            outlier_metrics=[
                {
                    "metric_id": "absconsions_bench_warrants",
                    "statuses_over_time": [
                        {
                            "end_date": "2023-05-01",
                            "metric_rate": 0.1,
                            "status": "FAR",
                        },
                    ],
                }
            ],
            top_x_pct_metrics=[],
        )

        events = [
            ActionStrategySurfacedEvent(
                state_code="us_pa",
                user_pseudonymized_id="any_hash",
                officer_pseudonymized_id="hash1",
                action_strategy=ActionStrategyType.ACTION_STRATEGY_OUTLIER.value,
                timestamp=datetime.date(2023, 8, 1),
            ),
            ActionStrategySurfacedEvent(
                state_code="us_pa",
                user_pseudonymized_id="any_hash",
                officer_pseudonymized_id="hash1",
                action_strategy=ActionStrategyType.ACTION_STRATEGY_OUTLIER_3_MONTHS.value,
                timestamp=datetime.date(2023, 8, 1),
            ),
            ActionStrategySurfacedEvent(
                state_code="us_pa",
                user_pseudonymized_id="any_hash",
                officer_pseudonymized_id="hash1",
                action_strategy=ActionStrategyType.ACTION_STRATEGY_OUTLIER_ABSCONSION.value,
                timestamp=datetime.date(2023, 8, 2),
            ),
        ]

        qualifier = OutliersActionStrategyQualifier(events=events, config=METRIC_CONFIG)
        self.assertEqual(
            ActionStrategyType.ACTION_STRATEGY_OUTLIER_NEW_OFFICER.value,
            qualifier.get_eligible_action_strategy_for_officer(officer, outcomes),
        )
