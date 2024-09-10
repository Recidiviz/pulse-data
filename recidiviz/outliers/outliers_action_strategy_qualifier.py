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
"""Class that contains eligibility logic for each action strategy. """
from datetime import date, datetime
from typing import List, Optional

import attr
from dateutil.relativedelta import relativedelta

from recidiviz.outliers.types import (
    ActionStrategySurfacedEvent,
    ActionStrategyType,
    OutliersBackendConfig,
    SupervisionOfficerEntity,
)


@attr.s
class OutliersActionStrategyQualifier:
    """
    Class that contains eligibility logic for each action strategy.
    """

    events: List[ActionStrategySurfacedEvent] = attr.ib()

    config: OutliersBackendConfig = attr.ib()

    def action_strategy_outlier_eligible(
        self, officer_pseudo_id: str, is_outlier: bool
    ) -> bool:
        """
        Officer is eligible for ACTION_STRATEGY_OUTLIER if
        (1) They are an outlier
        (2) ACTION_STRATEGY_OUTLIER prompt has not surfaced for this officer
        """
        disqualifying_events = [
            e
            for e in self.events
            if e.officer_pseudonymized_id == officer_pseudo_id
            and e.action_strategy == ActionStrategyType.ACTION_STRATEGY_OUTLIER.value
        ]
        return is_outlier and len(disqualifying_events) == 0

    def check_for_consecutive_3_months(self, outlier_metrics: List) -> bool:
        """
        Checks if the specified officer was an outlier for 3 consecutive months on any metric
        """
        for metric in outlier_metrics:
            statuses = metric["statuses_over_time"]
            if len(statuses) >= 3:
                for i in range(len(statuses) - 2):
                    # determine if metrics are consecutive
                    curr_date = datetime.fromisoformat(statuses[i]["end_date"])
                    prev_date = datetime.fromisoformat(statuses[i + 1]["end_date"])
                    two_dates_ago = datetime.fromisoformat(statuses[i + 2]["end_date"])
                    are_consecutive = curr_date == prev_date + relativedelta(
                        months=1
                    ) and prev_date == two_dates_ago + relativedelta(months=1)

                    # determine if metrics are all outliers
                    are_outliers = all(item["status"] == "FAR" for item in statuses)

                    # If the metrics are consecutive AND outliers, criteria is met so return true
                    if are_consecutive and are_outliers:
                        return True
        return False

    def date_is_period_prior_than_current(self, timestamp: date) -> bool:
        """
        Checks if the date provided is within the same calendar month as today
        """
        current_date = date.today()
        first_of_this_month = current_date.replace(day=1)

        return timestamp < first_of_this_month

    def date_is_within_current_or_prior_period(self, timestamp: date) -> bool:
        """
        Checks if the date provided is within two periods (calendar months) prior to today
        """
        current_date = date.today()
        start_date_of_last_period = current_date.replace(day=1) - relativedelta(
            months=1
        )

        return current_date >= timestamp >= start_date_of_last_period

    def action_strategy_outlier_3_months_eligible(
        self,
        officer: SupervisionOfficerEntity,
    ) -> bool:
        """
        Officer is eligible for ACTION_STRATEGY_OUTLIER_3_MONTHS if
        (1) they are an outlier on any metric for 3+ consecutive months,
        (2) this action strategy was not yet surfaced,
        (3) ACTION_STRATEGY_OUTLIER has surfaced in a previous month
            OR ACTION_STRATEGY_60_PERC_OUTLIERS has surfaced in a previous month.
        """
        is_eligible = self.check_for_consecutive_3_months(officer.outlier_metrics)
        if not is_eligible:
            return False

        officer_pseudo_id = officer.pseudonymized_id
        disqualifying_events = [
            e
            for e in self.events
            if e.officer_pseudonymized_id == officer_pseudo_id
            and e.action_strategy
            == ActionStrategyType.ACTION_STRATEGY_OUTLIER_3_MONTHS.value
        ]
        if len(disqualifying_events) > 0:
            return False

        outlier_as_surfaced = False
        sixty_perc_outlier_as_surfaced = False
        for e in self.events:
            if e.officer_pseudonymized_id == officer_pseudo_id:
                if (
                    e.action_strategy
                    == ActionStrategyType.ACTION_STRATEGY_OUTLIER.value
                    and self.date_is_period_prior_than_current(e.timestamp)
                ):
                    outlier_as_surfaced = True
            if (
                e.action_strategy
                == ActionStrategyType.ACTION_STRATEGY_60_PERC_OUTLIERS.value
                and self.date_is_period_prior_than_current(e.timestamp)
            ):

                sixty_perc_outlier_as_surfaced = True
        return outlier_as_surfaced or sixty_perc_outlier_as_surfaced

    def action_strategy_outlier_absconsion_eligible(
        self, officer: SupervisionOfficerEntity
    ) -> bool:
        """
        Officer is eligible for ACTION_STRATEGY_OUTLIER_ABSCONSION if
        (1) They are ONLY an outlier on an absconsion metric
        (2) There are not any surfaced events with this type in the current or previous calendar months
        """
        # Check that there is only one outlier metric
        if len(officer.outlier_metrics) != 1:
            return False
        # Check that the outlier metric is an absconsion metric
        if not any(
            metric.name == officer.outlier_metrics[0]["metric_id"]
            and metric.is_absconsion_metric
            for metric in self.config.metrics
        ):
            return False

        officer_pseudo_id = officer.pseudonymized_id
        disqualifying_events = [
            e
            for e in self.events
            if e.officer_pseudonymized_id == officer_pseudo_id
            and e.action_strategy
            == ActionStrategyType.ACTION_STRATEGY_OUTLIER_ABSCONSION.value
            and self.date_is_within_current_or_prior_period(e.timestamp)
        ]
        if len(disqualifying_events) > 0:
            return False

        return True

    def action_strategy_60_perc_outliers_eligible(
        self, officers: List[SupervisionOfficerEntity]
    ) -> bool:
        """
        Supervisor is eligible for ACTION_STRATEGY_60_PERC_OUTLIERS if
        (1) They have more than 4 officers on their team
        (2) 60% or more of their officers are outliers in the current month
        (3) ACTION_STRATEGY_60_PERC_OUTLIERS has not been surfaced for this supervisor
        """
        # if supervisor has fewer than 4 officers on their team, they are not eligible
        if len(officers) < 4:
            return False

        # if less than 60 percent of officers are outliers, supervisor is not eligible
        num_outliers = len([o for o in officers if len(o.outlier_metrics) > 0])
        if num_outliers / len(officers) < 0.6:
            return False

        # if the supervisor has already surfaced this banner, they are not eligible
        disqualifying_events = [
            e
            for e in self.events
            if e.action_strategy
            == ActionStrategyType.ACTION_STRATEGY_60_PERC_OUTLIERS.value
        ]
        if len(disqualifying_events) > 0:
            return False
        return True

    def get_eligible_action_strategy_for_officer(
        self, officer: SupervisionOfficerEntity
    ) -> Optional[str]:
        """
        Returns the eligible action strategy type enum value for a given officer,
        or None if the officer is not eligible for any of the action strategies
        """
        is_outlier = len(officer.outlier_metrics) > 0
        officer_pseudo_id = officer.pseudonymized_id
        if self.action_strategy_outlier_eligible(officer_pseudo_id, is_outlier):
            return ActionStrategyType.ACTION_STRATEGY_OUTLIER.value
        if self.action_strategy_outlier_3_months_eligible(officer):
            return ActionStrategyType.ACTION_STRATEGY_OUTLIER_3_MONTHS.value
        if self.action_strategy_outlier_absconsion_eligible(officer):
            return ActionStrategyType.ACTION_STRATEGY_OUTLIER_ABSCONSION.value
        return None

    def get_eligible_action_strategy_for_supervisor(
        self, officers: List[SupervisionOfficerEntity]
    ) -> Optional[str]:
        """
        Returns the eligible action strategy type enum value for a supervisor given their officers,
        or None if the supervisor is not eligible for any of the action strategies
        """
        if self.action_strategy_60_perc_outliers_eligible(officers):
            return ActionStrategyType.ACTION_STRATEGY_60_PERC_OUTLIERS.value
        return None
