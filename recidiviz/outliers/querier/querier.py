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
"""Implements Querier abstractions for Outliers data sources"""
from copy import copy
from datetime import date
from enum import Enum
from typing import Dict, List

import attr
import numpy as np
from dateutil.relativedelta import relativedelta
from sqlalchemy import and_
from sqlalchemy.orm import Session, aliased

from recidiviz.aggregated_metrics.metric_time_periods import MetricTimePeriod
from recidiviz.calculator.query.state.views.analyst_data.models.metric_unit_of_analysis_type import (
    MetricUnitOfAnalysisType,
)
from recidiviz.calculator.query.state.views.outliers.supervision_metrics_helpers import (
    OUTLIERS_CONFIGS_BY_STATE,
)
from recidiviz.common.constants.states import StateCode
from recidiviz.persistence.database.schema.outliers.schema import (
    SupervisionOfficer,
    SupervisionOfficerMetric,
    SupervisionStateMetric,
    SupervisionUnit,
)
from recidiviz.persistence.database.session_factory import SessionFactory
from recidiviz.persistence.database.sqlalchemy_database_key import SQLAlchemyDatabaseKey


class TargetStatus(Enum):
    MET = "MET"
    NEAR = "NEAR"
    FAR = "FAR"


@attr.s
class Entity:
    # The name of the unit of analysis, i.e. full name of a SupervisionOfficer object
    name: str = attr.ib()
    # The current rate for this unit of analysis
    rate: float = attr.ib()
    # Categorizes how the rate for this entity compares to the target value
    target_status: TargetStatus = attr.ib()
    # The rate for the prior YEAR period for this unit of analysis
    prev_rate: float = attr.ib()
    # The target status for the prior YEAR period for this unit of analysis
    prev_target_status: TargetStatus = attr.ib()


@attr.s
class MetricContext:
    # Unless otherwise specified, the target is the state average for the current period
    target: float = attr.ib()
    # The target for the previous period
    prev_target: float = attr.ib()
    # The smallest value out of all the officers in a state
    min: float = attr.ib()
    # The largest value out of all the officers in a state
    max: float = attr.ib()
    # The interquartile range (IQR) given the metric value for all the officers in the state for the current period
    iqr: float = attr.ib()
    # The IQR for the previous period
    prev_iqr: float = attr.ib()
    # List of all records of the SupervisionOfficerMetric joined to the SupervisionOfficer table for
    # a specific metric and end date
    current_period_officer_metrics: list = attr.ib()
    prev_period_officer_metrics: list = attr.ib()


@attr.s
class MetricInfo:
    context: Dict[str, float] = attr.ib()
    entities: List[Entity] = attr.ib()


@attr.s
class OutliersReportData:
    metrics: Dict[str, MetricInfo] = attr.ib()
    entity_type: MetricUnitOfAnalysisType = attr.ib()


class OutliersQuerier:
    """Implements Querier abstractions for Outliers data sources"""

    def get_officer_level_report_data_for_all_units(
        self, state_code: StateCode, db_key: SQLAlchemyDatabaseKey, end_date: date
    ) -> dict:
        """
        Returns officer-level data by unit for all units in a state in the following format:
        {
            < supervision unit id >: {
                entity_type: "OFFICER"
                metrics: {
                    < metric id >: {
                        context: {
                            target: float,
                            min: float,
                            max: float,
                        },
                        entities: List[Entity] (as defined above)
                    }

                }
            },
            ...
        }
        """
        end_date = end_date.replace(day=1)
        prev_end_date = end_date - relativedelta(months=1)

        with SessionFactory.using_database(db_key, autocommit=False) as session:
            unit_ids = session.query(SupervisionUnit.external_id).all()
            metric_id_to_metric_context = {
                metric_id: self._get_metric_context_from_db(
                    session, metric_id, end_date, prev_end_date
                )
                for metric_id in self.get_state_metrics(state_code)
            }

            return {
                external_id: self._get_officer_level_data_for_unit(
                    external_id,
                    metric_id_to_metric_context,
                )
                for (external_id,) in unit_ids
            }

    def _get_officer_level_data_for_unit(
        self,
        unit_external_id: str,
        metric_id_to_metric_context: Dict[str, MetricContext],
    ) -> OutliersReportData:
        """
        Given the unit_external_id, get the officer-level data for the period with end_date=end_date compared to
        the period with end_date=prev_end_date.
        """
        metrics_results: Dict[str, MetricInfo] = {}

        for metric_id, metric_context in metric_id_to_metric_context.items():
            entities: List[Entity] = []

            metric_context = metric_id_to_metric_context[metric_id]

            # Get officer metrics for this unit
            officer_metrics_to_officer_for_unit = list(
                filter(
                    lambda officer_metric_record: (
                        officer_metric_record.location_external_id == unit_external_id
                    ),
                    copy(metric_context.current_period_officer_metrics),
                )
            )

            for officer_metric_to_officer_record in officer_metrics_to_officer_for_unit:
                officer_id = officer_metric_to_officer_record.officer_id
                rate = (
                    officer_metric_to_officer_record.metric_value
                    / officer_metric_to_officer_record.avg_daily_population
                )

                status = self.get_target_status(
                    metric_id,
                    rate,
                    metric_context.target,
                    metric_context.iqr,
                )

                prev_period_record = [
                    past_officer_metric_record
                    for past_officer_metric_record in metric_context.prev_period_officer_metrics
                    if past_officer_metric_record.officer_id == officer_id
                ]

                prev_rate = (
                    prev_period_record[0].metric_value
                    / prev_period_record[0].avg_daily_population
                )

                prev_status = self.get_target_status(
                    metric_id,
                    prev_rate,
                    metric_context.prev_target,
                    metric_context.iqr,
                )

                entities.append(
                    Entity(
                        name=officer_metric_to_officer_record.full_name,
                        rate=rate,
                        target_status=status,
                        prev_rate=prev_rate,
                        prev_target_status=prev_status,
                    )
                )

            info = MetricInfo(
                context={
                    "target": metric_context.target,
                    "min": metric_context.min,
                    "max": metric_context.max,
                },
                entities=entities,
            )
            metrics_results[metric_id] = info

        return OutliersReportData(
            entity_type=MetricUnitOfAnalysisType.SUPERVISION_OFFICER,
            metrics=metrics_results,
        )

    def _get_metric_context_from_db(
        self, session: Session, metric_id: str, end_date: date, prev_end_date: date
    ) -> MetricContext:
        """
        For the given metric_id, calculate and hydrate the MetricContext object as defined above.
        """

        target = self.get_state_target_from_db(session, metric_id, end_date)
        prev_target = self.get_state_target_from_db(session, metric_id, prev_end_date)

        officer_metric = aliased(SupervisionOfficerMetric)
        avg_pop_metric = aliased(SupervisionOfficerMetric)

        combined_period_officer_metrics = (
            session.query(officer_metric)
            .select_from(officer_metric)
            .join(
                avg_pop_metric,
                and_(
                    avg_pop_metric.officer_id == officer_metric.officer_id,
                    avg_pop_metric.end_date == officer_metric.end_date,
                    avg_pop_metric.period == officer_metric.period,
                    avg_pop_metric.metric_id == "avg_daily_population",
                    officer_metric.metric_id != "avg_daily_population",
                ),
            )
            .join(
                SupervisionOfficer,
                SupervisionOfficer.external_id == officer_metric.officer_id,
            )
            .filter(
                officer_metric.metric_id == metric_id,
                officer_metric.end_date.in_([end_date, prev_end_date]),
                officer_metric.period == MetricTimePeriod.YEAR.value,
            )
            .with_entities(
                officer_metric.officer_id,
                officer_metric.metric_id,
                officer_metric.metric_value,
                officer_metric.end_date,
                avg_pop_metric.metric_value.label("avg_daily_population"),
                SupervisionOfficer.full_name,
                SupervisionOfficer.location_external_id,
            )
            .all()
        )

        metric_rates = [
            record.metric_value / record.avg_daily_population
            for record in combined_period_officer_metrics
        ]

        min_rate_for_combined_periods = min(metric_rates)
        max_rate_for_combined_periods = max(metric_rates)

        current_period_officer_metrics = list(
            filter(
                lambda officer_metric_record: (
                    officer_metric_record.end_date == end_date
                ),
                copy(combined_period_officer_metrics),
            )
        )

        previous_period_officer_metrics = list(
            filter(
                lambda officer_metric_record: (
                    officer_metric_record.end_date == prev_end_date
                ),
                copy(combined_period_officer_metrics),
            )
        )

        iqr = self.get_iqr(
            [
                record.metric_value / record.avg_daily_population
                for record in current_period_officer_metrics
            ]
        )

        prev_iqr = self.get_iqr(
            [
                record.metric_value / record.avg_daily_population
                for record in previous_period_officer_metrics
            ]
        )

        return MetricContext(
            target=target,
            iqr=iqr,
            min=min_rate_for_combined_periods,
            max=max_rate_for_combined_periods,
            prev_target=prev_target,
            prev_iqr=prev_iqr,
            current_period_officer_metrics=current_period_officer_metrics,
            prev_period_officer_metrics=previous_period_officer_metrics,
        )

    @staticmethod
    def get_target_status(
        metric: str, rate: float, target: float, iqr: float
    ) -> TargetStatus:
        if "task_completions" in metric:
            if rate >= target:
                return TargetStatus.MET
            if target - rate <= iqr:
                return TargetStatus.NEAR
            return TargetStatus.FAR

        if rate <= target:
            return TargetStatus.MET
        if rate - target <= iqr:
            return TargetStatus.NEAR
        return TargetStatus.FAR

    @staticmethod
    def get_state_metrics(state_code: StateCode) -> List[str]:
        return OUTLIERS_CONFIGS_BY_STATE[state_code].metrics

    @staticmethod
    def get_state_target_from_db(
        session: Session, metric: str, end_date: date
    ) -> float:
        state_metric_value = (
            session.query(SupervisionStateMetric.metric_value)
            .filter(
                SupervisionStateMetric.metric_id == metric,
                SupervisionStateMetric.end_date == end_date,
                SupervisionStateMetric.period == MetricTimePeriod.YEAR.value,
            )
            .scalar()
        )

        state_avg_daily_population = (
            session.query(SupervisionStateMetric.metric_value)
            .filter(
                SupervisionStateMetric.metric_id == "avg_daily_population",
                SupervisionStateMetric.end_date == end_date,
                SupervisionStateMetric.period == MetricTimePeriod.YEAR.value,
            )
            .scalar()
        )

        return state_metric_value / state_avg_daily_population

    @staticmethod
    def get_iqr(values: List[float]) -> float:
        q1, q3 = np.percentile(values, [25, 75])
        iqr = q3 - q1
        return iqr
