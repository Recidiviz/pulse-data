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
from typing import Dict, List, Tuple

import numpy as np
from dateutil.relativedelta import relativedelta
from sqlalchemy import and_
from sqlalchemy.orm import Session, aliased

from recidiviz.aggregated_metrics.metric_time_periods import MetricTimePeriod
from recidiviz.common.constants.states import StateCode
from recidiviz.outliers.outliers_configs import OUTLIERS_CONFIGS_BY_STATE
from recidiviz.outliers.types import (
    MetricContext,
    MetricOutcome,
    OfficerMetricEntity,
    OfficerSupervisorReportData,
    OutlierMetricInfo,
    OutliersMetricConfig,
    TargetStatus,
    TargetStatusStrategy,
)
from recidiviz.persistence.database.schema.outliers.schema import (
    SupervisionOfficer,
    SupervisionOfficerMetric,
    SupervisionOfficerSupervisor,
    SupervisionStateMetric,
)
from recidiviz.persistence.database.schema_type import SchemaType
from recidiviz.persistence.database.session_factory import SessionFactory
from recidiviz.persistence.database.sqlalchemy_database_key import SQLAlchemyDatabaseKey


class OutliersQuerier:
    """Implements Querier abstractions for Outliers data sources"""

    def get_officer_level_report_data_for_all_officer_supervisors(
        self, state_code: StateCode, end_date: date
    ) -> Dict[str, OfficerSupervisorReportData]:
        """
        Returns officer-level data by officer supervisor for all officer supervisors in a state in the following format:
        {
            < SupervisionOfficerSupervisor.external_id >: {
                metrics: [
                    OutlierMetricInfo(
                        metric: OutliersMetricConfig,
                        target: float,
                        other_officers: {
                            TargetStatus.MET: float[],
                            TargetStatus.NEAR: float[],
                            TargetStatus.FAR: float[],
                        },
                        highlighted_officers: List[OfficerMetricEntity]
                    ),
                    OutlierMetricInfo(), ...
                ],
                metrics_without_outliers: List[OutliersMetricConfig],
                email_address: str
            },
            ...
        }
        """
        db_key = SQLAlchemyDatabaseKey(
            SchemaType.OUTLIERS, db_name=state_code.value.lower()
        )
        end_date = end_date.replace(day=1)
        prev_end_date = end_date - relativedelta(months=1)

        with SessionFactory.using_database(db_key, autocommit=False) as session:
            officer_supervisor_records = session.query(
                SupervisionOfficerSupervisor.external_id,
                SupervisionOfficerSupervisor.email,
            ).all()

            metric_name_to_metric_context = {
                metric: self._get_metric_context_from_db(
                    session, metric, end_date, prev_end_date
                )
                for metric in self.get_state_metrics(state_code)
            }

            officer_supervisor_id_to_data = {}

            for (external_id, email) in officer_supervisor_records:
                (
                    metrics,
                    metrics_with_outliers,
                ) = self._get_officer_level_data_for_officer_supervisor(
                    external_id,
                    metric_name_to_metric_context,
                )

                officer_supervisor_id_to_data[
                    external_id
                ] = OfficerSupervisorReportData(
                    metrics=metrics,
                    metrics_without_outliers=metrics_with_outliers,
                    recipient_email_address=email,
                )

            return officer_supervisor_id_to_data

    @staticmethod
    def _get_officer_level_data_for_officer_supervisor(
        supervision_officer_supervisor_id: str,
        metric_id_to_metric_context: Dict[OutliersMetricConfig, MetricContext],
    ) -> Tuple[List[OutlierMetricInfo], List[OutliersMetricConfig]]:
        """
        Given the supervision_officer_supervisor_id, get the officer-level data for all officers supervised by
        this officer for the period with end_date=end_date compared to the period with end_date=prev_end_date.
        """
        metrics_results: List[OutlierMetricInfo] = []
        metrics_without_outliers = []

        for metric, metric_context in metric_id_to_metric_context.items():
            entities = metric_context.entities

            other_officer_rates: Dict[TargetStatus, List[float]] = {
                TargetStatus.FAR: [],
                TargetStatus.MET: [],
                TargetStatus.NEAR: [],
            }
            highlighted_officers: List[OfficerMetricEntity] = []

            for entity in entities:
                target_status = entity.target_status
                rate = entity.rate

                if (
                    target_status == TargetStatus.FAR
                    and entity.supervisor_external_id
                    == supervision_officer_supervisor_id
                ):
                    highlighted_officers.append(entity)
                else:
                    other_officer_rates[target_status].append(rate)

            if highlighted_officers:
                info = OutlierMetricInfo(
                    metric=metric,
                    target=metric_context.target,
                    other_officers=other_officer_rates,
                    highlighted_officers=highlighted_officers,
                    target_status_strategy=metric_context.target_status_strategy,
                )
                metrics_results.append(info)
            else:
                # If there are no officers with "FAR" for a given metric in the unit, do not include the data
                # for the metric in the result.
                metrics_without_outliers.append(metric)

        return metrics_results, metrics_without_outliers

    def _get_metric_context_from_db(
        self,
        session: Session,
        metric: OutliersMetricConfig,
        end_date: date,
        prev_end_date: date,
    ) -> MetricContext:
        """
        For the given metric_id, calculate and hydrate the MetricContext object as defined above.
        """
        metric_id = metric.name

        target = self.get_state_target_from_db(session, metric_id, end_date)

        officer_metric = aliased(SupervisionOfficerMetric)
        avg_pop_metric = aliased(SupervisionOfficerMetric)

        # Get officer metrics for the given metric_id for the current and previous period
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
                SupervisionOfficer.supervisor_external_id,
            )
            .all()
        )

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

        target_status_strategy = (
            TargetStatusStrategy.ZERO_RATE
            if target - iqr <= 0 and metric.outcome_type == MetricOutcome.FAVORABLE
            else TargetStatusStrategy.IQR_THRESHOLD
        )

        # Generate the OfficerMetricEntity object for all officers
        entities: List[OfficerMetricEntity] = []
        for officer_metric_record in current_period_officer_metrics:
            rate = (
                officer_metric_record.metric_value
                / officer_metric_record.avg_daily_population
            )
            target_status = self.get_target_status(
                metric.outcome_type, rate, target, iqr, target_status_strategy
            )

            prev_period_record = [
                past_officer_metric_record
                for past_officer_metric_record in previous_period_officer_metrics
                if past_officer_metric_record.officer_id
                == officer_metric_record.officer_id
            ]

            if len(prev_period_record) > 1:
                raise ValueError(
                    f"Expected at most one entry for the previous period, but got {len(prev_period_record)}"
                    f"for officer_id: {officer_metric_record.officer_id} and {metric_id} metric"
                )

            prev_rate = (
                (
                    prev_period_record[0].metric_value
                    / prev_period_record[0].avg_daily_population
                )
                if len(prev_period_record) == 1
                else None
            )

            entities.append(
                OfficerMetricEntity(
                    name=officer_metric_record.full_name,
                    rate=rate,
                    target_status=target_status,
                    prev_rate=prev_rate,
                    supervisor_external_id=officer_metric_record.supervisor_external_id,
                )
            )

        return MetricContext(
            target=target,
            entities=entities,
            target_status_strategy=target_status_strategy,
        )

    @staticmethod
    def get_target_status(
        metric_outcome_type: MetricOutcome,
        rate: float,
        target: float,
        iqr: float,
        target_status_strategy: TargetStatusStrategy,
    ) -> TargetStatus:

        if target_status_strategy == TargetStatusStrategy.ZERO_RATE and rate == 0.0:
            return TargetStatus.FAR

        if metric_outcome_type == MetricOutcome.FAVORABLE:
            if rate >= target:
                return TargetStatus.MET
            if target - rate <= iqr:
                return TargetStatus.NEAR
            return TargetStatus.FAR

        # Otherwise, MetricOutcome is ADVERSE
        if rate <= target:
            return TargetStatus.MET
        if rate - target <= iqr:
            return TargetStatus.NEAR
        return TargetStatus.FAR

    @staticmethod
    def get_state_metrics(state_code: StateCode) -> List[OutliersMetricConfig]:
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
