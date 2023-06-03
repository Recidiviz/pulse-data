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
from typing import Dict, List, Optional

import attr
import numpy as np
from dateutil.relativedelta import relativedelta
from sqlalchemy import and_
from sqlalchemy.orm import Session, aliased

from recidiviz.aggregated_metrics.metric_time_periods import MetricTimePeriod
from recidiviz.calculator.query.state.views.outliers.supervision_metrics_helpers import (
    OUTLIERS_CONFIGS_BY_STATE,
)
from recidiviz.common.constants.states import StateCode
from recidiviz.persistence.database.schema.outliers.schema import (
    SupervisionOfficer,
    SupervisionOfficerMetric,
    SupervisionOfficerSupervisor,
    SupervisionStateMetric,
    SupervisionUnit,
)
from recidiviz.persistence.database.schema_type import SchemaType
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
    # The rate for the prior YEAR period for this unit of analysis;
    # None if there is no metric rate for the previous period
    prev_rate: Optional[float] = attr.ib()
    # The location_external_id for this entity
    location_external_id: str = attr.ib()


@attr.s
class MetricContext:
    # Unless otherwise specified, the target is the state average for the current period
    target: float = attr.ib()
    # All units of analysis for a given state and metric
    entities: List[Entity] = attr.ib()


@attr.s
class MetricInfo:
    # Unless otherwise specified, the target is the state average for the current period
    target: float = attr.ib()
    # Maps target status to a list of metric rates for all officers not included in unit_officers
    other_officers: Dict[TargetStatus, List[float]] = attr.ib()
    # Officers in a given unit who have the "FAR" status for a given metric
    unit_officers: List[Entity] = attr.ib()


@attr.s
class OutliersReportData:
    # Metric_ids mapped to MetricInfo objects, where there are officers with "FAR" status in this unit for the metrics
    metrics: Dict[str, MetricInfo] = attr.ib()
    # List of metric_ids where there are no outliers, i.e. officers with "FAR" status in this unit
    metrics_without_outliers: List[str] = attr.ib()


class OutliersQuerier:
    """Implements Querier abstractions for Outliers data sources"""

    def get_officer_level_report_data_for_all_units(
        self, state_code: StateCode, end_date: date
    ) -> dict:
        """
        Returns officer-level data by unit for all units in a state in the following format:
        {
            < supervision_unit_id >: {
                metrics: {
                    < metric id >: MetricInfo(
                        target: float,
                        other_officers: {
                            TargetStatus.MET: float[],
                            TargetStatus.NEAR: float[],
                            TargetStatus.FAR: float[],
                        },
                        unit_officers: List[Entity]
                    )
                },
                metrics_without_outliers: List[str]
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
        metrics_without_outliers = []

        for metric_id, metric_context in metric_id_to_metric_context.items():
            entities = metric_context.entities

            other_officer_rates: Dict[TargetStatus, List[float]] = {
                TargetStatus.FAR: [],
                TargetStatus.MET: [],
                TargetStatus.NEAR: [],
            }
            unit_officers: List[Entity] = []

            for entity in entities:
                target_status = entity.target_status
                rate = entity.rate

                if (
                    target_status == TargetStatus.FAR
                    and entity.location_external_id == unit_external_id
                ):
                    unit_officers.append(entity)
                else:
                    other_officer_rates[target_status].append(rate)

            if unit_officers:
                info = MetricInfo(
                    target=metric_context.target,
                    other_officers=other_officer_rates,
                    unit_officers=unit_officers,
                )
                metrics_results[metric_id] = info
            else:
                # If there are no officers with "FAR" for a given metric in the unit, do not include the data
                # for the metric in the result.
                metrics_without_outliers.append(metric_id)

        return OutliersReportData(
            metrics=metrics_results, metrics_without_outliers=metrics_without_outliers
        )

    def _get_metric_context_from_db(
        self, session: Session, metric_id: str, end_date: date, prev_end_date: date
    ) -> MetricContext:
        """
        For the given metric_id, calculate and hydrate the MetricContext object as defined above.
        """

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
                SupervisionOfficer.location_external_id,
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

        # Generate the Entity object for all officers
        entities: List[Entity] = []
        for officer_metric_record in current_period_officer_metrics:
            rate = (
                officer_metric_record.metric_value
                / officer_metric_record.avg_daily_population
            )
            target_status = self.get_target_status(metric_id, rate, target, iqr)

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
                Entity(
                    name=officer_metric_record.full_name,
                    rate=rate,
                    target_status=target_status,
                    prev_rate=prev_rate,
                    location_external_id=officer_metric_record.location_external_id,
                )
            )

        return MetricContext(target=target, entities=entities)

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

    @staticmethod
    def get_unit_id_to_supervision_officer_supervisor_email(
        state_code: StateCode,
    ) -> Dict[str, str]:
        """Returns a dictionary mapping a unit_id to the corresponding officer supervisor"""
        db_key = SQLAlchemyDatabaseKey(
            SchemaType.OUTLIERS, db_name=state_code.value.lower()
        )
        with SessionFactory.using_database(db_key, autocommit=False) as session:
            unit_to_supervisor_records = session.query(
                SupervisionUnit, SupervisionOfficerSupervisor
            ).join(
                SupervisionOfficerSupervisor,
                SupervisionUnit.external_id
                == SupervisionOfficerSupervisor.location_external_id,
            )
            return {
                record.SupervisionUnit.external_id: record.SupervisionOfficerSupervisor.email
                for record in unit_to_supervisor_records
            }
