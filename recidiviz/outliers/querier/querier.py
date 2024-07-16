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
import logging
from copy import copy
from datetime import date, datetime
from functools import cached_property
from typing import Any, Dict, List, Optional, Tuple

import attr
import cattrs
from dateutil.relativedelta import relativedelta
from sqlalchemy import and_, case, func
from sqlalchemy.dialects.postgresql import aggregate_order_by, insert
from sqlalchemy.orm import Session, aliased, sessionmaker

from recidiviz.aggregated_metrics.metric_time_periods import MetricTimePeriod
from recidiviz.calculator.query.state.views.outliers.outliers_enabled_states import (
    get_outliers_enabled_states,
)
from recidiviz.case_triage.outliers.user_context import UserContext
from recidiviz.common.constants.states import StateCode
from recidiviz.outliers.constants import DEFAULT_NUM_LOOKBACK_PERIODS
from recidiviz.outliers.outliers_configs import get_outliers_backend_config
from recidiviz.outliers.types import (
    ConfigurationStatus,
    ExcludedSupervisionOfficerEntity,
    MetricContext,
    MetricOutcome,
    OfficerMetricEntity,
    OfficerOutlierStatus,
    OfficerSupervisorReportData,
    OutlierMetricInfo,
    OutliersBackendConfig,
    OutliersMetricConfig,
    OutliersProductConfiguration,
    PersonName,
    SupervisionOfficerEntity,
    SupervisionOfficerSupervisorEntity,
    TargetStatus,
    TargetStatusStrategy,
    UserInfo,
)
from recidiviz.persistence.database.database_managers.state_segmented_database_manager import (
    StateSegmentedDatabaseManager,
)
from recidiviz.persistence.database.schema.insights.schema import (
    Configuration,
    MetricBenchmark,
    SupervisionClientEvent,
    SupervisionClients,
    SupervisionDistrictManager,
    SupervisionOfficer,
    SupervisionOfficerMetric,
    SupervisionOfficerOutlierStatus,
    SupervisionOfficerSupervisor,
    UserMetadata,
)
from recidiviz.persistence.database.schema_type import SchemaType


@attr.s(auto_attribs=True)
class OutliersQuerier:
    """Implements Querier abstractions for Outliers data sources"""

    state_code: StateCode = attr.ib()

    insights_database_manager: StateSegmentedDatabaseManager = attr.ib(
        factory=lambda: StateSegmentedDatabaseManager(
            get_outliers_enabled_states(), SchemaType.INSIGHTS
        )
    )

    @cached_property
    def insights_database_session(self) -> sessionmaker:
        return self.insights_database_manager.get_session(self.state_code)

    def get_officer_level_report_data_for_all_officer_supervisors(
        self, end_date: date
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
        end_date = end_date.replace(day=1)
        prev_end_date = end_date - relativedelta(months=1)

        with self.insights_database_session() as session:
            officer_supervisor_records = (
                session.query(SupervisionOfficerSupervisor)
                .select_from(SupervisionOfficerSupervisor)
                # In Phase 1, DM's will be CC'd onto supervisor emails; thus, determine which DM's
                # should be additional recipients by joining on the supervision_district
                .join(
                    SupervisionDistrictManager,
                    and_(
                        SupervisionOfficerSupervisor.state_code
                        == SupervisionDistrictManager.state_code,
                        SupervisionOfficerSupervisor.supervision_district
                        == SupervisionDistrictManager.supervision_district,
                    ),
                    # Ensure to use a LEFT OUTER JOIN
                    isouter=True,
                )
                .with_entities(
                    SupervisionOfficerSupervisor.external_id,
                    SupervisionOfficerSupervisor.email,
                    func.array_agg(SupervisionDistrictManager.email).label(
                        "additional_recipients"
                    ),
                )
                .group_by(
                    SupervisionOfficerSupervisor.external_id,
                    SupervisionOfficerSupervisor.email,
                )
                .all()
            )

            state_config = self.get_outliers_backend_config()

            metric_name_to_metric_context = {
                metric: self._get_metric_context_from_db(
                    session, metric, end_date, prev_end_date
                )
                for metric in state_config.metrics
            }

            officer_supervisor_id_to_data = {}

            for (
                external_id,
                email,
                additional_recipients,
            ) in officer_supervisor_records:
                (
                    metrics,
                    metrics_with_outliers,
                ) = self._get_officer_level_data_for_officer_supervisor(
                    session,
                    external_id,
                    metric_name_to_metric_context,
                )

                officer_supervisor_id_to_data[
                    external_id
                ] = OfficerSupervisorReportData(
                    metrics=metrics,
                    metrics_without_outliers=metrics_with_outliers,
                    recipient_email_address=email,
                    # Ensure that we don't duplicate emails for the intended recipient
                    additional_recipients=[
                        recipient
                        for recipient in additional_recipients
                        if recipient != email and recipient is not None
                    ],
                )

            return officer_supervisor_id_to_data

    @staticmethod
    def _get_officer_level_data_for_officer_supervisor(
        session: Session,
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
            officer_entities = metric_context.entities

            other_officer_rates: Dict[TargetStatus, List[float]] = {
                TargetStatus.FAR: [],
                TargetStatus.MET: [],
                TargetStatus.NEAR: [],
            }
            highlighted_officers: List[OfficerMetricEntity] = []

            for officer_entity in officer_entities:
                target_status = officer_entity.target_status
                rate = officer_entity.rate

                # .one() raises an exception if there are multiple or no results, which should be unexpected here
                officer = (
                    session.query(SupervisionOfficer)
                    .filter(
                        SupervisionOfficer.external_id == officer_entity.external_id
                    )
                    .with_entities(
                        SupervisionOfficer.external_id,
                        SupervisionOfficer.full_name,
                        SupervisionOfficer.supervisor_external_id,
                        SupervisionOfficer.supervisor_external_ids,
                        SupervisionOfficer.supervision_district,
                    )
                    .one()
                )

                if (
                    target_status == TargetStatus.FAR
                    and supervision_officer_supervisor_id
                    in officer.supervisor_external_ids
                ):
                    highlighted_officers.append(
                        OfficerMetricEntity(
                            name=PersonName(**officer.full_name),
                            external_id=officer.external_id,
                            supervisor_external_id=officer.supervisor_external_id,
                            supervisor_external_ids=officer.supervisor_external_ids,
                            supervision_district=officer.supervision_district,
                            rate=officer_entity.rate,
                            target_status=officer_entity.target_status,
                            prev_rate=officer_entity.prev_rate,
                            prev_target_status=officer_entity.prev_target_status,
                        )
                    )
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
        target, target_status_strategy = self.get_target_from_db(
            session, metric, end_date
        )

        # Get officer metrics for the given metric_id for the current and previous period
        combined_period_officer_metrics = (
            session.query(SupervisionOfficerOutlierStatus)
            .filter(
                SupervisionOfficerOutlierStatus.metric_id == metric.name,
                SupervisionOfficerOutlierStatus.end_date.in_([end_date, prev_end_date]),
                SupervisionOfficerOutlierStatus.period == MetricTimePeriod.YEAR.value,
            )
            .with_entities(
                SupervisionOfficerOutlierStatus.officer_id,
                SupervisionOfficerOutlierStatus.metric_id,
                SupervisionOfficerOutlierStatus.end_date,
                SupervisionOfficerOutlierStatus.metric_rate,
                SupervisionOfficerOutlierStatus.status,
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

        # Generate the OfficerOutlierStatus object for all officers
        entities: List[OfficerOutlierStatus] = []
        for officer_metric_record in current_period_officer_metrics:
            rate = officer_metric_record.metric_rate
            target_status = officer_metric_record.status

            prev_period_record = [
                past_officer_metric_record
                for past_officer_metric_record in previous_period_officer_metrics
                if past_officer_metric_record.officer_id
                == officer_metric_record.officer_id
            ]

            if len(prev_period_record) > 1:
                raise ValueError(
                    f"Expected at most one entry for the previous period, but got {len(prev_period_record)}"
                    f"for officer_id: {officer_metric_record.officer_id} and {metric.name} metric"
                )

            prev_rate = (
                prev_period_record[0].metric_rate
                if len(prev_period_record) == 1
                else None
            )

            prev_target_status = prev_period_record[0].status if prev_rate else None

            entities.append(
                OfficerOutlierStatus(
                    external_id=officer_metric_record.officer_id,
                    rate=rate,
                    target_status=TargetStatus(target_status),
                    prev_rate=prev_rate,
                    prev_target_status=(
                        TargetStatus(prev_target_status) if prev_target_status else None
                    ),
                )
            )

        return MetricContext(
            target=target,
            entities=entities,
            target_status_strategy=target_status_strategy,
        )

    @staticmethod
    def get_target_from_db(
        session: Session,
        metric: OutliersMetricConfig,
        end_date: date,
        caseload_type: Optional[str] = None,
    ) -> Tuple[float, TargetStatusStrategy]:
        target_query = session.query(MetricBenchmark).filter(
            MetricBenchmark.metric_id == metric.name,
            MetricBenchmark.end_date == end_date,
            MetricBenchmark.period == MetricTimePeriod.YEAR.value,
        )

        if caseload_type:
            target_query.filter(MetricBenchmark.caseload_type == caseload_type)
        else:
            target_query.filter(MetricBenchmark.caseload_type is None)

        metric_benchmark = target_query.scalar()
        target = metric_benchmark.target

        target_status_strategy = (
            TargetStatusStrategy.ZERO_RATE
            if target - metric_benchmark.threshold <= 0
            and metric.outcome_type == MetricOutcome.FAVORABLE
            else TargetStatusStrategy.IQR_THRESHOLD
        )

        return metric_benchmark.target, target_status_strategy

    def get_outliers_backend_config(self) -> OutliersBackendConfig:
        return get_outliers_backend_config(self.state_code.value)

    def get_supervision_officer_supervisor_entities(
        self, pseudonymized_id: Optional[str] = None
    ) -> List[SupervisionOfficerSupervisorEntity]:
        """
        Returns a list of SupervisionOfficerSupervisorEntity objects that indicate whether supervisors are supervising
        officers that are outliers compared to statewide benchmarks in the latest period.

        :param pseudonymized_id: The pseudonymized id to filter supervisors by. If not provided, get all supervisor entities.
        :rtype: List of SupervisionOfficerSupervisorEntity
        """
        with self.insights_database_session() as session:
            end_date = self._get_latest_period_end_date(session)

            officers_subquery = session.query(
                SupervisionOfficer.external_id,
                func.unnest(SupervisionOfficer.supervisor_external_ids).label(
                    "supervisor_external_id"
                ),
            ).subquery()

            supervisors_query = (
                session.query(SupervisionOfficerSupervisor)
                .join(
                    officers_subquery,
                    and_(
                        officers_subquery.c.supervisor_external_id
                        == SupervisionOfficerSupervisor.external_id
                    ),
                )
                .join(
                    SupervisionOfficerOutlierStatus,
                    and_(
                        officers_subquery.c.external_id
                        == SupervisionOfficerOutlierStatus.officer_id,
                        SupervisionOfficerOutlierStatus.end_date == end_date,
                        # TODO(#24998): Account for comparing benchmarks by caseload type
                        SupervisionOfficerOutlierStatus.caseload_type == "ALL",
                    ),
                )
                .filter(SupervisionOfficerSupervisor.full_name.is_not(None))
                .group_by(
                    SupervisionOfficerSupervisor.external_id,
                    SupervisionOfficerSupervisor.full_name,
                    SupervisionOfficerSupervisor.supervision_district,
                    SupervisionOfficerSupervisor.pseudonymized_id,
                    SupervisionOfficerSupervisor.email,
                )
                .with_entities(
                    SupervisionOfficerSupervisor.external_id,
                    SupervisionOfficerSupervisor.full_name,
                    SupervisionOfficerSupervisor.supervision_district,
                    SupervisionOfficerSupervisor.pseudonymized_id,
                    SupervisionOfficerSupervisor.email,
                    (
                        func.sum(
                            case(
                                [
                                    (
                                        SupervisionOfficerOutlierStatus.status
                                        == TargetStatus.FAR.value,
                                        1,
                                    )
                                ],
                                else_=0,
                            ),
                        )
                        > 0
                    ).label("has_outliers"),
                )
            )

            if pseudonymized_id:
                supervisors_query = supervisors_query.filter(
                    SupervisionOfficerSupervisor.pseudonymized_id == pseudonymized_id
                )

            records = supervisors_query.all()

            return [
                SupervisionOfficerSupervisorEntity(
                    full_name=PersonName(**record.full_name),
                    external_id=record.external_id,
                    pseudonymized_id=record.pseudonymized_id,
                    supervision_district=record.supervision_district,
                    email=record.email,
                    has_outliers=record.has_outliers,
                )
                for record in records
            ]

    @staticmethod
    def _get_latest_period_end_date(session: Session) -> date:
        end_date = (
            session.query(func.max(SupervisionOfficerOutlierStatus.end_date))
            .filter(
                SupervisionOfficerOutlierStatus.period == MetricTimePeriod.YEAR.value,
            )
            .scalar()
        )

        return end_date

    def get_officers_for_supervisor(
        self,
        supervisor_external_id: str,
        num_lookback_periods: Optional[int],
        period_end_date: Optional[date] = None,
    ) -> List[SupervisionOfficerEntity]:
        """
        Returns a list of SupervisionOfficerEntity objects that represent the supervisor's officers and
        includes information on the state's metrics that the officer is an outlier for.

        :param supervisor_external_id: The external id of the supervisor to get outlier information for
        :param num_lookback_periods: The number of previous periods to get statuses for, prior to the period with end_date == period_end_date.
        :param period_end_date: The end date of the period to get Outliers for. If not provided, use the latest end date available.
        :rtype: List[SupervisionOfficerEntity]
        """
        id_to_entities = self.get_id_to_supervision_officer_entities(
            num_lookback_periods=num_lookback_periods,
            period_end_date=period_end_date,
            supervisor_external_id=supervisor_external_id,
        )
        return list(id_to_entities.values())

    def get_excluded_officers_for_supervisor(
        self,
        supervisor_external_id: str,
    ) -> List[ExcludedSupervisionOfficerEntity]:
        """
        Returns a list of ExcludedSupervisionOfficerEntity objects that represent the supervisor's officers that
        are currently excluded from metric calculations (see supervision_officer_metric_exclusions clause).

        :param supervisor_external_id: The external id of the supervisor to get outlier information for
        :rtype: List[ExcludedSupervisionOfficerEntity]
        """
        with self.insights_database_session() as session:
            officer_status_query = (
                session.query(SupervisionOfficer)
                .outerjoin(
                    SupervisionOfficerMetric,
                    SupervisionOfficer.external_id
                    == SupervisionOfficerMetric.officer_id,
                )
                .filter(
                    SupervisionOfficer.supervisor_external_ids.any(
                        supervisor_external_id
                    ),
                    SupervisionOfficerMetric.officer_id.is_(None),
                )
                .with_entities(
                    SupervisionOfficer.external_id,
                    SupervisionOfficer.full_name,
                    SupervisionOfficer.pseudonymized_id,
                    SupervisionOfficer.supervisor_external_id,
                    SupervisionOfficer.supervisor_external_ids,
                    SupervisionOfficer.supervision_district,
                    SupervisionOfficer.specialized_caseload_type,
                )
            )

            officer_status_records = officer_status_query.all()

            return [
                (
                    ExcludedSupervisionOfficerEntity(
                        full_name=PersonName(**record.full_name),
                        external_id=record.external_id,
                        pseudonymized_id=record.pseudonymized_id,
                        supervisor_external_id=record.supervisor_external_id,
                        supervisor_external_ids=record.supervisor_external_ids,
                        district=record.supervision_district,
                        caseload_type=record.specialized_caseload_type,
                    )
                )
                for record in officer_status_records
            ]

    def get_supervisor_entity_from_pseudonymized_id(
        self, supervisor_pseudonymized_id: str
    ) -> Optional[SupervisionOfficerSupervisorEntity]:
        """
        Returns the SupervisionOfficerSupervisorEntity given the supervisor_pseudonymized_id.
        """
        supervisor = self.get_supervision_officer_supervisor_entities(
            supervisor_pseudonymized_id
        )
        return supervisor[0] if len(supervisor) > 0 else None

    def get_benchmarks(
        self,
        num_lookback_periods: Optional[int] = 5,
        period_end_date: Optional[date] = None,
    ) -> List[dict]:
        """
        Returns a list of objects that have information for a given metric and benchmark. Each item in the returned
        list adhere to the below format:
            {
              "metric_id": str,
              "caseload_type": str,
              "benchmarks": [  # a list of benchmark values for each period in window
                {
                  "target": float
                  "threshold": float
                  "end_date": date
                }, ...
              ]
              "latest_period_values": {  # Officer rates by status for the latest period
                "far": float[],
                "near": float[],
                "met": float[]
              }
            }

        :param num_lookback_periods: The number of previous periods to get benchmark data for, prior to the period with end_date == period_end_date.
        :param period_end_date: The end date of the period to get  for. If not provided, use the latest end date available.
        :rtype: List of dictionary
        """
        num_lookback_periods = num_lookback_periods or DEFAULT_NUM_LOOKBACK_PERIODS

        with self.insights_database_session() as session:
            end_date = (
                self._get_latest_period_end_date(session)
                if period_end_date is None
                else period_end_date
            )

            earliest_end_date = end_date - relativedelta(months=num_lookback_periods)

            benchmarks_cte = (
                session.query(MetricBenchmark)
                .filter(
                    # Get the benchmarks for all periods between requested or latest end_date and earliest end date
                    MetricBenchmark.end_date.between(earliest_end_date, end_date),
                )
                .group_by(
                    MetricBenchmark.metric_id,
                    MetricBenchmark.caseload_type,
                    MetricBenchmark.period,
                )
                .with_entities(
                    MetricBenchmark.metric_id,
                    MetricBenchmark.caseload_type,
                    MetricBenchmark.period,
                    # Get an array of JSON objects for the metric's targets and thresholds in the selected periods
                    func.array_agg(
                        aggregate_order_by(
                            func.jsonb_build_object(
                                "end_date",
                                MetricBenchmark.end_date,
                                "target",
                                MetricBenchmark.target,
                                "threshold",
                                MetricBenchmark.threshold,
                            ),
                            MetricBenchmark.end_date.desc(),
                        )
                    ).label("benchmarks_over_time"),
                )
                .cte("benchmarks")
            )

            latest_period_rates_cte = (
                session.query(SupervisionOfficerOutlierStatus)
                .filter(
                    SupervisionOfficerOutlierStatus.end_date == end_date,
                )
                .group_by(
                    SupervisionOfficerOutlierStatus.caseload_type,
                    SupervisionOfficerOutlierStatus.metric_id,
                    SupervisionOfficerOutlierStatus.period,
                    SupervisionOfficerOutlierStatus.status,
                )
                .with_entities(
                    SupervisionOfficerOutlierStatus.caseload_type,
                    SupervisionOfficerOutlierStatus.metric_id,
                    SupervisionOfficerOutlierStatus.period,
                    SupervisionOfficerOutlierStatus.status,
                    # Aggregate the rates by caseload type, metric, id, and status
                    func.array_agg(
                        aggregate_order_by(
                            SupervisionOfficerOutlierStatus.metric_rate,
                            SupervisionOfficerOutlierStatus.metric_rate.asc(),
                        )
                    ).label("rates"),
                )
                .cte("latest_period_rates")
            )

            # In order to get the aggregated arrays by metric and caseload type in a single row, join on
            # the latest period CTEs repeatedly, once per status.
            latest_period_rates_far = aliased(latest_period_rates_cte)
            latest_period_rates_near = aliased(latest_period_rates_cte)
            latest_period_rates_met = aliased(latest_period_rates_cte)

            benchmarks = (
                session.query(benchmarks_cte)
                .join(
                    latest_period_rates_far,
                    and_(
                        benchmarks_cte.c.caseload_type
                        == latest_period_rates_far.c.caseload_type,
                        benchmarks_cte.c.metric_id
                        == latest_period_rates_far.c.metric_id,
                        benchmarks_cte.c.period == latest_period_rates_far.c.period,
                        latest_period_rates_far.c.status == "FAR",
                    ),
                    isouter=True,
                )
                .join(
                    latest_period_rates_near,
                    and_(
                        benchmarks_cte.c.caseload_type
                        == latest_period_rates_near.c.caseload_type,
                        benchmarks_cte.c.metric_id
                        == latest_period_rates_near.c.metric_id,
                        benchmarks_cte.c.period == latest_period_rates_near.c.period,
                        latest_period_rates_near.c.status == "NEAR",
                    ),
                    isouter=True,
                )
                .join(
                    latest_period_rates_met,
                    and_(
                        benchmarks_cte.c.caseload_type
                        == latest_period_rates_met.c.caseload_type,
                        benchmarks_cte.c.metric_id
                        == latest_period_rates_met.c.metric_id,
                        benchmarks_cte.c.period == latest_period_rates_met.c.period,
                        latest_period_rates_met.c.status == "MET",
                    ),
                    isouter=True,
                )
                .with_entities(
                    benchmarks_cte.c.caseload_type,
                    benchmarks_cte.c.metric_id,
                    benchmarks_cte.c.benchmarks_over_time,
                    func.coalesce(latest_period_rates_far.c.rates, []).label("far"),
                    func.coalesce(latest_period_rates_near.c.rates, []).label("near"),
                    func.coalesce(latest_period_rates_met.c.rates, []).label("met"),
                )
                .all()
            )

            return [
                {
                    "metric_id": record.metric_id,
                    "caseload_type": record.caseload_type,
                    "benchmarks": record.benchmarks_over_time,
                    "latest_period_values": {
                        "far": record.far,
                        "near": record.near,
                        "met": record.met,
                    },
                }
                for record in benchmarks
            ]

    def get_events_by_officer(
        self,
        pseudonymized_officer_id: str,
        metric_ids: List[str],
        period_end_date: Optional[date] = None,
    ) -> List[SupervisionClientEvent]:
        """
        Get the list of events for the given officer and metric(s) in the 12-month period ending with the period_end_date.

        :param pseudonymized_officer_id: The pseudonymized id of the officer to get events for.
        :param metric_ids: The list of metrics to get events for.
        :param period_end_date: The end date of the period to get for. If not provided, use the latest end date available.
        :rtype: List of dictionary
        """

        with self.insights_database_session() as session:
            end_date = (
                self._get_latest_period_end_date(session)
                if period_end_date is None
                else period_end_date
            )

            # The earliest event date is the start of the YEAR time period.
            earliest_event_date = end_date - relativedelta(years=1)

            events = (
                session.query(SupervisionClientEvent)
                .filter(
                    SupervisionClientEvent.pseudonymized_officer_id
                    == pseudonymized_officer_id,
                    SupervisionClientEvent.metric_id.in_(metric_ids),
                    # The metric time periods have an exclusive end date when calculated.
                    SupervisionClientEvent.event_date < end_date,
                    SupervisionClientEvent.event_date >= earliest_event_date,
                )
                .all()
            )

            return events

    def get_events_by_client(
        self,
        pseudonymized_client_id: str,
        metric_ids: List[str],
        period_end_date: date,
    ) -> List[SupervisionClientEvent]:
        """
        Get the list of events for the given client and metric(s) in the last 12 months relative to the provided end_date.

        :param pseudonymized_client_id: The pseudonymized id of the officer to get events for.
        :param metric_ids: The list of metrics to get events for.
        :param period_end_date: The end date of the year period to get events for.
        :rtype: List of SupervisionClientEvent
        """

        with self.insights_database_session() as session:
            # The earliest event date is the start of the year time period.
            earliest_event_date = period_end_date - relativedelta(years=1)

            events = (
                session.query(SupervisionClientEvent)
                .filter(
                    SupervisionClientEvent.pseudonymized_client_id
                    == pseudonymized_client_id,
                    SupervisionClientEvent.metric_id.in_(metric_ids),
                    SupervisionClientEvent.event_date <= period_end_date,
                    SupervisionClientEvent.event_date >= earliest_event_date,
                )
                .all()
            )

            return events

    def get_supervision_officer_entity(
        self,
        pseudonymized_officer_id: str,
        num_lookback_periods: Optional[int],
        period_end_date: Optional[date] = None,
    ) -> Optional[SupervisionOfficerEntity]:
        """
        Get the SupervisionOfficerEntity object for the requested officer, an entity that includes information on the state's metrics that the officer is an outlier for.
        If the officer doesn't have metrics for the period, return None.

        :param pseudonymized_officer_id: The pseudonymized id of the officer to get information for.
        :param num_lookback_periods: The number of previous periods to get benchmark data for, prior to the period with end_date == period_end_date.
        :param period_end_date: The end date of the period to get for. If not provided, use the latest end date available.
        :rtype: Optional[SupervisionOfficerEntity]
        """
        with self.insights_database_session() as session:
            officer_external_id = (
                session.query(SupervisionOfficer.external_id)
                .filter(SupervisionOfficer.pseudonymized_id == pseudonymized_officer_id)
                .scalar()
            )

            if officer_external_id is None:
                logging.info(
                    "Requested officer with provided pseudonymized_id not found: %s",
                    pseudonymized_officer_id,
                )
                return None

            id_to_entities = self.get_id_to_supervision_officer_entities(
                num_lookback_periods=num_lookback_periods,
                period_end_date=period_end_date,
                officer_external_id=officer_external_id,
            )

            if officer_external_id not in id_to_entities:
                end_date_str = (
                    period_end_date.strftime("%Y-%m-%d")
                    if period_end_date
                    else self._get_latest_period_end_date(session).strftime("%Y-%m-%d")
                )
                logging.info(
                    "Requested officer with external id %s does not have metrics for the period ending in %s",
                    officer_external_id,
                    end_date_str,
                )
                return None

            return id_to_entities[officer_external_id]

    def get_supervisor_from_external_id(
        self, external_id: str
    ) -> Optional[SupervisionOfficerSupervisor]:
        """
        Returns the SupervisionOfficerSupervisor given the external_id.
        """
        with self.insights_database_session() as session:
            supervisor = (
                session.query(SupervisionOfficerSupervisor)
                .filter(SupervisionOfficerSupervisor.external_id == external_id)
                .scalar()
            )

            return supervisor

    def get_id_to_supervision_officer_entities(
        self,
        num_lookback_periods: Optional[int],
        period_end_date: Optional[date] = None,
        officer_external_id: Optional[str] = None,
        supervisor_external_id: Optional[str] = None,
    ) -> Dict[str, SupervisionOfficerEntity]:
        """
        Returns a dictionary of officer external id to SupervisionOfficerEntity objects that includes information
        on the state's metrics that the officer is an outlier for or is in the top x% for.

        :param num_lookback_periods: The number of previous periods to get statuses for, prior to the period with end_date == period_end_date.
        :param period_end_date: The end date of the period to get Outliers for. If not provided, use the latest end date available.
        :param officer_external_id: The external id of an officer to filter by.
        :param supervisor_external_id: The external id of the supervisor to get officer entities for
        :rtype: Dict[str, SupervisionOfficerEntity]
        """
        num_lookback_periods = (
            DEFAULT_NUM_LOOKBACK_PERIODS
            if num_lookback_periods is None
            else num_lookback_periods
        )

        if officer_external_id is None and supervisor_external_id is None:
            raise ValueError(
                "Should provide either an external id for the officer or supervisor to filter on."
            )

        with self.insights_database_session() as session:
            end_date = (
                self._get_latest_period_end_date(session)
                if period_end_date is None
                else period_end_date
            )

            earliest_end_date = end_date - relativedelta(months=num_lookback_periods)

            avgs_subquery = (
                session.query(
                    SupervisionOfficerMetric.officer_id,
                    func.array_agg(
                        aggregate_order_by(
                            SupervisionOfficerMetric.metric_value,
                            SupervisionOfficerMetric.end_date.desc(),
                        )
                    )[1].label("avg_daily_population"),
                )
                .filter(SupervisionOfficerMetric.metric_id == "avg_daily_population")
                .group_by(SupervisionOfficerMetric.officer_id)
                .subquery()
            )

            officer_status_query = (
                session.query(SupervisionOfficer)
                .join(
                    avgs_subquery,
                    avgs_subquery.c.officer_id == SupervisionOfficer.external_id,
                )
                .join(
                    SupervisionOfficerOutlierStatus,
                    SupervisionOfficer.external_id
                    == SupervisionOfficerOutlierStatus.officer_id,
                )
                .filter(
                    # Get the statuses for all periods between requested or latest end_date and earliest end date
                    SupervisionOfficerOutlierStatus.end_date.between(
                        earliest_end_date, end_date
                    ),
                )
                .group_by(
                    SupervisionOfficer.external_id,
                    SupervisionOfficer.full_name,
                    SupervisionOfficer.pseudonymized_id,
                    SupervisionOfficer.supervisor_external_id,
                    SupervisionOfficer.supervisor_external_ids,
                    SupervisionOfficer.supervision_district,
                    SupervisionOfficer.specialized_caseload_type,
                    SupervisionOfficerOutlierStatus.metric_id,
                    avgs_subquery.c.avg_daily_population,
                )
                .with_entities(
                    SupervisionOfficer.external_id,
                    SupervisionOfficer.full_name,
                    SupervisionOfficer.pseudonymized_id,
                    SupervisionOfficer.supervisor_external_id,
                    SupervisionOfficer.supervisor_external_ids,
                    SupervisionOfficer.supervision_district,
                    SupervisionOfficer.specialized_caseload_type,
                    SupervisionOfficerOutlierStatus.metric_id,
                    # Get an array of JSON objects for the officer's rates and statuses in the selected periods
                    func.array_agg(
                        aggregate_order_by(
                            func.jsonb_build_object(
                                "end_date",
                                SupervisionOfficerOutlierStatus.end_date,
                                "metric_rate",
                                SupervisionOfficerOutlierStatus.metric_rate,
                                "status",
                                SupervisionOfficerOutlierStatus.status,
                            ),
                            SupervisionOfficerOutlierStatus.end_date.desc(),
                        )
                    ).label("statuses_over_time"),
                    # Get an array of JSON objects for the officer's rates and statuses in the selected periods
                    func.array_agg(
                        aggregate_order_by(
                            func.jsonb_build_object(
                                "is_top_x_pct",
                                SupervisionOfficerOutlierStatus.is_top_x_pct,
                                "top_x_pct",
                                SupervisionOfficerOutlierStatus.top_x_pct,
                                "end_date",
                                SupervisionOfficerOutlierStatus.end_date,
                            ),
                            SupervisionOfficerOutlierStatus.end_date.desc(),
                        )
                    ).label("is_top_x_pct_over_time"),
                    avgs_subquery.c.avg_daily_population,
                )
            )

            if officer_external_id:
                # If the officer external id is specified, filter by the officer external id.
                officer_status_query = officer_status_query.filter(
                    SupervisionOfficer.external_id == officer_external_id
                )

            if supervisor_external_id:
                # If the supervisor external id is specified, filter by the supervisor external id.
                officer_status_query = officer_status_query.filter(
                    SupervisionOfficer.supervisor_external_ids.any(
                        supervisor_external_id
                    )
                )

            officer_status_records = officer_status_query.all()

            officer_external_id_to_entity: Dict[str, SupervisionOfficerEntity] = {}

            for record in officer_status_records:
                external_id = record.external_id

                # Get whether or not the officer was an outlier for the period with the requested end date
                try:
                    # Since statuses_over_time is sorted by end_date descending, the first item should be the latest.
                    latest_period_status_obj = record.statuses_over_time[0]
                except IndexError:
                    # If the officer doesn't have any status between the earliest_end_date and end_date, skip this row.
                    continue

                if latest_period_status_obj["end_date"] != str(end_date):
                    # If the latest status for the officer isn't the same as the requested end date, skip this row.
                    continue

                is_outlier = latest_period_status_obj["status"] == "FAR"

                # Get whether or not the officer is in the top x% of a metric for the period with the requested end date
                try:
                    # Since is_top_x_pct_over_time is sorted by end_date descending, the first item should be the latest.
                    latest_is_top_x_pct_obj = record.is_top_x_pct_over_time[0]

                    if latest_is_top_x_pct_obj["end_date"] != str(end_date):
                        # If the officer doesn't have any is_top_x_pct between the earliest_end_date and end_date, skip this row.
                        is_top_x_pct = False
                    else:
                        is_top_x_pct = latest_is_top_x_pct_obj.get(
                            "is_top_x_pct", False
                        )

                except IndexError:
                    # If is_top_x_pct_over_time is empty, the metric is not configured to calculate top x% officers.
                    is_top_x_pct = False

                if record.external_id in officer_external_id_to_entity:
                    if is_outlier:
                        officer_external_id_to_entity[
                            external_id
                        ].outlier_metrics.append(
                            {
                                "metric_id": record.metric_id,
                                "statuses_over_time": record.statuses_over_time,
                            }
                        )
                    if is_top_x_pct:
                        officer_external_id_to_entity[
                            external_id
                        ].top_x_pct_metrics.append(
                            {
                                "metric_id": record.metric_id,
                                "top_x_pct": record.is_top_x_pct_over_time[0][
                                    "top_x_pct"
                                ],
                            }
                        )
                else:
                    officer_external_id_to_entity[
                        external_id
                    ] = SupervisionOfficerEntity(
                        full_name=PersonName(**record.full_name),
                        external_id=record.external_id,
                        pseudonymized_id=record.pseudonymized_id,
                        supervisor_external_id=record.supervisor_external_id,
                        supervisor_external_ids=record.supervisor_external_ids,
                        district=record.supervision_district,
                        caseload_type=record.specialized_caseload_type,
                        avg_daily_population=record.avg_daily_population,
                        outlier_metrics=(
                            [
                                {
                                    "metric_id": record.metric_id,
                                    "statuses_over_time": record.statuses_over_time,
                                }
                            ]
                            if is_outlier
                            else []
                        ),
                        top_x_pct_metrics=(
                            [
                                {
                                    "metric_id": record.metric_id,
                                    "top_x_pct": record.statuses_over_time,
                                }
                            ]
                            if is_top_x_pct
                            else []
                        ),
                    )

            return officer_external_id_to_entity

    def get_client_from_pseudonymized_id(
        self, pseudonymized_id: str
    ) -> Optional[SupervisionClients]:
        """
        Returns the SupervisionClient entity given the pseudonymized_id.
        """
        with self.insights_database_session() as session:
            client = (
                session.query(SupervisionClients)
                .filter(SupervisionClients.pseudonymized_client_id == pseudonymized_id)
                .scalar()
            )

            return client

    def get_user_metadata(self, pseudonymized_id: str) -> Optional[UserMetadata]:
        """Returns the UserMetadata entity given the pseudonymized_id."""
        with self.insights_database_session() as session:
            return (
                session.query(UserMetadata)
                .filter(UserMetadata.pseudonymized_id == pseudonymized_id)
                .scalar()
            )

    def update_user_metadata(
        self, pseudonymized_id: str, has_seen_onboarding: bool
    ) -> None:
        """Upserts a UserMetadata entity"""
        with self.insights_database_session() as session:
            upsert_stmt = (
                insert(UserMetadata)
                .values(
                    pseudonymized_id=pseudonymized_id,
                    has_seen_onboarding=has_seen_onboarding,
                )
                .on_conflict_do_update(
                    index_elements=["pseudonymized_id"],
                    set_={"has_seen_onboarding": has_seen_onboarding},
                )
            )
            session.execute(upsert_stmt)
            session.commit()

    def get_user_info(self, pseudonymized_id: str) -> UserInfo:
        """
        Composes the UserInfo entity given the pseudonymized_id by reading a relevant supervisor
        entity (if one exists) and user metadata.
        """
        supervisor_entity = self.get_supervisor_entity_from_pseudonymized_id(
            pseudonymized_id
        )
        user_metadata = self.get_user_metadata(pseudonymized_id)

        return UserInfo(
            entity=supervisor_entity,
            role="supervision_officer_supervisor" if supervisor_entity else None,
            has_seen_onboarding=(
                user_metadata.has_seen_onboarding if user_metadata else False
            ),
        )

    def get_configuration_for_user(
        self, user_context: Optional[UserContext]
    ) -> Configuration:
        """
        Gets the relevant Configuration database entity for the user.

        Note: there is no UserContext object created when generating emails. Thus,
        emails will always use the latest, active Configuration object set for the
        entire state, i.e. there is no feature variant.
        """
        with self.insights_database_session() as session:
            user_feature_variants = (
                list(user_context.feature_variants.keys()) if user_context else []
            )

            # Get all active configurations
            active_configs_query = session.query(Configuration).filter(
                Configuration.status == ConfigurationStatus.ACTIVE.value,
            )

            if user_feature_variants:
                # Get all active configurations with feature variants that line up with the user's feature variants
                fv_configs = active_configs_query.filter(
                    Configuration.feature_variant.in_(user_feature_variants)
                ).all()

                if len(fv_configs) > 1:
                    # If there are multiple active configurations relevant to the user's feature
                    # variants, log an error and return the default configuration.
                    logging.error(
                        "Multiple configurations and feature variants may apply to this user, however only one should apply. The default configuration will be shown instead."
                    )
                elif len(fv_configs) == 1:
                    # If there is a single match, return the configuration
                    return fv_configs[0]

            default_config = active_configs_query.filter(
                Configuration.feature_variant.is_(None)
            ).one()

            return default_config

    def get_configurations(
        self,
    ) -> List[Configuration]:
        """
        Gets all of the Configuration database entities for the state in reverse
        time order.
        """
        with self.insights_database_session() as session:
            # Get all configurations
            configs = (
                session.query(Configuration)
                .order_by(Configuration.updated_at.desc())
                .all()
            )

            return configs

    def add_configuration(self, config_dict: Dict[str, Any]) -> Configuration:
        """
        Adds an active Configuration entity and deactivates any active entity that shares
        the feature variant of the new entity.
        """
        with self.insights_database_session() as session:
            try:
                # Deserialize the new Configuration entity
                config = Configuration(**config_dict)

                # Deactivate active configurations with the same feature variant
                feature_variant = config.feature_variant
                if feature_variant is None:
                    config_query = session.query(Configuration).filter(
                        Configuration.feature_variant.is_(None)
                    )
                else:
                    config_query = session.query(Configuration).filter(
                        Configuration.feature_variant == feature_variant
                    )

                configs_to_deactivate = config_query.filter(
                    Configuration.status == ConfigurationStatus.ACTIVE.value
                ).all()

                for c in configs_to_deactivate:
                    c.status = ConfigurationStatus.INACTIVE.value

                # Add the new Configuration object
                session.add(config)
            except:
                session.rollback()
                raise

            session.commit()
            session.refresh(config)

        return config

    def get_configuration(self, config_id: int) -> Configuration:
        """
        Gets a configuration by id.
        """
        with self.insights_database_session() as session:
            config = (
                session.query(Configuration).filter(Configuration.id == config_id).one()
            )

            return config

    def deactivate_configuration(self, config_id: int) -> None:
        """
        Deactivates the configuration with the given id. Raises an exception if the
        DB returns anything other than one entity or if the configuration to deactivate
        is the only active default (feature_variant=None) configuration.
        """

        with self.insights_database_session() as session:
            config = (
                session.query(Configuration).filter(Configuration.id == config_id).one()
            )

            # Silently log an error because we shouldn't try to deactvate inactive configurations
            if config.status == ConfigurationStatus.INACTIVE.value:
                logging.error("Configuration %s is already inactive", config_id)
                return

            if config.feature_variant is None:
                active_default_configs = (
                    session.query(Configuration)
                    .filter(
                        Configuration.feature_variant.is_(None),
                        Configuration.status == ConfigurationStatus.ACTIVE.value,
                    )
                    .all()
                )

                if len(active_default_configs) == 1:
                    raise ValueError(
                        f"Cannot deactivate the only active default configuration for {self.state_code.value}"
                    )

            config.status = ConfigurationStatus.INACTIVE.value
            session.commit()

    def get_product_configuration(
        self, user_context: Optional[UserContext] = None
    ) -> OutliersProductConfiguration:
        """
        Gets the configuration information used externally, i.e. copy, for products for
        the user from the Configuration database entity and the backend configuration.
        """
        config_dict = {}

        backend_config = self.get_outliers_backend_config().to_json()
        # Include the deprecated metrics in the product configuration so that the
        # frontend will handle displaying the correct metrics based on the responses
        # from other endpoints.
        deprecated_metrics = backend_config.pop("deprecated_metrics")
        backend_config["metrics"].extend(deprecated_metrics)
        config_dict.update(backend_config)

        user_config = self.get_configuration_for_user(user_context).to_dict()
        config_dict.update(user_config)

        # The below fields are only used internally, so omit them from the result
        config_dict.pop("status")
        config_dict.pop("id")

        # Without the below lines, I was getting StructureHandlerNotFoundError for the
        # updated_at field.
        c = cattrs.Converter()
        c.register_structure_hook(datetime, lambda dt, _: dt)

        return c.structure(config_dict, OutliersProductConfiguration)
