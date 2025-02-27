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
from datetime import date
from functools import cached_property
from typing import Dict, List, Optional, Tuple

import attr
from dateutil.relativedelta import relativedelta
from sqlalchemy import and_, case, func
from sqlalchemy.dialects.postgresql import aggregate_order_by
from sqlalchemy.orm import Session, aliased, sessionmaker

from recidiviz.aggregated_metrics.metric_time_periods import MetricTimePeriod
from recidiviz.calculator.query.state.views.outliers.outliers_enabled_states import (
    get_outliers_enabled_states,
)
from recidiviz.common.constants.states import StateCode
from recidiviz.outliers.constants import DEFAULT_NUM_LOOKBACK_PERIODS
from recidiviz.outliers.outliers_configs import OUTLIERS_CONFIGS_BY_STATE
from recidiviz.outliers.types import (
    MetricContext,
    MetricOutcome,
    OfficerMetricEntity,
    OfficerSupervisorReportData,
    OutlierMetricInfo,
    OutliersConfig,
    OutliersMetricConfig,
    PersonName,
    SupervisionOfficerEntity,
    SupervisionOfficerSupervisorEntity,
    TargetStatus,
    TargetStatusStrategy,
)
from recidiviz.persistence.database.database_managers.state_segmented_database_manager import (
    StateSegmentedDatabaseManager,
)
from recidiviz.persistence.database.schema.outliers.schema import (
    MetricBenchmark,
    SupervisionClientEvent,
    SupervisionDistrictManager,
    SupervisionOfficer,
    SupervisionOfficerOutlierStatus,
    SupervisionOfficerSupervisor,
)
from recidiviz.persistence.database.schema_type import SchemaType


@attr.s(auto_attribs=True)
class OutliersQuerier:
    """Implements Querier abstractions for Outliers data sources"""

    state_code: StateCode = attr.ib()
    database_manager: StateSegmentedDatabaseManager = attr.ib(
        factory=lambda: StateSegmentedDatabaseManager(
            get_outliers_enabled_states(), SchemaType.OUTLIERS
        )
    )

    @cached_property
    def database_session(self) -> sessionmaker:
        return self.database_manager.get_session(self.state_code)

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

        with self.database_session() as session:
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

            state_config = self.get_outliers_config()

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
        target, target_status_strategy = self.get_target_from_db(
            session, metric, end_date
        )

        # Get officer metrics for the given metric_id for the current and previous period
        combined_period_officer_metrics = (
            session.query(SupervisionOfficerOutlierStatus)
            .join(
                SupervisionOfficer,
                SupervisionOfficer.external_id
                == SupervisionOfficerOutlierStatus.officer_id,
            )
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
                SupervisionOfficer.full_name,
                SupervisionOfficer.supervisor_external_id,
                SupervisionOfficer.supervision_district,
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

        # Generate the OfficerMetricEntity object for all officers
        entities: List[OfficerMetricEntity] = []
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
                OfficerMetricEntity(
                    name=PersonName(**officer_metric_record.full_name),
                    rate=rate,
                    target_status=TargetStatus(target_status),
                    prev_rate=prev_rate,
                    prev_target_status=TargetStatus(prev_target_status)
                    if prev_target_status
                    else None,
                    supervisor_external_id=officer_metric_record.supervisor_external_id,
                    supervision_district=officer_metric_record.supervision_district,
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

    def get_outliers_config(self) -> OutliersConfig:
        return OUTLIERS_CONFIGS_BY_STATE[self.state_code]

    def get_supervision_officer_supervisor_entities(
        self, pseudonymized_id: Optional[str] = None
    ) -> List[SupervisionOfficerSupervisorEntity]:
        """
        Returns a list of SupervisionOfficerSupervisorEntity objects that indicate whether supervisors are supervising
        officers that are outliers compared to statewide benchmarks in the latest period.

        :param pseudonymized_id: The pseudonymized id to filter supervisors by. If not provided, get all supervisor entities.
        :rtype: List of SupervisionOfficerSupervisorEntity
        """
        with self.database_session() as session:
            end_date = self._get_latest_period_end_date(session)

            supervisors_query = (
                session.query(SupervisionOfficerSupervisor)
                .join(
                    SupervisionOfficer,
                    and_(
                        SupervisionOfficer.supervisor_external_id
                        == SupervisionOfficerSupervisor.external_id
                    ),
                )
                .join(
                    SupervisionOfficerOutlierStatus,
                    and_(
                        SupervisionOfficer.external_id
                        == SupervisionOfficerOutlierStatus.officer_id,
                        SupervisionOfficerOutlierStatus.end_date == end_date,
                        # TODO(#24998): Account for comparing benchmarks by caseload type
                        SupervisionOfficerOutlierStatus.caseload_type == "ALL",
                    ),
                )
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

        with self.database_session() as session:
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

        with self.database_session() as session:
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
        with self.database_session() as session:
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
        with self.database_session() as session:
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
        on the state's metrics that the officer is an outlier for.

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

        with self.database_session() as session:
            end_date = (
                self._get_latest_period_end_date(session)
                if period_end_date is None
                else period_end_date
            )

            earliest_end_date = end_date - relativedelta(months=num_lookback_periods)

            officer_status_query = (
                session.query(SupervisionOfficer)
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
                    SupervisionOfficer.supervision_district,
                    SupervisionOfficer.specialized_caseload_type,
                    SupervisionOfficerOutlierStatus.metric_id,
                )
                .with_entities(
                    SupervisionOfficer.external_id,
                    SupervisionOfficer.full_name,
                    SupervisionOfficer.pseudonymized_id,
                    SupervisionOfficer.supervisor_external_id,
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
                    SupervisionOfficer.supervisor_external_id == supervisor_external_id
                )

            officer_status_records = officer_status_query.all()

            officer_external_id_to_entity: Dict[str, SupervisionOfficerEntity] = {}

            for record in officer_status_records:
                external_id = record.external_id

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
                else:
                    officer_external_id_to_entity[
                        external_id
                    ] = SupervisionOfficerEntity(
                        full_name=PersonName(**record.full_name),
                        external_id=record.external_id,
                        pseudonymized_id=record.pseudonymized_id,
                        supervisor_external_id=record.supervisor_external_id,
                        district=record.supervision_district,
                        caseload_type=record.specialized_caseload_type,
                        outlier_metrics=[
                            {
                                "metric_id": record.metric_id,
                                "statuses_over_time": record.statuses_over_time,
                            }
                        ]
                        if is_outlier
                        else [],
                    )

            return officer_external_id_to_entity
