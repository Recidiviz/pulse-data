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
from typing import Dict, List, Optional, Tuple

from dateutil.relativedelta import relativedelta
from sqlalchemy import and_, func
from sqlalchemy.dialects.postgresql import aggregate_order_by
from sqlalchemy.orm import Session, aliased

from recidiviz.aggregated_metrics.metric_time_periods import MetricTimePeriod
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
    TargetStatus,
    TargetStatusStrategy,
)
from recidiviz.persistence.database.schema.outliers.schema import (
    MetricBenchmark,
    SupervisionDistrictManager,
    SupervisionOfficer,
    SupervisionOfficerOutlierStatus,
    SupervisionOfficerSupervisor,
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

            state_config = self.get_outliers_config(state_code)

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

    @staticmethod
    def get_outliers_config(state_code: StateCode) -> OutliersConfig:
        return OUTLIERS_CONFIGS_BY_STATE[state_code]

    def get_supervisors_with_outliers(
        self, state_code: StateCode
    ) -> List[SupervisionOfficerSupervisor]:
        """
        Return a list of SupervisionOfficerSupervisors who have outliers in the latest period.
        """
        db_key = SQLAlchemyDatabaseKey(
            SchemaType.OUTLIERS, db_name=state_code.value.lower()
        )

        with SessionFactory.using_database(db_key, autocommit=False) as session:
            latest_period_end_date = (
                session.query(func.max(SupervisionOfficerOutlierStatus.end_date))
                .filter(
                    SupervisionOfficerOutlierStatus.period
                    == MetricTimePeriod.YEAR.value,
                )
                .scalar()
            )

            # Get external ids of officers with outliers
            supervisor_ids_with_outliers = [
                external_id
                for external_id, report_data in self.get_officer_level_report_data_for_all_officer_supervisors(
                    state_code, latest_period_end_date
                ).items()
                if report_data.metrics
            ]

            supervisor_entities = (
                session.query(SupervisionOfficerSupervisor)
                .filter(
                    SupervisionOfficerSupervisor.external_id.in_(
                        supervisor_ids_with_outliers
                    )
                )
                .all()
            )
            return supervisor_entities

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
        state_code: StateCode,
        supervisor_external_id: str,
        num_lookback_periods: Optional[int],
        period_end_date: Optional[date] = None,
    ) -> List[SupervisionOfficerEntity]:
        """
        Returns a list of SupervisionOfficerEntity objects that represent the supervisor's officers and
        includes information on the state's metrics that the officer is an outlier for.

        :param state_code: The user's state code
        :param supervisor_external_id: The external id of the supervisor to get outlier information for
        :param num_lookback_periods: The number of previous periods to get statuses for, prior to the period with end_date == period_end_date.
        :param period_end_date: The end date of the period to get Outliers for. If not provided, use the latest end date available.
        :rtype: List[SupervisionOfficerEntity]
        """
        db_key = SQLAlchemyDatabaseKey(
            SchemaType.OUTLIERS, db_name=state_code.value.lower()
        )

        num_lookback_periods = num_lookback_periods or DEFAULT_NUM_LOOKBACK_PERIODS

        with SessionFactory.using_database(db_key, autocommit=False) as session:
            end_date = (
                self._get_latest_period_end_date(session)
                if period_end_date is None
                else period_end_date
            )

            earliest_end_date = end_date - relativedelta(months=num_lookback_periods)

            officer_status_records = (
                session.query(SupervisionOfficer)
                .join(
                    SupervisionOfficerOutlierStatus,
                    SupervisionOfficer.external_id
                    == SupervisionOfficerOutlierStatus.officer_id,
                )
                .filter(
                    # Get officers who are supervised by this supervisor
                    SupervisionOfficer.supervisor_external_id == supervisor_external_id,
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
                .all()
            )

            officer_external_id_to_entity: Dict[str, SupervisionOfficerEntity] = {}

            for record in officer_status_records:
                officer_external_id = record.external_id

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
                            officer_external_id
                        ].outlier_metrics.append(
                            {
                                "metric_id": record.metric_id,
                                "statuses_over_time": record.statuses_over_time,
                            }
                        )
                else:
                    officer_external_id_to_entity[
                        officer_external_id
                    ] = SupervisionOfficerEntity(
                        full_name=PersonName(**record.full_name),
                        external_id=officer_external_id,
                        pseudonymized_id=record.pseudonymized_id,
                        supervisor_external_id=supervisor_external_id,
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

            return list(officer_external_id_to_entity.values())

    @staticmethod
    def get_supervisor_from_pseudonymized_id(
        state_code: StateCode, supervisor_pseudonymized_id: str
    ) -> Optional[SupervisionOfficerSupervisor]:
        """
        Returns the SupervisionOfficerSupervisor given the supervisor_pseudonymized_id.
        """
        db_key = SQLAlchemyDatabaseKey(
            SchemaType.OUTLIERS, db_name=state_code.value.lower()
        )

        with SessionFactory.using_database(db_key, autocommit=False) as session:
            supervisor = (
                session.query(SupervisionOfficerSupervisor)
                .filter(
                    SupervisionOfficerSupervisor.pseudonymized_id
                    == supervisor_pseudonymized_id
                )
                .scalar()
            )

            return supervisor

    def get_benchmarks(
        self,
        state_code: StateCode,
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

        :param state_code: The user's state code
        :param num_lookback_periods: The number of previous periods to get benchmark data for, prior to the period with end_date == period_end_date.
        :param period_end_date: The end date of the period to get  for. If not provided, use the latest end date available.
        :rtype: List of dictionary
        """
        db_key = SQLAlchemyDatabaseKey(
            SchemaType.OUTLIERS, db_name=state_code.value.lower()
        )

        num_lookback_periods = num_lookback_periods or DEFAULT_NUM_LOOKBACK_PERIODS

        with SessionFactory.using_database(db_key, autocommit=False) as session:
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
                    func.array_agg(SupervisionOfficerOutlierStatus.metric_rate).label(
                        "rates"
                    ),
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
