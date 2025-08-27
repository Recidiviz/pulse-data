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

from recidiviz.aggregated_metrics.metric_time_period_config import MetricTimePeriod
from recidiviz.calculator.query.state.views.analyst_data.insights_caseload_category_sessions import (
    InsightsCaseloadCategoryType,
)
from recidiviz.calculator.query.state.views.outliers.outliers_enabled_states import (
    get_outliers_enabled_states,
)
from recidiviz.case_triage.outliers.user_context import UserContext
from recidiviz.common.constants.states import StateCode
from recidiviz.outliers.constants import DEFAULT_NUM_LOOKBACK_PERIODS
from recidiviz.outliers.outliers_configs import get_outliers_backend_config
from recidiviz.outliers.types import (
    ActionStrategySurfacedEvent,
    ConfigurationStatus,
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
    SupervisionOfficerOutcomes,
    SupervisionOfficerSupervisorEntity,
    TargetStatus,
    TargetStatusStrategy,
    UserInfo,
    VitalsMetric,
)
from recidiviz.persistence.database.database_managers.state_segmented_database_manager import (
    StateSegmentedDatabaseManager,
)
from recidiviz.persistence.database.schema.insights.schema import (
    ActionStrategySurfacedEvents,
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
    feature_variants: list[str] = attr.ib()

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
                        # for the supervisor, it doesn't matter which location we choose because
                        # we only send emails for PA which has the same value for both location
                        # fields
                        func.lower(
                            SupervisionOfficerSupervisor.supervision_location_for_list_page
                        )
                        == func.lower(SupervisionDistrictManager.supervision_district),
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

            state_config = self.get_product_configuration()

            metric_name_to_metric_context = {
                metric: self._get_metric_context_from_db(
                    session,
                    metric,
                    end_date,
                    prev_end_date,
                    state_config.primary_category_type,
                )
                for metric in self.get_outcomes_metrics()
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
        category_type_to_compare: InsightsCaseloadCategoryType,
    ) -> MetricContext:
        """
        For the given metric_id, calculate and hydrate the MetricContext object as defined above.
        """
        target, target_status_strategy = self.get_target_from_db(
            session, metric, end_date, category_type_to_compare
        )

        # Get officer metrics for the given metric_id for the current and previous period
        combined_period_officer_metrics = (
            session.query(SupervisionOfficerOutlierStatus)
            .filter(
                SupervisionOfficerOutlierStatus.metric_id == metric.name,
                SupervisionOfficerOutlierStatus.end_date.in_([end_date, prev_end_date]),
                SupervisionOfficerOutlierStatus.period == MetricTimePeriod.YEAR.value,
                SupervisionOfficerOutlierStatus.category_type
                == category_type_to_compare.value,
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
        category_type_to_compare: InsightsCaseloadCategoryType,
    ) -> Tuple[float, TargetStatusStrategy]:
        target_query = session.query(MetricBenchmark).filter(
            MetricBenchmark.metric_id == metric.name,
            MetricBenchmark.end_date == end_date,
            MetricBenchmark.period == MetricTimePeriod.YEAR.value,
            MetricBenchmark.category_type == category_type_to_compare.value,
        )

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

    def get_outcomes_metrics(
        self,
    ) -> List[OutliersMetricConfig]:
        return [
            m
            for m in self.get_outliers_backend_config().metrics
            if (m.feature_variant and m.feature_variant in self.feature_variants)
            or (
                m.inverse_feature_variant
                and m.inverse_feature_variant not in self.feature_variants
            )
            or (m.feature_variant is None and m.inverse_feature_variant is None)
        ]

    def get_supervision_officer_supervisor_entities(
        self,
        pseudonymized_id: Optional[str] = None,
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
                        SupervisionOfficerOutlierStatus.caseload_category == "ALL",
                        SupervisionOfficerOutlierStatus.metric_id.in_(
                            [m.name for m in self.get_outcomes_metrics()]
                        ),
                    ),
                    # We shouldn't exclude supervisors who don't have outcomes officers
                    isouter=True,
                )
                .filter(SupervisionOfficerSupervisor.full_name.is_not(None))
                .group_by(
                    SupervisionOfficerSupervisor.external_id,
                    SupervisionOfficerSupervisor.full_name,
                    SupervisionOfficerSupervisor.supervision_location_for_list_page,
                    SupervisionOfficerSupervisor.supervision_location_for_supervisor_page,
                    SupervisionOfficerSupervisor.pseudonymized_id,
                    SupervisionOfficerSupervisor.email,
                )
                .with_entities(
                    SupervisionOfficerSupervisor.external_id,
                    SupervisionOfficerSupervisor.full_name,
                    SupervisionOfficerSupervisor.supervision_location_for_list_page,
                    SupervisionOfficerSupervisor.supervision_location_for_supervisor_page,
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
                    supervision_location_for_list_page=record.supervision_location_for_list_page,
                    supervision_location_for_supervisor_page=record.supervision_location_for_supervisor_page,
                    email=record.email,
                    has_outliers=record.has_outliers,
                )
                for record in records
            ]

    @staticmethod
    def _get_latest_period_end_date(
        session: Session, daily_metric: bool = False
    ) -> date:
        """
        Returns the relevant most recent period end date - for either the typical
        first-of-the-month outcomes metric period OR the daily rolling metric period
        (e.g. for zero grant metric usage)
        """
        if daily_metric:
            end_date = (
                session.query(func.max(SupervisionOfficerMetric.end_date))
                .filter(
                    # filter for a zero grants metric
                    SupervisionOfficerMetric.metric_id
                    == "prop_period_with_critical_caseload"
                )
                .scalar()
            )
            return end_date

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
        category_type_to_compare: InsightsCaseloadCategoryType,
        include_workflows_info: bool,
        num_lookback_periods: Optional[int] = None,
    ) -> List[SupervisionOfficerEntity]:
        """
        Returns a list of SupervisionOfficerEntity objects that represent the supervisor's officers and
        includes information on the state's metrics that the officer is an outlier for.

        :param supervisor_external_id: The external id of the supervisor to get outlier information for
        :param category_type_to_compare: The category type to use to determine the officers' caseload categories
        :param num_lookback_periods: The number of previous periods to get statuses for, prior to the period with end_date == period_end_date.
        :rtype: List[SupervisionOfficerEntity]
        """
        id_to_entities = self.get_id_to_supervision_officer_entities(
            category_type_to_compare=category_type_to_compare,
            num_lookback_periods=num_lookback_periods,
            supervisor_external_id=supervisor_external_id,
            include_workflows_info=include_workflows_info,
        )
        return list(id_to_entities.values())

    def get_officer_outcomes_for_supervisor(
        self,
        supervisor_external_id: str,
        category_type_to_compare: InsightsCaseloadCategoryType,
        num_lookback_periods: Optional[int] = None,
        period_end_date: Optional[date] = None,
    ) -> List[SupervisionOfficerOutcomes]:
        """
        Returns a list of SupervisionOfficerOutcomes objects that represent the supervisor's officers outcomes
        information for the state's metrics that the officer may be an outlier for.

        :param supervisor_external_id: The external id of the supervisor to get outcomes information for
        :param category_type_to_compare: The category type to use to determine the officers' caseload categories
        :param num_lookback_periods: The number of previous periods to get statuses for, prior to the period with end_date == period_end_date.
        :param period_end_date: The end date of the period to get outcomes info for. If not provided, use the latest end date available.
        :rtype: List[SupervisionOfficerOutcomes]
        """
        id_to_entities = self.get_id_to_supervision_officer_outcomes_entities(
            category_type_to_compare=category_type_to_compare,
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
        category_type_to_compare: InsightsCaseloadCategoryType,
        num_lookback_periods: Optional[int] = 5,
        period_end_date: Optional[date] = None,
    ) -> List[dict]:
        """
        Returns a list of objects that have information for each metric and benchmark within the given caseload category type.
        Each item in the returned list adheres to the below format:
            {
              "metric_id": str,
              "caseload_category": str,
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
                    MetricBenchmark.category_type == category_type_to_compare.value,
                    MetricBenchmark.metric_id.in_(
                        [m.name for m in self.get_outcomes_metrics()]
                    ),
                )
                .group_by(
                    MetricBenchmark.metric_id,
                    MetricBenchmark.caseload_category,
                    MetricBenchmark.period,
                )
                .with_entities(
                    MetricBenchmark.metric_id,
                    MetricBenchmark.caseload_category,
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
                    SupervisionOfficerOutlierStatus.caseload_category,
                    SupervisionOfficerOutlierStatus.metric_id,
                    SupervisionOfficerOutlierStatus.period,
                    SupervisionOfficerOutlierStatus.status,
                )
                .with_entities(
                    SupervisionOfficerOutlierStatus.caseload_category,
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
                        benchmarks_cte.c.caseload_category
                        == latest_period_rates_far.c.caseload_category,
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
                        benchmarks_cte.c.caseload_category
                        == latest_period_rates_near.c.caseload_category,
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
                        benchmarks_cte.c.caseload_category
                        == latest_period_rates_met.c.caseload_category,
                        benchmarks_cte.c.metric_id
                        == latest_period_rates_met.c.metric_id,
                        benchmarks_cte.c.period == latest_period_rates_met.c.period,
                        latest_period_rates_met.c.status == "MET",
                    ),
                    isouter=True,
                )
                .with_entities(
                    benchmarks_cte.c.caseload_category,
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
                    "caseload_category": record.caseload_category,
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
            latest_event_date = period_end_date + relativedelta(months=2)

            events = (
                session.query(SupervisionClientEvent)
                .filter(
                    SupervisionClientEvent.pseudonymized_client_id
                    == pseudonymized_client_id,
                    SupervisionClientEvent.metric_id.in_(metric_ids),
                    SupervisionClientEvent.event_date <= latest_event_date,
                    SupervisionClientEvent.event_date >= earliest_event_date,
                )
                .all()
            )

            return events

    def get_supervision_officer_entity(
        self,
        pseudonymized_officer_id: str,
        category_type_to_compare: InsightsCaseloadCategoryType,
        include_workflows_info: bool,
        num_lookback_periods: Optional[int],
    ) -> Optional[SupervisionOfficerEntity]:
        """
        Get the SupervisionOfficerEntity object for the requested officer. Returns None
        if officer is not found.

        :param pseudonymized_officer_id: The pseudonymized id of the officer to get information for.
        :param category_type_to_compare: The category type to use to determine the officer's caseload category
        :param num_lookback_periods: The number of previous periods to get benchmark data for, prior to the period with end_date == period_end_date.
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
                category_type_to_compare=category_type_to_compare,
                num_lookback_periods=num_lookback_periods,
                officer_external_id=officer_external_id,
                include_workflows_info=include_workflows_info,
            )

            if officer_external_id not in id_to_entities:
                logging.info(
                    "Query for requested officer entity with external id %s was unsuccessful",
                    officer_external_id,
                )
                return None

            return id_to_entities[officer_external_id]

    def _include_in_outcomes_subquery(self) -> Any:
        """
        Returns a subquery for pulling the current include_in_outcomes values for all
        supervision officers.
        """
        with self.insights_database_session() as session:
            return (
                session.query(
                    SupervisionOfficerMetric.officer_id,
                    SupervisionOfficerMetric.metric_value.label("include_in_outcomes"),
                )
                .filter(
                    SupervisionOfficerMetric.end_date
                    == self._get_latest_period_end_date(session),
                    SupervisionOfficerMetric.metric_id == "include_in_outcomes",
                )
                .subquery()
            )

    def get_all_supervision_officers_required_info_only(
        self,
    ) -> List[SupervisionOfficerEntity]:
        """
        Retrieves all supervision officers and returns them as SupervisionOfficerEntity instances.
        This only populates the minimum needed information for each officer.

        Returns:
            List[SupervisionOfficerEntity]: A list of SupervisionOfficerEntity instances.
        """
        with self.insights_database_session() as session:
            include_in_outcomes_subquery = self._include_in_outcomes_subquery()
            officers = (
                session.query(SupervisionOfficer)
                .join(
                    include_in_outcomes_subquery,
                    include_in_outcomes_subquery.c.officer_id
                    == SupervisionOfficer.external_id,
                )
                .with_entities(
                    SupervisionOfficer.external_id,
                    SupervisionOfficer.full_name,
                    SupervisionOfficer.pseudonymized_id,
                    SupervisionOfficer.supervisor_external_id,
                    SupervisionOfficer.supervisor_external_ids,
                    SupervisionOfficer.supervision_district,
                    include_in_outcomes_subquery.c.include_in_outcomes,
                    SupervisionOfficer.email,
                    SupervisionOfficer.latest_login_date,
                )
            ).all()

            return [
                SupervisionOfficerEntity(
                    external_id=officer.external_id,
                    full_name=PersonName(**officer.full_name),
                    pseudonymized_id=officer.pseudonymized_id,
                    supervisor_external_id=officer.supervisor_external_id,
                    supervisor_external_ids=officer.supervisor_external_ids,
                    district=officer.supervision_district,
                    include_in_outcomes=bool(officer.include_in_outcomes),
                    email=officer.email,
                )
                for officer in officers
            ]

    def get_supervision_officer_outcomes(
        self,
        pseudonymized_officer_id: str,
        category_type_to_compare: InsightsCaseloadCategoryType,
        num_lookback_periods: Optional[int],
        period_end_date: Optional[date] = None,
    ) -> Optional[SupervisionOfficerOutcomes]:
        """
        Get the SupervisionOfficerOutcomes object for the requested officer, an entity that contains information on the state's metrics that the officer may be an outlier for.
        If the officer doesn't have metrics for the period, return None.

        :param pseudonymized_officer_id: The pseudonymized id of the officer to get information for.
        :param category_type_to_compare: The category type to use to determine the officer's caseload category
        :param num_lookback_periods: The number of previous periods to get benchmark data for, prior to the period with end_date == period_end_date.
        :param period_end_date: The end date of the period to get outcomes info for. If not provided, use the latest end date available.
        :rtype: Optional[SupervisionOfficerOutcomes]
        """
        with self.insights_database_session() as session:
            # Return early if there is no officer with the given pseudo ID.
            officer_external_id = (
                session.query(SupervisionOfficer.external_id)
                .filter(
                    SupervisionOfficer.pseudonymized_id == pseudonymized_officer_id,
                )
                .scalar()
            )

            if officer_external_id is None:
                logging.info(
                    "Requested officer with provided pseudonymized_id not found: %s",
                    pseudonymized_officer_id,
                )
                return None

            id_to_entities = self.get_id_to_supervision_officer_outcomes_entities(
                category_type_to_compare=category_type_to_compare,
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
                    "Officer with external id %s does not have outcomes info for the period ending in %s",
                    officer_external_id,
                    end_date_str,
                )
                return None

            return id_to_entities[officer_external_id]

    def supervisor_exists_with_external_id(self, external_id: str) -> bool:
        """
        Returns whether there exists a SupervisionOfficerSupervisor with the given external_id.
        """
        with self.insights_database_session() as session:
            supervisor = (
                session.query(SupervisionOfficerSupervisor.external_id)
                .filter(SupervisionOfficerSupervisor.external_id == external_id)
                .scalar()
            )

            return supervisor is not None

    def get_supervision_officer_from_pseudonymized_id(
        self, pseudonymized_id: str
    ) -> Optional[SupervisionOfficer]:
        """
        Returns the SupervisionOfficer given the pseudonymized_id.
        """
        with self.insights_database_session() as session:
            supervisor = (
                session.query(SupervisionOfficer)
                .filter(SupervisionOfficer.pseudonymized_id == pseudonymized_id)
                .scalar()
            )

            return supervisor

    def get_supervision_officers_by_external_ids(
        self, external_ids: List[str]
    ) -> List[SupervisionOfficer]:
        """Retrieve supervision officers by their external IDs."""
        return (
            self.insights_database_session()
            .query(SupervisionOfficer)
            .filter(SupervisionOfficer.external_id.in_(external_ids))
            .all()
        )

    def get_supervision_officer_supervisors_by_external_ids(
        self, external_ids: List[str]
    ) -> List[SupervisionOfficerSupervisor]:
        """Retrieve supervisors for supervision officers by their external IDs."""
        return (
            self.insights_database_session()
            .query(SupervisionOfficerSupervisor)
            .filter(SupervisionOfficerSupervisor.external_id.in_(external_ids))
            .all()
        )

    def _init_vitals_metrics_dict_for_query(self) -> Dict[str, VitalsMetric]:
        return {
            metric.metric_id: VitalsMetric(metric_id=metric.metric_id)
            for metric in self.get_outliers_backend_config().vitals_metrics
        }

    def _get_vitals_metrics_from_officer_pseudonymized_ids(
        self, supervision_officer_pseudonymized_ids: Optional[List[str]]
    ) -> List[VitalsMetric]:
        """
        Retrieve vitals metrics for officers by officer or supervisor ID.

        :param officer_id: List[str] pseudonymized ids for the officers vitals metrics will be retrieved for.
                                     If not included, all officers' metrics will be retrieved.
        :return: List of vitals metrics entities
        """

        # FETCHING METRIC IDS

        # Extract metric IDs from configuration

        vitals_metrics_by_metric_id = self._init_vitals_metrics_dict_for_query()

        TODAY = datetime.today().date()
        # A date that is within the current period and after the end of the previous period.
        # The "current period" is the past 30 days if `period` is DAY
        # or the span of time between the 1st of last month and the 1st of this month
        # if `period` is MONTH.
        # TODO(#45008): Consider cleaning up query so these cutoffs aren't necessary
        DAY_PERIOD_DATE_CUTOFF = TODAY - relativedelta(days=30)
        MONTH_PERIOD_DATE_CUTOFF = TODAY - relativedelta(months=1)

        with self.insights_database_session() as session:
            # Subquery: Get the current metric value and (if it exists) previous metric
            # value for each officer, filtering by the provided pseudonymized IDs and
            # vitals metrics IDs.
            #
            # NOTE: We assume that there will be up to two calculated metric values,
            # and that those values will represent the most recent two periods.
            # In other words, we assume there are up to two rows per officer ID and metric ID
            # combination in the source table, where the end_dates are:
            # - for metrics calculated with a period of DAY, such as assessments,
            #   1) the day before the current date and
            #   2) the day 30 days before the current date
            # - for metrics calculated with a period of MONTH, such as timely_contact_due_date_based,
            #   1) the 1st of the current month and
            #   2) the 1st of the previous month
            # (where "the current date" is the date the aggregated metrics collection was last refreshed.)
            #
            # https://github.com/Recidiviz/pulse-data/pull/35760#discussion_r1882412306
            filtered_officers_and_metrics = (
                session.query(
                    SupervisionOfficerMetric.metric_id,
                    SupervisionOfficerMetric.metric_value,
                    SupervisionOfficerMetric.period,
                    SupervisionOfficerMetric.end_date,
                    SupervisionOfficer.pseudonymized_id,
                    func.lag(SupervisionOfficerMetric.metric_value)
                    .over(
                        partition_by=[
                            SupervisionOfficerMetric.metric_id,
                            SupervisionOfficerMetric.officer_id,
                        ],
                        order_by=SupervisionOfficerMetric.end_date,
                    )
                    .label("previous_metric_value"),
                    func.lag(SupervisionOfficerMetric.end_date)
                    .over(
                        partition_by=[
                            SupervisionOfficerMetric.metric_id,
                            SupervisionOfficerMetric.officer_id,
                        ],
                        order_by=SupervisionOfficerMetric.end_date,
                    )
                    .label("previous_end_date"),
                )
                .join(
                    SupervisionOfficer,
                    SupervisionOfficerMetric.officer_id
                    == SupervisionOfficer.external_id,
                )
                .filter(
                    SupervisionOfficerMetric.metric_id.in_(vitals_metrics_by_metric_id),
                    (
                        SupervisionOfficer.pseudonymized_id.in_(
                            supervision_officer_pseudonymized_ids
                        )
                        if supervision_officer_pseudonymized_ids is not None
                        else True
                    ),
                )
                .subquery()
            )

            # Query: Get the pseudo ID, metric ID, current period metric value,
            # and the delta between the current and previous period metric values as
            # metric_30d_delta.
            #
            # NOTE: This will only calculate a difference for the provided
            # pseudonymized IDs that have a metric value for the current period.
            vitals_metrics_for_officers_query = session.query(
                filtered_officers_and_metrics.c.pseudonymized_id,
                filtered_officers_and_metrics.c.metric_id,
                filtered_officers_and_metrics.c.metric_value,
                filtered_officers_and_metrics.c.end_date,
                filtered_officers_and_metrics.c.previous_end_date,
                func.round(
                    func.coalesce(
                        filtered_officers_and_metrics.c.metric_value
                        - filtered_officers_and_metrics.c.previous_metric_value,
                        0,
                    ).label("metric_30d_delta")
                ),
            ).filter(
                filtered_officers_and_metrics.c.metric_value.isnot(None),
                case(
                    (
                        filtered_officers_and_metrics.c.period == "DAY",
                        filtered_officers_and_metrics.c.end_date
                        > DAY_PERIOD_DATE_CUTOFF,
                    ),
                    (
                        filtered_officers_and_metrics.c.period == "MONTH",
                        filtered_officers_and_metrics.c.end_date
                        > MONTH_PERIOD_DATE_CUTOFF,
                    ),
                    else_=False,
                ),
            )

            for (
                pseudonymized_id,
                metric_id,
                metric_value,
                end_date,
                previous_end_date,
                metric_30d_delta,
            ) in vitals_metrics_for_officers_query:
                vitals_metrics_by_metric_id[metric_id].add_officer_vitals_entity(
                    pseudonymized_id,
                    metric_value,
                    metric_30d_delta,
                    end_date,
                    previous_end_date,
                )

        return sorted(vitals_metrics_by_metric_id.values())

    def get_vitals_metrics_for_supervision_officer(
        self,
        officer_pseudonymized_id: str,
    ) -> List[VitalsMetric]:
        """
        Retrieve vitals metrics for officers by officer pseudo ID.
        """

        return self._get_vitals_metrics_from_officer_pseudonymized_ids(
            supervision_officer_pseudonymized_ids=[officer_pseudonymized_id]
        )

    def get_vitals_metrics_for_supervision_officer_supervisor(
        self,
        supervisor_pseudonymized_id: str,
        can_access_all_supervisors: Optional[bool] = False,
    ) -> List[VitalsMetric]:
        """
        Retrieve vitals metrics for officers by supervisor pseudo ID.
        """

        supervisor: SupervisionOfficerSupervisorEntity | None = (
            self.get_supervisor_entity_from_pseudonymized_id(
                supervisor_pseudonymized_id
            )
        )

        if supervisor is None:
            raise ValueError("Supervisor with given pseudonymized id not found.")

        with self.insights_database_session() as session:
            officer_pseudonymized_ids_filter = None

            if not can_access_all_supervisors:
                officer_pseudonymized_ids_filter = [
                    row[0]
                    for row in session.query(SupervisionOfficer.pseudonymized_id)
                    .filter(
                        SupervisionOfficer.supervisor_external_ids.any(
                            supervisor.external_id
                        )
                    )
                    .all()
                ]

                if not officer_pseudonymized_ids_filter:
                    return sorted(self._init_vitals_metrics_dict_for_query().values())

            return self._get_vitals_metrics_from_officer_pseudonymized_ids(
                supervision_officer_pseudonymized_ids=officer_pseudonymized_ids_filter
            )

    def get_id_to_supervision_officer_entities(
        self,
        category_type_to_compare: InsightsCaseloadCategoryType,
        include_workflows_info: bool,
        num_lookback_periods: Optional[int],
        officer_external_id: Optional[str] = None,
        supervisor_external_id: Optional[str] = None,
    ) -> Dict[str, SupervisionOfficerEntity]:
        """
        Returns a dictionary of officer external id to SupervisionOfficerEntity objects.

        :param category_type_to_compare: The category type to use to determine the officers' caseload categories
        :param num_lookback_periods: The number of previous periods to get statuses for, prior to the period with end_date == period_end_date.
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
            zero_grants_metrics_end_date = self._get_latest_period_end_date(
                session, daily_metric=True
            )

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
                .filter(
                    SupervisionOfficerMetric.metric_id == "avg_daily_population",
                    SupervisionOfficerMetric.category_type
                    == category_type_to_compare.value,
                )
                .group_by(SupervisionOfficerMetric.officer_id)
                .subquery()
            )

            include_in_outcomes_subquery = self._include_in_outcomes_subquery()

            # The entities we'll be selecting from our query
            query_entities = [
                SupervisionOfficer.external_id,
                SupervisionOfficer.full_name,
                SupervisionOfficer.pseudonymized_id,
                SupervisionOfficer.supervisor_external_id,
                SupervisionOfficer.supervisor_external_ids,
                SupervisionOfficer.supervision_district,
                SupervisionOfficer.email,
                SupervisionOfficer.earliest_person_assignment_date,
                SupervisionOfficer.latest_login_date,
                avgs_subquery.c.avg_daily_population,
                include_in_outcomes_subquery.c.include_in_outcomes,
            ]

            officer_status_query = (
                session.query(SupervisionOfficer)
                .join(
                    avgs_subquery,
                    avgs_subquery.c.officer_id == SupervisionOfficer.external_id,
                )
                .join(
                    include_in_outcomes_subquery,
                    include_in_outcomes_subquery.c.officer_id
                    == SupervisionOfficer.external_id,
                )
                .group_by(
                    SupervisionOfficer.external_id,
                    SupervisionOfficer.full_name,
                    SupervisionOfficer.pseudonymized_id,
                    SupervisionOfficer.supervisor_external_id,
                    SupervisionOfficer.supervisor_external_ids,
                    SupervisionOfficer.supervision_district,
                    SupervisionOfficer.email,
                    SupervisionOfficer.earliest_person_assignment_date,
                    SupervisionOfficer.latest_login_date,
                    avgs_subquery.c.avg_daily_population,
                    include_in_outcomes_subquery.c.include_in_outcomes,
                )
                .with_entities(*query_entities)
            )

            # We only query workflows_info for authorized users
            if include_workflows_info:

                # Get officers who've been active all year
                active_officers_subquery = (
                    session.query(
                        SupervisionOfficerMetric.officer_id,
                    )
                    .filter(
                        # filter for officers who have had a caseload the whole year
                        SupervisionOfficerMetric.metric_id
                        == "prop_period_with_critical_caseload",
                        # Because this metric is calculated as a float value, it sometimes
                        # has a value slightly over 1.0 (e.g. 1.000002).
                        SupervisionOfficerMetric.metric_value >= 1.0,
                        SupervisionOfficerMetric.category_type == "ALL",
                        # Look at relevant end_date
                        SupervisionOfficerMetric.end_date
                        == zero_grants_metrics_end_date,
                    )
                    .subquery()
                )

                # Get zero grant opportunities list for active officers
                task_completions_subquery = (
                    session.query(
                        SupervisionOfficerMetric.officer_id,
                        func.array_agg(SupervisionOfficerMetric.metric_id).label(
                            "zero_grant_opportunities"
                        ),
                    )
                    .join(
                        active_officers_subquery,
                        active_officers_subquery.c.officer_id
                        == SupervisionOfficerMetric.officer_id,
                    )
                    .filter(
                        # Filter for completion metrics
                        SupervisionOfficerMetric.metric_id.like("task_completions%"),
                        # Filter for zero grants
                        SupervisionOfficerMetric.metric_value == 0,
                        SupervisionOfficerMetric.category_type == "ALL",
                        SupervisionOfficerMetric.end_date
                        == zero_grants_metrics_end_date,
                    )
                    .group_by(SupervisionOfficerMetric.officer_id)
                    .subquery()
                )

                query_entities.append(
                    task_completions_subquery.c.zero_grant_opportunities,
                )

                # Update the officer status query to include workflows info
                officer_status_query = (
                    officer_status_query.join(
                        task_completions_subquery,
                        SupervisionOfficer.external_id
                        == task_completions_subquery.c.officer_id,
                        # use a LEFT OUTER JOIN
                        isouter=True,
                    )
                    .group_by(
                        task_completions_subquery.c.zero_grant_opportunities,
                    )
                    .with_entities(*query_entities)
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
                officer_external_id_to_entity[external_id] = SupervisionOfficerEntity(
                    full_name=PersonName(**record.full_name),
                    external_id=record.external_id,
                    pseudonymized_id=record.pseudonymized_id,
                    supervisor_external_id=record.supervisor_external_id,
                    supervisor_external_ids=record.supervisor_external_ids,
                    district=record.supervision_district,
                    email=record.email,
                    earliest_person_assignment_date=record.earliest_person_assignment_date,
                    avg_daily_population=record.avg_daily_population,
                    zero_grant_opportunities=(
                        [
                            x.replace("task_completions_", "")
                            for x in record.zero_grant_opportunities or []
                        ]
                        if include_workflows_info
                        else None
                    ),
                    include_in_outcomes=bool(record.include_in_outcomes),
                    latest_login_date=record.latest_login_date,
                )

            return officer_external_id_to_entity

    def get_id_to_supervision_officer_outcomes_entities(
        self,
        category_type_to_compare: InsightsCaseloadCategoryType,
        num_lookback_periods: Optional[int] = None,
        period_end_date: Optional[date] = None,
        officer_external_id: Optional[str] = None,
        supervisor_external_id: Optional[str] = None,
    ) -> Dict[str, SupervisionOfficerOutcomes]:
        """
        Returns a dictionary of officer external id to SupervisionOfficerOutcomes objects that includes information
        on the state's metrics that the officer may be an outlier for or is in the top x% for.

        :param category_type_to_compare: The category type to use to determine the officers' caseload categories
        :param num_lookback_periods: The number of previous periods to get statuses for, prior to the period with end_date == period_end_date.
        :param period_end_date: The end date of the period to get outcomes info for. If not provided, use the latest end date available.
        :param officer_external_id: The external id of an officer to filter by.
        :param supervisor_external_id: The external id of the supervisor to get officer outcomes info for
        :rtype: Dict[str, SupervisionOfficerOutcomes]
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
                    SupervisionOfficerOutlierStatus.category_type
                    == category_type_to_compare.value,
                    SupervisionOfficerOutlierStatus.metric_id.in_(
                        [m.name for m in self.get_outcomes_metrics()]
                    ),
                )
                .group_by(
                    SupervisionOfficer.external_id,
                    SupervisionOfficer.pseudonymized_id,
                    SupervisionOfficerOutlierStatus.metric_id,
                )
                .with_entities(
                    SupervisionOfficer.external_id,
                    SupervisionOfficer.pseudonymized_id,
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
                                "caseload_category",
                                SupervisionOfficerOutlierStatus.caseload_category,
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

            officer_external_id_to_entity: Dict[str, SupervisionOfficerOutcomes] = {}

            for record in officer_status_records:
                external_id = record.external_id

                # Get whether or not the officer was an outlier for the period with the requested end date

                # If the officer doesn't have any status between the earliest_end_date and end_date, skip this row.
                if not record.statuses_over_time:
                    continue
                # Since statuses_over_time is sorted by end_date descending, the first item should be the latest.
                latest_period_status_obj = record.statuses_over_time[0]

                if latest_period_status_obj["end_date"] != str(end_date):
                    # If the latest status for the officer isn't the same as the requested end date, skip this row.
                    continue

                # Get the caseload category for the latest period so we can put it directly on the outcomes info entity
                latest_period_caseload_category = latest_period_status_obj[
                    "caseload_category"
                ]
                is_outlier = latest_period_status_obj["status"] == "FAR"

                # Get whether or not the officer is in the top x% of a metric for the period with the requested end date

                # If is_top_x_pct_over_time is empty, the metric is not configured to calculate top x% officers.
                if not record.is_top_x_pct_over_time:
                    is_top_x_pct = False
                else:
                    # Since is_top_x_pct_over_time is sorted by end_date descending, the first item should be the latest.
                    latest_is_top_x_pct_obj = record.is_top_x_pct_over_time[0]

                    if latest_is_top_x_pct_obj["end_date"] != str(end_date):
                        # If the officer doesn't have any is_top_x_pct between the earliest_end_date and end_date, skip this row.
                        is_top_x_pct = False
                    else:
                        is_top_x_pct = latest_is_top_x_pct_obj.get(
                            "is_top_x_pct", False
                        )

                if external_id in officer_external_id_to_entity:
                    existing_entity = officer_external_id_to_entity[external_id]
                    if is_outlier:
                        existing_entity.outlier_metrics.append(
                            {
                                "metric_id": record.metric_id,
                                "statuses_over_time": record.statuses_over_time,
                            }
                        )
                    if is_top_x_pct:
                        existing_entity.top_x_pct_metrics.append(
                            {
                                "metric_id": record.metric_id,
                                "top_x_pct": record.is_top_x_pct_over_time[0][
                                    "top_x_pct"
                                ],
                            }
                        )
                    if (
                        existing_entity.caseload_category
                        != latest_period_caseload_category
                    ):
                        raise ValueError(
                            f"Officer with pseudonymized id {existing_entity.pseudonymized_id} has multiple caseload_category values for the latest period"
                        )
                else:
                    officer_external_id_to_entity[
                        external_id
                    ] = SupervisionOfficerOutcomes(
                        external_id=record.external_id,
                        pseudonymized_id=record.pseudonymized_id,
                        caseload_category=latest_period_caseload_category,
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
        self,
        pseudonymized_id: str,
        request_json: dict,
    ) -> None:
        """Upserts a UserMetadata entity"""
        with self.insights_database_session() as session:
            upsert_stmt = (
                insert(UserMetadata)
                .values(pseudonymized_id=pseudonymized_id, **request_json)
                .on_conflict_do_update(
                    index_elements=["pseudonymized_id"],
                    set_=request_json,
                )
            )
            session.execute(upsert_stmt)
            session.commit()

    def get_supervisor_user_info(self, pseudonymized_id: str) -> UserInfo:
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
            has_dismissed_data_unavailable_note=(
                user_metadata.has_dismissed_data_unavailable_note
                if user_metadata
                else False
            ),
            has_dismissed_rate_over_100_percent_note=(
                user_metadata.has_dismissed_rate_over_100_percent_note
                if user_metadata
                else False
            ),
        )

    def get_officer_user_info(
        self,
        pseudonymized_id: str,
        include_workflows_info: bool,
        category_type_to_compare: InsightsCaseloadCategoryType,
    ) -> UserInfo:
        """
        Composes the UserInfo entity given the pseudonymized_id by reading a relevant officer
        entity (if one exists) and user metadata.
        """
        officer_entity = self.get_supervision_officer_entity(
            pseudonymized_id,
            category_type_to_compare=category_type_to_compare,
            include_workflows_info=include_workflows_info,
            num_lookback_periods=0,
        )

        return UserInfo(
            entity=officer_entity,
            role="supervision_officer" if officer_entity else None,
            has_seen_onboarding=True,
            has_dismissed_data_unavailable_note=False,
            has_dismissed_rate_over_100_percent_note=False,
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
                user_context.feature_variants if user_context else []
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

        backend_config_obj = self.get_outliers_backend_config()
        # Filter out metrics that the user doesn't have access to based on feature variants
        backend_config_obj.metrics = self.get_outcomes_metrics()
        backend_config = backend_config_obj.to_json()
        # Include the deprecated metrics in the product configuration so that the
        # frontend will handle displaying the correct metrics based on the responses
        # from other endpoints.
        deprecated_metrics = backend_config.pop("deprecated_metrics")
        backend_config["metrics"].extend(deprecated_metrics)
        # TODO(#32104): Put this logic somewhere like the admin panel or FE where feature variants are checked better
        if (
            user_context
            and "supervisorHomepageWorkflows" in user_context.feature_variants
        ):
            available_specialized_caseload_categories = backend_config.pop(
                "available_specialized_caseload_categories"
            )
            primary_category_type = backend_config["primary_category_type"]
            all_category_type_options = [
                InsightsCaseloadCategoryType.ALL.value,
                *available_specialized_caseload_categories.keys(),
            ]
            if primary_category_type not in all_category_type_options:
                raise ValueError(
                    f"Invalid product configuration: primary_category_type is {primary_category_type}, options are {','.join(all_category_type_options)}"
                )

            backend_config[
                "caseload_categories"
            ] = available_specialized_caseload_categories.get(primary_category_type, [])
        else:
            del backend_config["primary_category_type"]

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

    def get_action_strategy_surfaced_events_for_supervisor(
        self, supervisor_pseudonymized_id: str
    ) -> List[ActionStrategySurfacedEvent]:
        """
        Gets the ActionStrategySurfacedEvents for a given supervisor.
        """
        with self.insights_database_session() as session:
            supervisor_events = (
                session.query(ActionStrategySurfacedEvents)
                .filter(
                    ActionStrategySurfacedEvents.user_pseudonymized_id
                    == supervisor_pseudonymized_id
                )
                .with_entities(
                    ActionStrategySurfacedEvents.state_code,
                    ActionStrategySurfacedEvents.user_pseudonymized_id,
                    ActionStrategySurfacedEvents.officer_pseudonymized_id,
                    ActionStrategySurfacedEvents.action_strategy,
                    ActionStrategySurfacedEvents.timestamp,
                )
                .all()
            )

            return [ActionStrategySurfacedEvent(**event) for event in supervisor_events]

    def get_most_recent_action_strategy_surfaced_event_for_supervisor(
        self, supervisor_pseudonymized_id: str
    ) -> ActionStrategySurfacedEvent:
        """
        Gets the most recent ActionStrategySurfacedEvent for a given supervisor.
        """
        with self.insights_database_session() as session:
            supervisor_event = (
                session.query(ActionStrategySurfacedEvents)
                .filter(
                    ActionStrategySurfacedEvents.user_pseudonymized_id
                    == supervisor_pseudonymized_id
                )
                .with_entities(
                    ActionStrategySurfacedEvents.state_code,
                    ActionStrategySurfacedEvents.user_pseudonymized_id,
                    ActionStrategySurfacedEvents.officer_pseudonymized_id,
                    ActionStrategySurfacedEvents.action_strategy,
                    ActionStrategySurfacedEvents.timestamp,
                )
                .order_by(ActionStrategySurfacedEvents.timestamp.desc())
                .first()
            )

            return ActionStrategySurfacedEvent(**supervisor_event)

    def insert_action_strategy_surfaced_event(
        self, event: ActionStrategySurfacedEvent
    ) -> None:
        """
        Inserts an ActionStrategySurfacedEvent into the action_strategy_surfaced_events table.
        """
        with self.insights_database_session() as session:
            insert_stmt = insert(ActionStrategySurfacedEvents).values(event.to_json())
            session.execute(insert_stmt)
            session.commit()
