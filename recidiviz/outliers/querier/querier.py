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
from sqlalchemy.orm import Session

from recidiviz.aggregated_metrics.metric_time_periods import MetricTimePeriod
from recidiviz.common.constants.states import StateCode
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
