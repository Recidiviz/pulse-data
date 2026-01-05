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
"""Report context for the Outliers Supervision Officer Supervisor email.

This module can be run as a script for local development. It is intended to be run within
the admin_panel container in docker-compose. It will generate one or more HTML files that you can open 
in the browser or tool of your choice for inspection.

You can populate the template with the fixture data that is written into this file:

docker exec pulse-data-admin_panel_backend-1 uv run \
    python -m recidiviz.reporting.context.outliers_supervision_officer_supervisor.context \
    fixture

Or you can point it at your local database, if you have populated it with staging data using
recidiviz.tools.outliers.load_local_db. This option assumes your data is current as of this month.

docker exec pulse-data-admin_panel_backend-1 uv run \
    python -m recidiviz.reporting.context.outliers_supervision_officer_supervisor.context \
    db --state_code US_PA

"""
import argparse
import datetime
import logging
import os
import sys
from collections import defaultdict
from itertools import groupby
from typing import List, Optional

from dateutil.relativedelta import relativedelta
from jinja2 import Template

from recidiviz.aggregated_metrics.metric_time_period_config import MetricTimePeriod
from recidiviz.calculator.query.state.views.outliers.outliers_enabled_states import (
    get_outliers_enabled_states,
)
from recidiviz.common.constants.states import StateCode
from recidiviz.common.str_field_utils import join_with_conjunction
from recidiviz.outliers.constants import (
    ABSCONSIONS_BENCH_WARRANTS,
    INCARCERATION_STARTS,
    INCARCERATION_STARTS_TECHNICAL_VIOLATION,
    TASK_COMPLETIONS_FULL_TERM_DISCHARGE,
    TASK_COMPLETIONS_TRANSFER_TO_LIMITED_SUPERVISION,
)
from recidiviz.outliers.querier.querier import OutliersQuerier
from recidiviz.outliers.types import (
    MetricOutcome,
    OfficerSupervisorReportData,
    OutlierMetricInfo,
    OutliersProductConfiguration,
    OutliersVitalsMetricConfig,
    TargetStatusStrategy,
)
from recidiviz.persistence.database.database_managers.state_segmented_database_manager import (
    StateSegmentedDatabaseManager,
)
from recidiviz.persistence.database.schema_type import SchemaType
from recidiviz.persistence.database.sqlalchemy_engine_manager import (
    SQLAlchemyEngineManager,
)
from recidiviz.reporting.asset_generation.client import AssetGenerationClient
from recidiviz.reporting.asset_generation.types import AssetResponseBase
from recidiviz.reporting.context.outliers_supervision_officer_supervisor.data_retrieval import (
    retrieve_data_for_outliers_supervision_officer_supervisor,
)
from recidiviz.reporting.context.outliers_supervision_officer_supervisor.fixtures import (
    create_fixture,
    get_metric_fixtures_for_state,
    highlighted_officers_fixture_adverse,
    highlighted_officers_fixture_favorable,
    highlighted_officers_fixture_favorable_zero,
    other_officers_fixture_adverse,
    other_officers_fixture_favorable,
    other_officers_fixture_favorable_zero,
    target_fixture_adverse,
    target_fixture_favorable,
    target_fixture_favorable_zero,
)
from recidiviz.reporting.context.outliers_supervision_officer_supervisor.types import (
    Faq,
    Highlights,
    MetricHighlightDetail,
    MultipleMetricHighlight,
)
from recidiviz.reporting.context.report_context import ReportContext
from recidiviz.reporting.email_reporting_utils import (
    DATETIME_FORMAT_STR,
    Batch,
    ReportType,
    get_date_from_batch_id,
)
from recidiviz.reporting.recipient import Recipient
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

assets = AssetGenerationClient()


class OutliersSupervisionOfficerSupervisorContext(ReportContext):
    """Report context for the Outliers Supervision Officer Supervisor email."""

    _chart_width = 571

    def get_report_type(self) -> ReportType:
        return ReportType.OutliersSupervisionOfficerSupervisor

    def get_required_recipient_data_fields(self) -> List[str]:
        return ["report", "config"]

    @property
    def html_template(self) -> Template:
        return self.jinja_env.get_template(
            "outliers_supervision_officer_supervisor/email.html.jinja2"
        )

    def _prepare_for_generation(self) -> dict:
        prepared_data = {
            "headline": f"Your {self._report_month} Unit Alert",
            "state_name": self._state_name,
            "officer_label": self._config.supervision_officer_label,
            "metric_periods": {
                "current": self._current_metric_period,
                "previous": self._previous_metric_period,
            },
            "chart_width": self._chart_width,
            "show_metric_section_headings": self._show_metric_section_headings,
            "favorable_metrics": [
                self._prepare_metric(m)
                for m in self._report_data.metrics
                if m.metric.outcome_type == MetricOutcome.FAVORABLE
            ],
            "adverse_metrics": [
                self._prepare_metric(m)
                for m in self._report_data.metrics
                if m.metric.outcome_type == MetricOutcome.ADVERSE
            ],
            "highlights": self._highlights,
            "faq": self._faq,
            "feedback_form_url": "https://forms.gle/c7WaTaC7wMu9CrsB9",
        }
        self.prepared_data = prepared_data
        return prepared_data

    @property
    def _state_name(self) -> str:
        return (
            "Idaho"
            # special case due to Atlas migration
            if self.batch.state_code == StateCode.US_IX
            else self.batch.state_code.get_state().name
        )

    @property
    def _report_date(self) -> datetime.date:
        return get_date_from_batch_id(self.batch)

    @property
    def _report_month(self) -> str:
        return self._report_date.strftime("%B")

    @property
    def _report_data(self) -> OfficerSupervisorReportData:
        return self.recipient_data["report"]

    @property
    def _config(self) -> OutliersProductConfiguration:
        return self.recipient_data["config"]

    def _prepare_metric(self, metric_info: OutlierMetricInfo) -> dict:
        return {
            "title_display_name": metric_info.metric.title_display_name,
            "body_display_name": metric_info.metric.body_display_name,
            "legend_zero": metric_info.target_status_strategy
            == TargetStatusStrategy.ZERO_RATE,
            "far_direction": (
                "above"
                if metric_info.metric.outcome_type == MetricOutcome.ADVERSE
                else "below"
            ),
            "event_name": metric_info.metric.event_name,
            "chart": {
                "url": self._request_chart(metric_info).url,
                "alt_text": self._chart_alt_text(metric_info),
            },
        }

    def _request_chart(self, metric_info: OutlierMetricInfo) -> AssetResponseBase:
        return assets.generate_outliers_supervisor_chart(
            self.batch.state_code,
            f"{self.recipient.email_address}-{metric_info.metric.name}",
            self._chart_width,
            metric_info,
        )

    def _chart_alt_text(self, metric_info: OutlierMetricInfo) -> str:
        if metric_info.target_status_strategy == TargetStatusStrategy.ZERO_RATE:
            highlight_condition = f"{'have' if len(metric_info.highlighted_officers) > 1 else 'has'} zero {metric_info.metric.event_name}"
        elif metric_info.target_status_strategy == TargetStatusStrategy.IQR_THRESHOLD:
            highlight_condition = f"{'are' if len(metric_info.highlighted_officers) > 1 else 'is'} far from the state average"
        else:
            raise ValueError(
                f"target_status_strategy {metric_info.target_status_strategy} is not supported"
            )

        highlighted_names = join_with_conjunction(
            [o.name.formatted_first_last for o in metric_info.highlighted_officers]
        )
        return f"Swarm plot of all {metric_info.metric.body_display_name}s in the state where {highlighted_names} {highlight_condition} for the current reporting period."

    @property
    def _show_metric_section_headings(self) -> bool:
        """Only True if there is more than one type of metric configured for the current state"""
        outcome_types = set(m.outcome_type for m in self._config.metrics)
        return len(outcome_types) > 1

    def _get_metric_period(self, date: datetime.date) -> str:
        end_month = date.strftime("%b")
        end_year = date.strftime("%y")

        period_start = date - relativedelta(years=1)
        start_month = period_start.strftime("%b")
        start_year = period_start.strftime("%y")

        return f"{start_month} &rsquo;{start_year}&ndash;{end_month} &rsquo;{end_year}"

    @property
    def _current_metric_period(self) -> str:
        return self._get_metric_period(self._report_date)

    @property
    def _previous_metric_period(self) -> str:
        return self._get_metric_period(self._report_date - relativedelta(months=1))

    @property
    def _highlights(self) -> Highlights:
        """Computes highlights based on report data and creates objects needed to display them."""
        return Highlights(
            multiple_metrics=self._multiple_metrics_highlight,
            no_outliers=self._no_outliers_highlight,
        )

    @property
    def _multiple_metrics_highlight(self) -> Optional[List[MultipleMetricHighlight]]:
        """Computes highlight for officers who are outliers on multiple metrics"""
        if not self._report_data.metrics:
            return None

        metrics_by_officer: defaultdict[str, List[OutlierMetricInfo]] = defaultdict(
            list
        )
        for metric in self._report_data.metrics:
            for officer in metric.highlighted_officers:
                metrics_by_officer[officer.name.formatted_first_last].append(metric)

        # filter and sort by number of metrics. officers only qualify for this highlight
        # if they are named on two or more metrics
        officers_to_highlight = sorted(
            ((k, v) for k, v in metrics_by_officer.items() if len(v) > 1),
            key=lambda o: len(o[1]),
            reverse=True,
        )

        if not officers_to_highlight:
            return None

        # group each officer's metrics by target strategy
        groups = [
            groupby(
                sorted(o[1], key=lambda m: m.target_status_strategy.value),
                key=lambda m: m.target_status_strategy,
            )
            for o in officers_to_highlight
        ]
        # convert each group into a Detail
        details: List[List[MetricHighlightDetail]] = [
            [
                (
                    MetricHighlightDetail(
                        condition="is far from the state average on",
                        metrics=join_with_conjunction(
                            [metric.metric.body_display_name for metric in metrics]
                        ),
                    )
                    if strategy == TargetStatusStrategy.IQR_THRESHOLD
                    else MetricHighlightDetail(
                        condition="has zero",
                        metrics=join_with_conjunction(
                            [metric.metric.event_name for metric in metrics]
                        ),
                    )
                )
                for strategy, metrics in group
            ]
            for group in groups
        ]

        return [
            MultipleMetricHighlight(name=officer[0], details=details[index])
            for index, officer in enumerate(officers_to_highlight)
        ]

    @property
    def _no_outliers_highlight(self) -> Optional[str]:
        """computes highlight for metrics without outliers"""
        if self._report_data.metrics_without_outliers:
            return join_with_conjunction(
                [
                    m.body_display_name
                    for m in self._report_data.metrics_without_outliers
                ],
                conjunction="or",
            )
        return None

    @property
    def _faq(self) -> List[Faq]:
        return [
            Faq(
                text="How are these metrics calculated?",
                url=self._config.learn_more_url,
            ),
            Faq(
                text="How is “far” from average defined?",
                url=self._config.learn_more_url,
            ),
            Faq(text="Who made this email?", url=self._config.learn_more_url),
            Faq(text="Read the full methodology", url=self._config.learn_more_url),
        ]


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    subparsers = parser.add_subparsers(dest="cmd")

    # untracked directory for convenience
    default_dest = "recidiviz/local/reporting/outliers_supervision_officer_supervisor/"

    fixture_parser = subparsers.add_parser("fixture")
    fixture_parser.add_argument("--dest", default=default_dest)

    db_parser = subparsers.add_parser("db")
    db_parser.add_argument(
        "--state_code", dest="state_code", type=StateCode, required=True
    )
    db_parser.add_argument("--dest", default=default_dest)

    known_args, _ = parser.parse_known_args(sys.argv[1:])

    if known_args.cmd == "fixture":
        test_report_date = datetime.datetime.now()
        test_state_code = StateCode.US_OZ

        metric_fixtures = get_metric_fixtures_for_state(test_state_code)
        test_config = OutliersProductConfiguration(
            updated_at=datetime.datetime(2024, 1, 1),
            updated_by="alexa@recidiviz.org",
            feature_variant=None,
            supervision_district_label="district",
            supervision_district_manager_label="district manager",
            supervision_jii_label="client",
            supervisor_has_no_outlier_officers_label="Nice! No officers are outliers on any metrics this month.",
            officer_has_no_outlier_metrics_label="Nice! No outlying metrics this month.",
            supervisor_has_no_officers_with_eligible_clients_label="Nice! No outstanding opportunities for now.",
            officer_has_no_eligible_clients_label="Nice! No outstanding opportunities for now.",
            supervision_unit_label="unit",
            supervision_supervisor_label="supervisor",
            supervision_officer_label="officer",
            metrics=[
                metric_fixtures[INCARCERATION_STARTS],
                metric_fixtures[INCARCERATION_STARTS_TECHNICAL_VIOLATION],
                metric_fixtures[ABSCONSIONS_BENCH_WARRANTS],
                metric_fixtures[TASK_COMPLETIONS_FULL_TERM_DISCHARGE],
                metric_fixtures[TASK_COMPLETIONS_TRANSFER_TO_LIMITED_SUPERVISION],
            ],
            learn_more_url="https://recidiviz.org",
            vitals_metrics_methodology_url="https://recidiviz.org",
            vitals_metrics=[
                OutliersVitalsMetricConfig(
                    metric_id="timely_risk_assessment",
                    title_display_name="Timely Risk Assessment",
                    body_display_name="Assessment",
                    numerator_query_fragment="avg_population_assessment_required - avg_population_assessment_overdue",
                    denominator_query_fragment="avg_population_assessment_required",
                    metric_time_period=MetricTimePeriod.DAY,
                ),
                OutliersVitalsMetricConfig(
                    metric_id="timely_contact",
                    title_display_name="Timely Contact",
                    body_display_name="Contact",
                    numerator_query_fragment="avg_population_contacts_required - avg_population_contacts_overdue",
                    denominator_query_fragment="avg_population_contacts_required",
                    metric_time_period=MetricTimePeriod.DAY,
                ),
            ],
        )
    elif known_args.cmd == "db":
        test_report_date = datetime.datetime.now()
        test_state_code = known_args.state_code

        # # init the database collection to fetch from local
        database_manager = StateSegmentedDatabaseManager(
            get_outliers_enabled_states(), SchemaType.INSIGHTS
        )
        database_key = database_manager.database_key_for_state(test_state_code.value)
        outliers_engine = SQLAlchemyEngineManager.get_engine_for_database(database_key)

        test_config = OutliersQuerier(
            test_state_code,
            # This querier is only used for emails, so there are no user feature variants to check.
            # Limit our metrics to ones that a user with default FV values would see.
            [],
        ).get_product_configuration()

    else:
        raise NotImplementedError()

    batch = Batch(
        state_code=test_state_code,
        batch_id=test_report_date.strftime(DATETIME_FORMAT_STR),
        report_type=ReportType.OutliersSupervisionOfficerSupervisor,
    )

    def make_email(report: OfficerSupervisorReportData) -> None:
        recipient = Recipient(
            email_address=report.recipient_email_address,
            state_code=batch.state_code,
            data={
                "report": report,
                "config": test_config,
            },
        )
        context = OutliersSupervisionOfficerSupervisorContext(batch, recipient)
        try:
            with local_project_id_override(GCP_PROJECT_STAGING):
                report_data = context.get_prepared_data()

            filepath_relative = os.path.join(
                known_args.dest,
                batch.state_code.value,
                f"{recipient.email_address}.html",
            )
            dest = os.path.join(os.getcwd(), filepath_relative)
            os.makedirs(os.path.dirname(dest), exist_ok=True)

            contents = context.html_template.render(**report_data)
            with open(dest, "w", encoding="utf8") as f:
                f.write(contents)
                print(f"HTML generated at {filepath_relative}")

        except Exception:
            logging.error("ERROR: %s", report.recipient_email_address, exc_info=True)

    if known_args.cmd == "db":
        all_reports = retrieve_data_for_outliers_supervision_officer_supervisor(batch)
        for current_report in all_reports.values():
            make_email(current_report)

    elif known_args.cmd == "fixture":
        make_email(
            OfficerSupervisorReportData(
                [
                    create_fixture(
                        metric_fixtures[INCARCERATION_STARTS],
                        target_fixture_adverse,
                        other_officers_fixture_adverse,
                        highlighted_officers_fixture_adverse,
                    ),
                    create_fixture(
                        metric_fixtures[INCARCERATION_STARTS_TECHNICAL_VIOLATION],
                        target_fixture_adverse,
                        other_officers_fixture_adverse,
                        highlighted_officers_fixture_adverse[:1],
                    ),
                    create_fixture(
                        metric_fixtures[TASK_COMPLETIONS_FULL_TERM_DISCHARGE],
                        target_fixture_favorable,
                        other_officers_fixture_favorable,
                        highlighted_officers_fixture_favorable,
                    ),
                    create_fixture(
                        metric_fixtures[
                            TASK_COMPLETIONS_TRANSFER_TO_LIMITED_SUPERVISION
                        ],
                        target_fixture_favorable_zero,
                        other_officers_fixture_favorable_zero,
                        highlighted_officers_fixture_favorable_zero,
                        TargetStatusStrategy.ZERO_RATE,
                    ),
                ],
                [
                    metric_fixtures[ABSCONSIONS_BENCH_WARRANTS],
                ],
                "test-outliers-supervisor@recidiviz.org",
                [],
            )
        )
