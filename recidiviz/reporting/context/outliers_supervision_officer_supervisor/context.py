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
the admin_panel container in docker-compose. It will print the template HTML to stdout,
where it can be redirected to the destination of your choosing  (e.g. a file that you open
 in a browser).

You can populate the template with the fixture data that is written into this file: 

docker exec pulse-data-admin_panel_backend-1 pipenv run \
    python -m recidiviz.reporting.context.outliers_supervision_officer_supervisor.context \
    fixture

Or you can point it at your local database, if you have populated it with 
recidiviz.tools.outliers.load_local_db. 

For staging data (which is assumed to be up to date):

docker exec pulse-data-admin_panel_backend-1 pipenv run \
    python -m recidiviz.reporting.context.outliers_supervision_officer_supervisor.context \
    db --state_code US_PA --source GCS

For fixture data (which has a hardcoded report date):

docker exec pulse-data-admin_panel_backend-1 pipenv run \
    python -m recidiviz.reporting.context.outliers_supervision_officer_supervisor.context \
    db --state_code US_PA --source FIXTURE
"""
import argparse
import datetime
import sys
from collections import defaultdict
from itertools import groupby
from typing import List, Optional

from jinja2 import Template

from recidiviz.calculator.query.state.views.outliers.outliers_enabled_states import (
    get_outliers_enabled_states,
)
from recidiviz.common.constants.states import StateCode
from recidiviz.common.str_field_utils import join_with_conjunction
from recidiviz.outliers.constants import (
    ABSCONSIONS_BENCH_WARRANTS,
    INCARCERATION_STARTS,
    INCARCERATION_STARTS_TECHNICAL_VIOLATION,
    TASK_COMPLETIONS_TRANSFER_TO_LIMITED_SUPERVISION,
)
from recidiviz.outliers.types import (
    OfficerSupervisorReportData,
    OutlierMetricInfo,
    TargetStatusStrategy,
)
from recidiviz.persistence.database.database_managers.state_segmented_database_manager import (
    StateSegmentedDatabaseManager,
)
from recidiviz.persistence.database.schema_type import SchemaType
from recidiviz.persistence.database.sqlalchemy_engine_manager import (
    SQLAlchemyEngineManager,
)
from recidiviz.reporting.context.outliers_supervision_officer_supervisor.data_retrieval import (
    retrieve_data_for_outliers_supervision_officer_supervisor,
)
from recidiviz.reporting.context.outliers_supervision_officer_supervisor.fixtures import (
    create_fixture,
    highlighted_officers_fixture,
    metric_fixtures,
    other_officers_fixture,
    target_fixture,
)
from recidiviz.reporting.context.outliers_supervision_officer_supervisor.types import (
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


class OutliersSupervisionOfficerSupervisorContext(ReportContext):
    """Report context for the Outliers Supervision Officer Supervisor email."""

    def get_report_type(self) -> ReportType:
        return ReportType.OutliersSupervisionOfficerSupervisor

    def get_required_recipient_data_fields(self) -> List[str]:
        return ["report"]

    @property
    def html_template(self) -> Template:
        return self.jinja_env.get_template(
            "outliers_supervision_officer_supervisor/email.html.jinja2"
        )

    def _prepare_for_generation(self) -> dict:
        prepared_data = {
            "headline": f"Your {self._report_month} Unit Report",
            "highlights": self._highlights,
        }
        self.prepared_data = prepared_data
        return prepared_data

    @property
    def _report_month(self) -> str:
        return get_date_from_batch_id(self.batch).strftime("%B")

    @property
    def _report_data(self) -> OfficerSupervisorReportData:
        return self.recipient_data["report"]

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
                metrics_by_officer[officer.name].append(metric)

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


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    subparsers = parser.add_subparsers(dest="cmd")

    fixture_parser = subparsers.add_parser("fixture")

    db_parser = subparsers.add_parser("db")
    db_parser.add_argument(
        "--state_code", dest="state_code", type=StateCode, required=True
    )
    db_parser.add_argument("--source", default="GCS", choices=["GCS", "FIXTURE"])

    known_args, _ = parser.parse_known_args(sys.argv[1:])

    if known_args.cmd == "fixture":
        test_report_date = datetime.datetime.now()
        test_state_code = StateCode.US_OZ

        test_report = OfficerSupervisorReportData(
            [
                create_fixture(
                    metric_fixtures[INCARCERATION_STARTS],
                    target_fixture,
                    other_officers_fixture,
                    highlighted_officers_fixture,
                ),
                create_fixture(
                    metric_fixtures[INCARCERATION_STARTS_TECHNICAL_VIOLATION],
                    target_fixture,
                    other_officers_fixture,
                    highlighted_officers_fixture[:1],
                ),
                create_fixture(
                    metric_fixtures[TASK_COMPLETIONS_TRANSFER_TO_LIMITED_SUPERVISION],
                    target_fixture,
                    other_officers_fixture,
                    highlighted_officers_fixture[:1],
                    TargetStatusStrategy.ZERO_RATE,
                ),
            ],
            [
                metric_fixtures[ABSCONSIONS_BENCH_WARRANTS],
            ],
            "test-outliers-supervisor@recidiviz.org",
        )
    elif known_args.cmd == "db":
        # Fixture data is for may 2023
        test_report_date = (
            datetime.datetime.now()
            if known_args.source == "GCS"
            else datetime.datetime(2023, 5, 1)
        )
        test_state_code = known_args.state_code

        # # init the database collection to fetch from local
        database_manager = StateSegmentedDatabaseManager(
            get_outliers_enabled_states(), SchemaType.OUTLIERS
        )
        database_key = database_manager.database_key_for_state(test_state_code.value)
        outliers_engine = SQLAlchemyEngineManager.get_engine_for_database(database_key)

    else:
        raise NotImplementedError()

    batch = Batch(
        state_code=test_state_code,
        batch_id=test_report_date.strftime(DATETIME_FORMAT_STR),
        report_type=ReportType.OutliersSupervisionOfficerSupervisor,
    )

    if known_args.cmd == "db":
        all_reports = retrieve_data_for_outliers_supervision_officer_supervisor(batch)
        # basically just picking one at random, do something smarter if you need to
        test_report = next(iter(all_reports.values()))

    recipient = Recipient(
        email_address=test_report.recipient_email_address,
        state_code=batch.state_code,
        data={
            "report": test_report,
        },
    )
    context = OutliersSupervisionOfficerSupervisorContext(batch, recipient)
    with local_project_id_override(GCP_PROJECT_STAGING):
        report_data = context.get_prepared_data()

    print(context.html_template.render(**report_data))
