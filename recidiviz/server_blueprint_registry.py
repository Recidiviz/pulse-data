# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2022 Recidiviz, Inc.
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
"""Global registration of all endpoints for the main Recidiviz server backend."""
from typing import List, Tuple

from flask import Blueprint

from recidiviz.admin_panel.all_routes import admin_panel_blueprint
from recidiviz.auth.auth_endpoint import auth_endpoint_blueprint
from recidiviz.backup.backup_manager import backup_manager_blueprint
from recidiviz.big_query.view_update_manager import view_update_manager_blueprint
from recidiviz.calculator.calculation_data_storage_manager import (
    calculation_data_storage_manager_blueprint,
)
from recidiviz.case_triage.ops_routes import case_triage_ops_blueprint
from recidiviz.ingest.aggregate.parse import aggregate_parse_blueprint
from recidiviz.ingest.aggregate.scrape_aggregate_reports import (
    scrape_aggregate_reports_blueprint,
)
from recidiviz.ingest.aggregate.single_count import store_single_count_blueprint
from recidiviz.ingest.direct.direct_ingest_control import direct_ingest_control
from recidiviz.ingest.justice_counts.control import justice_counts_control
from recidiviz.ingest.scrape.infer_release import infer_release_blueprint
from recidiviz.ingest.scrape.scraper_control import scraper_control
from recidiviz.ingest.scrape.scraper_status import scraper_status
from recidiviz.ingest.scrape.worker import worker
from recidiviz.metrics.export.view_export_manager import export_blueprint
from recidiviz.persistence.batch_persistence import batch_blueprint
from recidiviz.persistence.database.bq_refresh.cloud_sql_to_bq_refresh_control import (
    cloud_sql_to_bq_blueprint,
)
from recidiviz.practices.etl.routes import get_practices_etl_blueprint
from recidiviz.validation.validation_manager import validation_manager_blueprint

scraper_blueprints_with_url_prefixes: List[Tuple[Blueprint, str]] = [
    (batch_blueprint, "/batch"),
    (aggregate_parse_blueprint, "/aggregate"),
    (infer_release_blueprint, "/infer_release"),
    (scraper_control, "/scraper"),
    (scraper_status, "/scraper"),
    (worker, "/scraper"),
    (scrape_aggregate_reports_blueprint, "/scrape_aggregate_reports"),
    (store_single_count_blueprint, "/single_count"),
]
default_blueprints_with_url_prefixes: List[Tuple[Blueprint, str]] = [
    (admin_panel_blueprint, "/admin"),
    (auth_endpoint_blueprint, "/auth"),
    (backup_manager_blueprint, "/backup_manager"),
    (calculation_data_storage_manager_blueprint, "/calculation_data_storage_manager"),
    (case_triage_ops_blueprint, "/case_triage_ops"),
    (cloud_sql_to_bq_blueprint, "/cloud_sql_to_bq"),
    (direct_ingest_control, "/direct"),
    (export_blueprint, "/export"),
    (justice_counts_control, "/justice_counts"),
    (get_practices_etl_blueprint(), "/practices-etl"),
    (validation_manager_blueprint, "/validation_manager"),
    (view_update_manager_blueprint, "/view_update"),
]


def get_blueprints_for_documentation() -> List[Tuple[Blueprint, str]]:
    all_blueprints_with_url_prefixes = (
        scraper_blueprints_with_url_prefixes + default_blueprints_with_url_prefixes
    )

    return all_blueprints_with_url_prefixes
