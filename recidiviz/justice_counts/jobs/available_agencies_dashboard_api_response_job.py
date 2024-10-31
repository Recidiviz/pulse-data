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
"""
This script generates a JSON response for the agency dashboards homepage and uploads
it to Google Cloud Storage. It queries agencies with enabled dashboards, retrieves their
corresponding metrics, and constructs the homepage JSON data for each agency. The result
is uploaded as a string to a specified location in Google Cloud Storage.

The script supports both staging and production environments.

Args:
    --project-id: Specifies the GCP project environment to run the script in (staging/production).
    --dry-run: If set to True, the script will log actions without making any changes.

Returns:
    The script doesn't return any value but logs actions and any errors encountered.

Usage:
    python -m available_agencies_dashboard_api_response_job --project-id GCP_PROJECT_JUSTICE_COUNTS_PRODUCTION
"""

import argparse
import json
import logging

import sentry_sdk

from recidiviz.cloud_storage.gcsfs_factory import GcsfsFactory
from recidiviz.common.fips import (
    get_county_code_to_county_fips,
    get_county_code_to_county_name,
)
from recidiviz.justice_counts.agency import AgencyInterface
from recidiviz.justice_counts.control_panel.utils import (
    get_available_agency_dashboards_api_response_file_path,
)
from recidiviz.justice_counts.metric_setting import MetricSettingInterface
from recidiviz.justice_counts.report import ReportInterface
from recidiviz.justice_counts.utils.constants import JUSTICE_COUNTS_SENTRY_DSN
from recidiviz.justice_counts.utils.geoid import get_fips_code_to_geoid
from recidiviz.persistence.database.constants import JUSTICE_COUNTS_DB_SECRET_PREFIX
from recidiviz.persistence.database.schema_type import SchemaType
from recidiviz.persistence.database.session import Session
from recidiviz.persistence.database.sqlalchemy_database_key import SQLAlchemyDatabaseKey
from recidiviz.persistence.database.sqlalchemy_engine_manager import (
    SQLAlchemyEngineManager,
)
from recidiviz.utils.environment import (
    GCP_PROJECT_JUSTICE_COUNTS_PRODUCTION,
    GCP_PROJECT_JUSTICE_COUNTS_STAGING,
)
from recidiviz.utils.params import str_to_bool

logger = logging.getLogger(__name__)


def create_parser() -> argparse.ArgumentParser:
    """Returns an argument parser for the script."""
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--project-id",
        choices=[
            GCP_PROJECT_JUSTICE_COUNTS_STAGING,
            GCP_PROJECT_JUSTICE_COUNTS_PRODUCTION,
        ],
        help="Used to select which GCP project in which to run this script.",
        required=False,
    )
    parser.add_argument("--dry-run", type=str_to_bool, default=False)
    return parser


if __name__ == "__main__":
    sentry_sdk.init(
        dsn=JUSTICE_COUNTS_SENTRY_DSN,
        # Enable performance monitoring
        enable_tracing=True,
    )

    logging.basicConfig(level=logging.INFO)
    args = create_parser().parse_args()
    environment_str = (
        "PRODUCTION"
        if args.project_id == GCP_PROJECT_JUSTICE_COUNTS_PRODUCTION
        else "STAGING"
    )
    logger.info("Running in %s environment", environment_str)

    database_key = SQLAlchemyDatabaseKey.for_schema(
        SchemaType.JUSTICE_COUNTS,
    )
    justice_counts_engine = SQLAlchemyEngineManager.init_engine(
        database_key=database_key,
        secret_prefix_override=JUSTICE_COUNTS_DB_SECRET_PREFIX,
    )
    global_session = Session(bind=justice_counts_engine)

    try:
        logger.info("Fetching agencies with enabled dashboards.")
        agency_query = AgencyInterface.get_agencies_with_enabled_dashboard(
            session=global_session
        )
        agency_id_to_metric_key_to_metric_interface = (
            MetricSettingInterface.get_agency_id_to_metric_key_to_metric_interface(
                session=global_session, agency_query=agency_query
            )
        )
        agency_id_to_metric_key_dim_id_to_available_members = (
            ReportInterface.get_agency_id_to_metric_key_dim_id_to_available_members(
                session=global_session
            )
        )
        fips_code_to_geoid = get_fips_code_to_geoid()
        county_code_to_county_name = get_county_code_to_county_name()
        county_code_to_county_fips = get_county_code_to_county_fips()

        agency_json_list = []
        for agency in agency_query:
            logger.info("Generating dashboard JSON for agency: %s", agency.name)
            agency_json = AgencyInterface.get_dashboard_homepage_json(
                fips_code_to_geoid=fips_code_to_geoid,
                county_code_to_county_name=county_code_to_county_name,
                county_code_to_county_fips=county_code_to_county_fips,
                agency=agency,
                metric_key_to_metric_interface=agency_id_to_metric_key_to_metric_interface[
                    agency.id
                ],
                metric_key_dim_id_to_available_members=agency_id_to_metric_key_dim_id_to_available_members[
                    agency.id
                ],
            )
            agency_json_list.append(agency_json)
            if args.dry_run is True:
                logger.info(
                    "DRY RUN: Dashboard Homepage Json for %s: %s",
                    agency.name,
                    json.dumps(agency_json),
                )

        if args.dry_run is False:
            fs = GcsfsFactory.build()
            path = get_available_agency_dashboards_api_response_file_path(
                project_id=args.project_id
            )
            json_data = json.dumps(agency_json_list)

            logger.info("Uploading agency dashboard JSON to %s", path)
            fs.upload_from_string(
                path=path, contents=json_data, content_type="text/plain"
            )
            logger.info("Upload successful.")

    except Exception as e:
        logger.exception("Agency Dashboard API Response Job Failed: %s", str(e))
