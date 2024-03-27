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

"""
Moves an agency and its metric settings, reports, and datapoints from the source environment
to the destination environment.

python -m recidiviz.tools.justice_counts.migrate_agency \
  --source=justice-counts-staging
  --destination=justice-counts-production
  --agency_ids=1,2,3
  --dry-run=true
"""
import argparse
import logging
from collections import defaultdict
from typing import Any, Dict, List

from recidiviz.justice_counts.agency import AgencyInterface
from recidiviz.justice_counts.datapoint import DatapointInterface
from recidiviz.justice_counts.metric_setting import MetricSettingInterface
from recidiviz.justice_counts.metrics.metric_interface import MetricInterface
from recidiviz.justice_counts.report import ReportInterface
from recidiviz.justice_counts.user_account import UserAccountInterface
from recidiviz.persistence.database.constants import JUSTICE_COUNTS_DB_SECRET_PREFIX
from recidiviz.persistence.database.schema.justice_counts import schema
from recidiviz.persistence.database.schema_type import SchemaType
from recidiviz.persistence.database.session_factory import SessionFactory
from recidiviz.persistence.database.sqlalchemy_database_key import SQLAlchemyDatabaseKey
from recidiviz.tools.postgres.cloudsql_proxy_control import cloudsql_proxy_control
from recidiviz.utils.environment import (
    GCP_PROJECT_JUSTICE_COUNTS_PRODUCTION,
    GCP_PROJECT_JUSTICE_COUNTS_STAGING,
)
from recidiviz.utils.metadata import local_project_id_override
from recidiviz.utils.params import str_to_bool, str_to_list

logger = logging.getLogger(__name__)

agency_name_to_user_jsons: Dict[str, List[Dict[str, Any]]] = defaultdict(list)
agency_name_to_report_json: Dict[str, List[Dict[str, Any]]] = defaultdict(list)
report_id_to_datapoint_json_list: Dict[str, List[Dict[str, Any]]] = defaultdict(list)
agency_name_to_agency_json: Dict[str, Dict[str, Any]] = {}
agency_name_to_metric_settings: Dict[str, List[MetricInterface]] = {}


def create_parser() -> argparse.ArgumentParser:
    """Returns an argument parser for the script."""
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--source",
        choices=[
            GCP_PROJECT_JUSTICE_COUNTS_STAGING,
            GCP_PROJECT_JUSTICE_COUNTS_PRODUCTION,
        ],
        type=str,
        required=True,
    )
    parser.add_argument(
        "--destination",
        choices=[
            GCP_PROJECT_JUSTICE_COUNTS_STAGING,
            GCP_PROJECT_JUSTICE_COUNTS_PRODUCTION,
        ],
        type=str,
        required=True,
    )

    parser.add_argument(
        "--agency_ids",
        type=str_to_list,
        help="Ids of the agencies that you want to migrate.",
        required=True,
    )

    parser.add_argument("--dry-run", type=str_to_bool, default=True)
    parser.add_argument("--add_csg_users", type=str_to_bool, default=False)
    return parser


def migrate_agency_data(
    destination_project_name: str,
    dry_run: bool,
    schema_type: SchemaType,
    add_csg_users: bool,
) -> None:
    """
    Migrates agency data by creating new agencies, reports, and datapoints
    based on the data saved in the global variables listed at the top of the
    file (i.e agency_name_to_metric_settings).
    """
    database_key = SQLAlchemyDatabaseKey.for_schema(schema_type)
    with local_project_id_override(destination_project_name):
        with cloudsql_proxy_control.connection(
            schema_type=schema_type,
            secret_prefix_override=JUSTICE_COUNTS_DB_SECRET_PREFIX,
        ):
            with SessionFactory.for_proxy(
                database_key=database_key,
                secret_prefix_override=JUSTICE_COUNTS_DB_SECRET_PREFIX,
                autocommit=False,
            ) as session:
                logger.info(
                    "------------------------------------------------------------"
                )
                logger.info("Migrating data to %s", destination_project_name)
                for agency_name, agency_json in agency_name_to_agency_json.items():
                    logger.info(
                        "------------------------------------------------------------"
                    )
                    logger.info("Migrating %s", agency_name)
                    existing_agency = AgencyInterface.get_agency_by_name(
                        session=session, name=agency_name
                    )

                    if existing_agency is not None:
                        logger.info(
                            "%s already exists in %s, skipping",
                            agency_name,
                            destination_project_name,
                        )
                        continue

                    curr_agency = schema.Agency(
                        name=agency_name,
                        systems=agency_json["systems"],
                        state_code=agency_json["state_code"],
                        fips_county_code=agency_json["fips_county_code"],
                        is_superagency=agency_json["is_superagency"],
                        is_dashboard_enabled=agency_json["is_dashboard_enabled"],
                        created_at=agency_json["created_at"],
                    )

                    session.add(curr_agency)
                    logger.info("Migrating Users...")
                    existing_user_emails = set()
                    if add_csg_users is True:
                        csg_users = UserAccountInterface.get_csg_users(session=session)
                        for csg_user in csg_users:
                            session.add(
                                schema.AgencyUserAccountAssociation(
                                    user_account=csg_user,
                                    agency=curr_agency,
                                    role=schema.UserAccountRole.AGENCY_ADMIN,
                                )
                            )
                            existing_user_emails.add(csg_user.email)
                    for user_json in agency_name_to_user_jsons[agency_name]:
                        if user_json["email"] in existing_user_emails:
                            continue

                        existing_user = UserAccountInterface.get_user_by_email(
                            session=session,
                            email=user_json["email"],
                        )
                        if existing_user is None:
                            logger.info(
                                "User with email %s does not exist in %s, skipping",
                                user_json["email"],
                                destination_project_name,
                            )
                            continue

                        session.add(
                            schema.AgencyUserAccountAssociation(
                                user_account=existing_user,
                                agency=curr_agency,
                                role=schema.UserAccountRole(
                                    user_json["role"]
                                    if user_json["role"] is not None
                                    else schema.UserAccountRole.READ_ONLY
                                ),
                            )
                        )

                    logger.info("Migrating Metric Settings...")
                    for metric_setting in agency_name_to_metric_settings[agency_name]:
                        DatapointInterface.add_or_update_agency_datapoints(
                            session=session,
                            agency=curr_agency,
                            agency_metric=metric_setting,
                        )
                    logger.info("Migrating Reports...")
                    for report_json in agency_name_to_report_json[agency_name]:
                        (
                            date_range_start,
                            date_range_end,
                        ) = ReportInterface.get_date_range(
                            month=report_json["month"],
                            year=report_json["year"],
                            frequency=report_json["frequency"],
                        )
                        logger.info(
                            "Migrating %s/%s - %s/%s Report ...",
                            str(date_range_start.month),
                            str(date_range_start.year),
                            str(date_range_end.month),
                            str(date_range_end.year),
                        )
                        report_type = (
                            schema.ReportingFrequency.MONTHLY.value
                            if report_json["frequency"]
                            == schema.ReportingFrequency.MONTHLY.value
                            else schema.ReportingFrequency.ANNUAL.value
                        )

                        curr_report = schema.Report(
                            source=curr_agency,
                            type=report_type,
                            created_at=report_json["created_at"],
                            acquisition_method=schema.AcquisitionMethod.CONTROL_PANEL,
                            project=schema.Project.JUSTICE_COUNTS_CONTROL_PANEL,
                            status=schema.ReportStatus(report_json["status"]),
                            is_recurring=report_json["is_recurring"],
                            date_range_start=date_range_start,
                            date_range_end=date_range_end,
                            instance=ReportInterface.get_report_instance(
                                report_type=report_type,
                                date_range_start=date_range_start,
                            ),
                            publish_date=report_json["publish_date"],
                            last_modified_at=report_json["last_modified_at"],
                        )

                        session.add(curr_report)
                        logger.info("Migrating Report Datapoints")
                        for datapoint_json in report_id_to_datapoint_json_list[
                            report_json["id"]
                        ]:
                            session.add(
                                schema.Datapoint(
                                    report=curr_report,
                                    value=datapoint_json["value"],
                                    metric_definition_key=datapoint_json[
                                        "metric_definition_key"
                                    ],
                                    start_date=date_range_start,
                                    end_date=date_range_end,
                                    dimension_identifier_to_member=datapoint_json[
                                        "dimension_identifier_to_member"
                                    ],
                                    value_type=schema.ValueType(
                                        datapoint_json["value_type"]
                                    )
                                    if datapoint_json["value_type"] is not None
                                    else None,
                                    created_at=datapoint_json["created_at"],
                                    last_updated=datapoint_json["last_updated"],
                                    is_report_datapoint=True,
                                )
                            )
                if dry_run is False:
                    session.commit()


def get_agency_data(
    agency_ids_strs: List[str],
    source_project_name: str,
    schema_type: SchemaType,
) -> None:
    """
    Fetches agency metadata, reports, datapoints, and metic settings
    and saves them to global variables.
    """
    database_key = SQLAlchemyDatabaseKey.for_schema(schema_type)
    with local_project_id_override(source_project_name):
        with cloudsql_proxy_control.connection(
            schema_type=schema_type,
            secret_prefix_override=JUSTICE_COUNTS_DB_SECRET_PREFIX,
        ):
            with SessionFactory.for_proxy(
                database_key=database_key,
                secret_prefix_override=JUSTICE_COUNTS_DB_SECRET_PREFIX,
                autocommit=False,
            ) as session:
                logger.info(
                    "------------------------------------------------------------"
                )
                logger.info("Fetching agency data from %s", source_project_name)
                agency_ids = [int(id) for id in agency_ids_strs]

                agencies_to_migrate = AgencyInterface.get_agencies_by_id(
                    session=session, agency_ids=agency_ids
                )
                reports_to_migrate = ReportInterface.get_reports_by_agency_ids(
                    session=session, agency_ids=agency_ids, include_datapoints=True
                )

                logger.info("Saving report from %s", source_project_name)
                global agency_name_to_report_json
                agency_name_to_report_json = defaultdict(list)
                global report_id_to_datapoint_json_list
                report_id_to_datapoint_json_list = defaultdict(list)

                for report in reports_to_migrate:
                    json = ReportInterface.to_json_response(
                        report=report,
                        agency_name=report.source.name,
                        editor_id_to_json={},
                    )
                    json["created_at"] = report.created_at
                    agency_name_to_report_json[report.source.name].append(json)
                    datapoint_json_list = [
                        {
                            "report_id": d.report_id,
                            "metric_definition_key": d.metric_definition_key,
                            "value": d.value,
                            "value_type": d.value_type.value
                            if d.value_type is not None
                            else None,
                            "dimension_identifier_to_member": d.dimension_identifier_to_member,
                            "created_at": d.created_at,
                            "last_updated": d.last_updated,
                        }
                        for d in report.datapoints
                    ]
                    report_id_to_datapoint_json_list[report.id] = datapoint_json_list

                global agency_name_to_agency_json
                agency_name_to_agency_json = {
                    a.name: a.to_json(with_team=True, with_settings=True)
                    for a in agencies_to_migrate
                }

                logger.info(
                    "Saving metric setting data and user information from %s",
                    source_project_name,
                )
                global agency_name_to_metric_settings
                agency_name_to_metric_settings = {}
                global agency_name_to_user_jsons
                agency_name_to_user_jsons = defaultdict(list)
                for agency in agencies_to_migrate:
                    metric_settings = (
                        MetricSettingInterface.get_agency_metric_interfaces(
                            session=session,
                            agency=agency,
                        )
                    )
                    agency_name_to_metric_settings[agency.name] = metric_settings
                    agency_name_to_user_jsons[agency.name] = [
                        {
                            "email": u.user_account.email,
                            "role": u.role.value if u.role is not None else u.role,
                        }
                        for u in agency.user_account_assocs
                    ]


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    args = create_parser().parse_args()
    logging.info("DRY RUN: %s", args.dry_run)
    get_agency_data(
        agency_ids_strs=args.agency_ids,
        source_project_name=args.source,
        schema_type=SchemaType.JUSTICE_COUNTS,
    )

    migrate_agency_data(
        schema_type=SchemaType.JUSTICE_COUNTS,
        destination_project_name=args.destination,
        dry_run=args.dry_run,
        add_csg_users=args.add_csg_users,
    )
