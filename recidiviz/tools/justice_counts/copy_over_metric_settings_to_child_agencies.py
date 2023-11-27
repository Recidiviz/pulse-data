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

"""
Copies all metric settings from a super agency to its child agencies.
If child agencies have existing settings that conflict with the super agency,
they will be overwritten. If the child agency has existing settings that do not
conflict with the super agency, then they will not be affected.

The following are a list of metric definition keys grouped by system. 
These can be passed into the metric_definition_key_subset flag to 
update a subset of metrics.

COURTS: 
- COURTS_AND_PRETRIAL_FUNDING
- COURTS_AND_PRETRIAL_EXPENSES
- COURTS_AND_PRETRIAL_TOTAL_STAFF
- COURTS_AND_PRETRIAL_PRETRIAL_RELEASES
- COURTS_AND_PRETRIAL_CASES_FILED
- COURTS_AND_PRETRIAL_CASES_DISPOSED
- COURTS_AND_PRETRIAL_SENTENCES
- COURTS_AND_PRETRIAL_ARRESTS_ON_PRETRIAL_RELEASE

DEFENSE:
- DEFENSE_FUNDING
- DEFENSE_EXPENSES
- DEFENSE_TOTAL_STAFF
- DEFENSE_CASELOADS_PEOPLE
- DEFENSE_CASELOADS_STAFF
- DEFENSE_CASES_APPOINTED_COUNSEL
- DEFENSE_CASES_DISPOSED
- DEFENSE_COMPLAINTS_SUSTAINED

JAILS:
- JAILS_FUNDING
- JAILS_EXPENSES
- JAILS_TOTAL_STAFF
- JAILS_PRE_ADJUDICATION_ADMISSIONS
- JAILS_POST_ADJUDICATION_ADMISSIONS
- JAILS_PRE_ADJUDICATION_POPULATION
- JAILS_POST_ADJUDICATION_POPULATION
- JAILS_PRE_ADJUDICATION_RELEASES
- JAILS_POST_ADJUDICATION_RELEASES
- JAILS_READMISSIONS
- JAILS_USE_OF_FORCE_INCIDENTS 
- JAILS_GRIEVANCES_UPHELD

LAW_ENFORCEMENT:
- LAW_ENFORCEMENT_FUNDING
- LAW_ENFORCEMENT_EXPENSES
- LAW_ENFORCEMENT_TOTAL_STAFF
- LAW_ENFORCEMENT_CALLS_FOR_SERVICE
- LAW_ENFORCEMENT_ARRESTS
- LAW_ENFORCEMENT_REPORTED_CRIME
- LAW_ENFORCEMENT_USE_OF_FORCE_INCIDENTS
- LAW_ENFORCEMENT_COMPLAINTS_SUSTAINED

PRISONS:
- PRISONS_FUNDING
- PRISONS_EXPENSES
- PRISONS_TOTAL_STAFF
- PRISONS_ADMISSIONS
- PRISONS_POPULATION
- PRISONS_RELEASES
- PRISONS_READMISSIONS
- PRISONS_USE_OF_FORCE_INCIDENTS
- PRISONS_GRIEVANCES_UPHELD

PROSECUTION: 
- PROSECUTION_FUNDING
- PROSECUTION_EXPENSES
- PROSECUTION_TOTAL_STAFF
- PROSECUTION_CASELOADS_PEOPLE
- PROSECUTION_CASELOADS_STAFF
- PROSECUTION_CASES REFERRED
- PROSECUTION_CASES_DECLINED
- PROSECUTION_CASES_DIVERTED
- PROSECUTION_CASES_PROSECUTED
- PROSECUTION_CASES_DISPOSED
- PROSECUTION_VIOLATIONS_WITH_DISCIPLINARY_ACTION

SUPERVISION: 
- SUPERVISION_FUNDING
- SUPERVISION_EXPENSES
- SUPERVISION_TOTAL_STAFF
- SUPERVISION_CASELOADS_PEOPLE
- SUPERVISION_CASELOADS_STAFF
- SUPERVISION_SUPERVISION_STARTS
- SUPERVISION_POPULATION
- SUPERVISION_SUPERVISION_TERMINATIONS
- SUPERVISION_SUPERVISION_VIOLATIONS
- SUPERVISION_REVOCATIONS
- SUPERVISION_RECONVICTIONS

Note: If you need to copy over metrics for a supervision subsystem, replace the SUPERVISION 
in the metric name with the name of the subsystem (i.e SUPERVISION_FUNDING -> PAROLE_FUNDING).

python -m recidiviz.tools.justice_counts.copy_over_metric_settings_to_child_agencies \
  --project_id=<justice-counts-staging OR justice-counts-production> \
  --super_agency_id=<agency_id> \
  --dry-run=<true Or false>
  --metric_definition_key_subset=ALL
"""

import argparse
import logging
from typing import List

from recidiviz.justice_counts.agency import AgencyInterface
from recidiviz.justice_counts.datapoint import DatapointInterface
from recidiviz.justice_counts.metrics.metric_registry import METRIC_KEY_TO_METRIC
from recidiviz.persistence.database.constants import JUSTICE_COUNTS_DB_SECRET_PREFIX
from recidiviz.persistence.database.schema_type import SchemaType
from recidiviz.persistence.database.session_factory import SessionFactory
from recidiviz.persistence.database.sqlalchemy_database_key import SQLAlchemyDatabaseKey
from recidiviz.tools.postgres.cloudsql_proxy_control import cloudsql_proxy_control
from recidiviz.utils.metadata import local_project_id_override
from recidiviz.utils.params import str_to_bool, str_to_list

logger = logging.getLogger(__name__)


def create_parser() -> argparse.ArgumentParser:
    """Returns an argument parser for the script."""
    parser = argparse.ArgumentParser()

    parser.add_argument(
        "--project_id",
        dest="project_id",
        type=str,
        required=True,
    )

    parser.add_argument(
        "--super_agency_id",
        type=int,
        help="The id of the super_agency you want to copy agency settings from.",
        required=True,
    )

    parser.add_argument(
        "--metric_definition_key_subset",
        type=str_to_list,
        help="List of metrics definition keys that should be copied over. To find a list of metric definition keys for all systems check the script (copy_over_metric_settings_to_child_agencies.py) ",
        required=True,
    )

    parser.add_argument("--dry-run", type=str_to_bool, default=True)
    return parser


def copy_metric_settings(
    super_agency_id: int,
    dry_run: bool,
    metric_definition_key_subset: List[str],
) -> None:
    """Copies metric settings from a super agency to a child agency."""
    schema_type = SchemaType.JUSTICE_COUNTS
    database_key = SQLAlchemyDatabaseKey.for_schema(schema_type)
    with cloudsql_proxy_control.connection(
        schema_type=schema_type, secret_prefix_override=JUSTICE_COUNTS_DB_SECRET_PREFIX
    ):
        with SessionFactory.for_proxy(
            database_key=database_key,
            secret_prefix_override=JUSTICE_COUNTS_DB_SECRET_PREFIX,
            autocommit=False,
        ) as session:
            if dry_run is True:
                logger.info("DRY RUN! THE FOLLOWING CHANGES WILL NOT BE COMMITTED.")
            super_agency_list = AgencyInterface.get_agencies_by_id(
                session=session, agency_ids=[super_agency_id]
            )

            if len(super_agency_list) == 0:
                logger.info(
                    "No agency was found with the super_agency_id provided. Please check that you are running the script in the right environment"
                )
                return

            super_agency = super_agency_list.pop()

            child_agencies = AgencyInterface.get_child_agencies_by_agency_ids(
                session=session, agency_ids=[super_agency_id]
            )
            super_agency_metric_settings = (
                DatapointInterface.get_metric_settings_by_agency(
                    session=session,
                    agency=super_agency,
                )
            )

            for child_agency in child_agencies:
                logger.info("Child Agency: %s", child_agency.name)
                for metric_setting in super_agency_metric_settings:
                    if "ALL" in set(
                        metric_definition_key_subset
                    ) or metric_setting.metric_definition.key in set(
                        metric_definition_key_subset
                    ):
                        logger.info("Metric %s, is being updated", metric_setting.key)
                        if (
                            metric_setting.metric_definition.key
                            not in METRIC_KEY_TO_METRIC
                        ):
                            logger.info(
                                "Metric deprecated: %s, skipping",
                                metric_setting.metric_definition.key,
                            )
                        if dry_run is False:
                            DatapointInterface.add_or_update_agency_datapoints(
                                session=session,
                                agency=child_agency,
                                agency_metric=metric_setting,
                            )

            if dry_run is False:
                session.commit()


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    args = create_parser().parse_args()
    with local_project_id_override(args.project_id):
        copy_metric_settings(
            dry_run=args.dry_run,
            super_agency_id=args.super_agency_id,
            metric_definition_key_subset=args.metric_definition_key_subset,
        )
